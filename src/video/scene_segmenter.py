"""Scene detection (pHash + temporal ceiling + debounce) and best-frame selection (Laplacian sharpness)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

import cv2
import imagehash
import numpy as np
from PIL import Image

from src.models.entities import SceneKeepReason
from src.video.video_scanner import VideoScanner

PHASH_THRESHOLD = 51
PHASH_HASH_SIZE = 16
TEMPORAL_CEILING_SEC = 30.0
DEBOUNCE_SEC = 3.0
SKIP_FRAMES_BEST = 2


def compute_segmentation_version() -> int:
    """Compute version integer from PHASH_THRESHOLD and DEBOUNCE_SEC for invalidation tracking."""
    return PHASH_THRESHOLD * 10000 + int(DEBOUNCE_SEC * 1000)


@dataclass(frozen=True)
class SceneResult:
    """One representative frame per closed scene."""

    best_frame_bytes: bytes
    best_pts: float
    scene_start_pts: float
    scene_end_pts: float
    keep_reason: SceneKeepReason
    sharpness_score: float = 0.0


def _frame_bytes_to_pil(frame_bytes: bytes, width: int, height: int) -> Image.Image:
    """Convert RGB24 frame_bytes to PIL Image for imagehash.

    This is a deliberate Pillow boundary: imagehash expects PIL.Image, and this
    path is not part of the image proxy worker's libvips-based hot path.
    """
    return Image.frombytes("RGB", (width, height), frame_bytes)


def _frame_bytes_to_gray(frame_bytes: bytes, width: int, height: int) -> np.ndarray:
    """Convert RGB24 frame_bytes to grayscale numpy (height, width) for OpenCV."""
    rgb = np.frombuffer(frame_bytes, dtype=np.uint8).reshape((height, width, 3))
    return cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)


def _hamming_distance(anchor_phash: imagehash.ImageHash, frame_phash: imagehash.ImageHash) -> int:
    """Hamming distance between two perceptual hashes (for testability)."""
    return int(anchor_phash - frame_phash)


def _trigger_keep_reason(
    anchor_phash: imagehash.ImageHash | None,
    scene_start_pts: float,
    frame_phash: imagehash.ImageHash,
    pts: float,
) -> SceneKeepReason | None:
    """
    CompositeStrategy: return reason when to trigger KEEP (new scene):
    - temporal: pts - scene_start_pts >= TEMPORAL_CEILING_SEC (30s ceiling).
    - phash: Hamming(anchor, frame) > PHASH_THRESHOLD and elapsed >= DEBOUNCE_SEC.
    - None: no trigger.
    """
    if anchor_phash is None:
        return None
    elapsed = pts - scene_start_pts
    if elapsed >= TEMPORAL_CEILING_SEC:
        return SceneKeepReason.temporal
    hamming = _hamming_distance(anchor_phash, frame_phash)
    if hamming <= PHASH_THRESHOLD:
        return None
    if elapsed < DEBOUNCE_SEC:
        return None
    return SceneKeepReason.phash


def _sharpness(frame_bytes: bytes, width: int, height: int) -> float:
    """Laplacian variance (sharpness) for frame_bytes (RGB24)."""
    gray = _frame_bytes_to_gray(frame_bytes, width, height)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


class SceneSegmenter:
    """
    Wraps VideoScanner to segment by scene (pHash drift + 30s ceiling + 3s debounce)
    and yield one best frame per scene (highest Laplacian sharpness, skipping first 2 frames).
    Supports resume via initial_scene_start_pts, initial_anchor_phash, and discard_until_pts.
    """

    def __init__(
        self,
        input_path: str | Path | VideoScanner,
        *,
        initial_scene_start_pts: float | None = None,
        initial_anchor_phash: str | None = None,
        discard_until_pts: float | None = None,
    ) -> None:
        if isinstance(input_path, VideoScanner):
            self._scanner = input_path
        else:
            self._scanner = VideoScanner(Path(input_path))
        self._width = self._scanner.out_width
        self._height = self._scanner.out_height
        self._initial_scene_start_pts = initial_scene_start_pts
        self._initial_anchor_phash = initial_anchor_phash
        self._discard_until_pts = discard_until_pts

    def iter_scenes(
        self, check_interrupt: Callable[[], bool] | None = None
    ) -> Iterator[tuple[SceneResult | None, tuple[str, float, float, float] | None]]:
        """
        Iterate over closed scenes, yielding one (SceneResult, next_state) per scene (best frame + bounds + keep_reason).
        On EOF the final open scene is closed and yielded with keep_reason=forced.
        If check_interrupt is set and returns True at the start of a frame, raises InterruptedError.
        """
        scene_start_pts: float = 0.0
        anchor_phash: imagehash.ImageHash | None = None
        if self._initial_scene_start_pts is not None:
            scene_start_pts = self._initial_scene_start_pts
        if self._initial_anchor_phash is not None:
            anchor_phash = imagehash.hex_to_hash(self._initial_anchor_phash)
        discard_until: float | None = self._discard_until_pts
        current_best_pts: float = 0.0
        current_best_sharpness: float = -1.0
        current_best_frame_bytes: bytes = b""
        skip_count = SKIP_FRAMES_BEST
        last_pts: float = 0.0
        last_frame_bytes: bytes = b""
        last_frame_sharpness: float = -1.0
        has_eligible_best = False
        seen_any_frame = False

        def close_scene(
            end_pts: float, reason: SceneKeepReason, next_anchor_phash: imagehash.ImageHash | None, next_pts: float
        ) -> Iterator[tuple[SceneResult | None, tuple[str, float, float, float] | None]]:
            nonlocal scene_start_pts, anchor_phash, current_best_pts, current_best_sharpness
            nonlocal current_best_frame_bytes, skip_count, has_eligible_best
            next_state: tuple[str, float, float, float] | None = None
            if next_anchor_phash is not None:
                next_state = (str(next_anchor_phash), next_pts, next_pts, -1.0)
            if has_eligible_best and current_best_frame_bytes:
                yield (
                    SceneResult(
                        best_frame_bytes=current_best_frame_bytes,
                        best_pts=current_best_pts,
                        scene_start_pts=scene_start_pts,
                        scene_end_pts=end_pts,
                        keep_reason=reason,
                        sharpness_score=current_best_sharpness,
                    ),
                    next_state,
                )
            elif reason is SceneKeepReason.forced and last_frame_bytes:
                # EOF with no eligible best (e.g. very short scene): persist one scene using last frame.
                yield (
                    SceneResult(
                        best_frame_bytes=last_frame_bytes,
                        best_pts=last_pts,
                        scene_start_pts=scene_start_pts,
                        scene_end_pts=end_pts,
                        keep_reason=reason,
                        sharpness_score=last_frame_sharpness,
                    ),
                    next_state,
                )
            else:
                yield (None, next_state)
            # Reset state for the next scene.
            # Note: This reset state is what gets persisted to the DB as 'video_active_state'.
            # The resume logic relies on this clean slate + the discard_until_pts catch-up loop.
            scene_start_pts = end_pts
            anchor_phash = None
            current_best_pts = end_pts
            current_best_sharpness = -1.0
            current_best_frame_bytes = b""
            skip_count = SKIP_FRAMES_BEST
            has_eligible_best = False

        for frame_bytes, pts in self._scanner.iter_frames():
            if check_interrupt and check_interrupt():
                raise InterruptedError("Pipeline interrupted by worker shutdown.")
            seen_any_frame = True
            last_pts = pts
            last_frame_bytes = frame_bytes
            last_frame_sharpness = _sharpness(frame_bytes, self._width, self._height)
            if discard_until is not None:
                if pts < discard_until:
                    continue
                discard_until = None
            pil_img = _frame_bytes_to_pil(frame_bytes, self._width, self._height)
            frame_phash = imagehash.phash(pil_img, hash_size=PHASH_HASH_SIZE)

            if anchor_phash is None:
                anchor_phash = frame_phash
                scene_start_pts = pts
                skip_count = SKIP_FRAMES_BEST
                current_best_pts = pts
                current_best_sharpness = -1.0
                current_best_frame_bytes = b""
                has_eligible_best = False

            reason = _trigger_keep_reason(anchor_phash, scene_start_pts, frame_phash, pts)
            if reason is not None:
                yield from close_scene(pts, reason, frame_phash, pts)
                anchor_phash = frame_phash
                scene_start_pts = pts
                skip_count = SKIP_FRAMES_BEST
                current_best_pts = pts
                current_best_sharpness = -1.0
                current_best_frame_bytes = b""
                has_eligible_best = False

            if skip_count > 0:
                skip_count -= 1
            else:
                shp = _sharpness(frame_bytes, self._width, self._height)
                if shp > current_best_sharpness:
                    current_best_sharpness = shp
                    current_best_pts = pts
                    current_best_frame_bytes = frame_bytes
                    has_eligible_best = True

        if seen_any_frame:
            yield from close_scene(last_pts, SceneKeepReason.forced, None, last_pts)
