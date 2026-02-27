"""Video scene indexing: resume-aware pipeline that persists scenes and active state to PostgreSQL."""

from pathlib import Path
from typing import TYPE_CHECKING, Callable

from PIL import Image
from rapidfuzz import fuzz

from src.core.config import get_config
from src.models.entities import SceneKeepReason
from src.repository.video_scene_repo import (
    VideoActiveState,
    VideoSceneRepository,
    VideoSceneRow,
)
from src.video.high_res_extractor import extract_frame as extract_high_res_frame
from src.video.scene_segmenter import SceneSegmenter
from src.video.video_scanner import SyncError, VideoScanner

if TYPE_CHECKING:
    from src.ai.vision_base import BaseVisionAnalyzer

SEMANTIC_DEDUP_RATIO = 85  # token_set_ratio above this flags semantic duplicate


def run_vision_on_scenes(
    asset_id: int,
    library_slug: str,
    repo: VideoSceneRepository,
    vision_analyzer: "BaseVisionAnalyzer",
    *,
    check_interrupt: Callable[[], bool] | None = None,
) -> None:
    """
    Run vision analysis on existing scene rep frames that have NULL description.

    Used by VideoWorker after VideoProxyWorker has persisted scenes from the 720p pipeline.
    Iterates scenes for the asset, loads rep_frame_path from disk, runs vision, updates description/metadata.
    """
    data_dir = Path(get_config().data_dir)
    scenes = repo.list_scenes(asset_id)
    to_process = [s for s in scenes if s.description is None]
    last_written_description: str | None = None
    for scene in to_process:
        if check_interrupt is not None and check_interrupt():
            raise InterruptedError("Vision backfill interrupted")
        rep_path = data_dir / scene.rep_frame_path
        if not rep_path.exists():
            continue
        analysis = vision_analyzer.analyze_image(rep_path)
        description = analysis.description or ""
        metadata: dict = {
            "moondream": {
                "description": analysis.description,
                "tags": analysis.tags,
                "ocr_text": analysis.ocr_text,
            },
        }
        if (
            last_written_description
            and description
            and fuzz.token_set_ratio(last_written_description, description) > SEMANTIC_DEDUP_RATIO
        ):
            metadata["semantic_duplicate"] = True
        repo.update_scene_vision(scene.id, description, metadata)
        last_written_description = description


def _write_rep_frame_jpeg(
    frame_bytes: bytes,
    width: int,
    height: int,
    out_path: Path,
) -> None:
    """Write RGB24 frame_bytes to a JPEG file at out_path.

    This helper intentionally uses Pillow as a boundary for JPEG encoding in the
    video scene pipeline; it is not on the main image proxy hot path that relies
    on libvips/pyvips.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.frombytes("RGB", (width, height), frame_bytes)
    img.save(out_path, "JPEG", quality=85)


def _write_high_res_jpeg(mjpeg_bytes: bytes, out_path: Path) -> None:
    """Write MJPEG bytes (complete JPEG) to out_path."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(mjpeg_bytes)


def run_video_scene_indexing(
    asset_id: int,
    video_path: str | Path,
    library_slug: str,
    repo: VideoSceneRepository,
    *,
    vision_analyzer: "BaseVisionAnalyzer | None" = None,
    on_scene_closed: Callable[[], None] | None = None,
    on_scene_saved: Callable[[Path, float, float], None] | None = None,
    check_interrupt: Callable[[], bool] | None = None,
) -> None:
    """
    Run the scene detection pipeline for one video asset: resume from last max(end_ts) if any,
    persist each closed scene and active state in a single transaction, and clear state on EOF.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(video_path)

    max_end_ts = repo.get_max_end_ts(asset_id)
    active_state = repo.get_active_state(asset_id)

    start_pts: float | None = None
    initial_scene_start_pts: float | None = None
    initial_anchor_phash: str | None = None
    discard_until_pts: float | None = None

    if max_end_ts is not None:
        start_pts = max(0.0, max_end_ts - 2.0)
        discard_until_pts = max_end_ts
        if active_state is not None:
            initial_scene_start_pts = active_state.scene_start_ts
            initial_anchor_phash = active_state.anchor_phash

    data_dir = Path(get_config().data_dir)
    scenes_dir = data_dir / "video_scenes" / library_slug / str(asset_id)

    def _run_once(*, hwaccel: str | None) -> tuple[int, str, str]:
        scanner = VideoScanner(video_path, start_pts=start_pts, hwaccel=hwaccel)
        segmenter = SceneSegmenter(
            scanner,
            initial_scene_start_pts=initial_scene_start_pts,
            initial_anchor_phash=initial_anchor_phash,
            discard_until_pts=discard_until_pts,
        )
        width = scanner.out_width
        height = scanner.out_height
        scenes_saved = 0
        try:
            for scene, next_state in segmenter.iter_scenes(check_interrupt=check_interrupt):
                active_state_for_db: VideoActiveState | None = None
                if next_state is not None:
                    active_state_for_db = VideoActiveState(
                        anchor_phash=next_state[0],
                        scene_start_ts=next_state[1],
                        current_best_pts=next_state[2],
                        current_best_sharpness=next_state[3],
                    )

                if scene is not None:
                    rep_path = scenes_dir / f"{scene.scene_start_pts:.3f}_{scene.scene_end_pts:.3f}.jpg"
                    description: str | None = None
                    metadata: dict | None = None

                    if vision_analyzer is not None:
                        high_res_bytes, showinfo_line = extract_high_res_frame(
                            video_path, scene.best_pts
                        )
                        if high_res_bytes is not None:
                            _write_high_res_jpeg(high_res_bytes, rep_path)
                            analysis = vision_analyzer.analyze_image(rep_path)
                            description = analysis.description or None
                            metadata = {
                                "moondream": {
                                    "description": analysis.description,
                                    "tags": analysis.tags,
                                    "ocr_text": analysis.ocr_text,
                                },
                                "showinfo": showinfo_line,
                            }
                            prev_desc = repo.get_last_scene_description(asset_id)
                            if (
                                prev_desc
                                and description
                                and fuzz.token_set_ratio(prev_desc, description) > SEMANTIC_DEDUP_RATIO
                            ):
                                if metadata is not None:
                                    metadata["semantic_duplicate"] = True
                        else:
                            _write_rep_frame_jpeg(
                                scene.best_frame_bytes, width, height, rep_path
                            )
                    else:
                        _write_rep_frame_jpeg(scene.best_frame_bytes, width, height, rep_path)

                    row = VideoSceneRow(
                        start_ts=scene.scene_start_pts,
                        end_ts=scene.scene_end_pts,
                        description=description,
                        metadata=metadata,
                        sharpness_score=scene.sharpness_score,
                        rep_frame_path=rep_path.relative_to(data_dir).as_posix(),
                        keep_reason=scene.keep_reason.value,
                    )
                    # ARCHITECTURE NOTE: We intentionally only update the active state when a scene CLOSES.
                    # Mid-scene best-frame tracking is kept purely in-memory for performance.
                    # If a crash occurs mid-scene, the resume logic safely rewinds to max_end_ts
                    # and recalculates the unclosed scene's best frame during the catch-up phase.
                    repo.save_scene_and_update_state(asset_id, row, active_state_for_db)
                    scenes_saved += 1
                    if on_scene_saved is not None:
                        on_scene_saved(rep_path, scene.scene_start_pts, scene.scene_end_pts)
                    if on_scene_closed:
                        on_scene_closed()
                else:
                    if active_state_for_db is not None:
                        repo.upsert_active_state(asset_id, active_state_for_db)
                    else:
                        repo.delete_active_state(asset_id)
        except SyncError as e:
            # Treat as a decode/scan failure; caller may retry without hwaccel.
            tail = scanner.stderr_tail() or str(e)
            return 0, scanner.ffmpeg_repro_command(), tail

        return scenes_saved, scanner.ffmpeg_repro_command(), scanner.stderr_tail()

    # Pass 1: hwaccel auto (default)
    scenes_saved, repro_auto, stderr_auto = _run_once(hwaccel="auto")

    if scenes_saved > 0:
        return

    # Pass 2: software decode (no hwaccel)
    scenes_saved2, repro_sw, stderr_sw = _run_once(hwaccel=None)

    if scenes_saved2 > 0:
        return

    # Still no frames/scenes.
    raise ValueError(
        "No frames produced by decoder; video may be unsupported or corrupt\n"
        f"Repro (hwaccel=auto): {repro_auto or '(unavailable)'}\n"
        f"FFmpeg stderr tail (hwaccel=auto):\n{stderr_auto or '(none)'}\n"
        f"Repro (hwaccel=none): {repro_sw or '(unavailable)'}\n"
        f"FFmpeg stderr tail (hwaccel=none):\n{stderr_sw or '(none)'}"
    )
