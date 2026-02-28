"""Tests for SceneSegmenter and CompositeStrategy (pHash, debounce, best-frame)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

from src.models.entities import SceneKeepReason
from src.video.scene_segmenter import (
    DEBOUNCE_SEC,
    PHASH_THRESHOLD,
    TEMPORAL_CEILING_SEC,
    SceneResult,
    SceneSegmenter,
    compute_segmentation_version,
    _sharpness,
    _trigger_keep_reason,
)
from src.video.video_scanner import SyncError, VideoScanner

pytestmark = [pytest.mark.fast]


# --- compute_segmentation_version ---


def test_compute_segmentation_version_deterministic():
    """compute_segmentation_version returns deterministic int from PHASH_THRESHOLD and DEBOUNCE_SEC."""
    v = compute_segmentation_version()
    assert isinstance(v, int)
    assert v == PHASH_THRESHOLD * 10000 + int(DEBOUNCE_SEC * 1000)


def test_compute_segmentation_version_changes_with_params():
    """compute_segmentation_version changes when PHASH_THRESHOLD or DEBOUNCE_SEC change."""
    with patch("src.video.scene_segmenter.PHASH_THRESHOLD", 40):
        with patch("src.video.scene_segmenter.DEBOUNCE_SEC", 5.0):
            v = compute_segmentation_version()
    expected = 40 * 10000 + int(5.0 * 1000)
    assert v == expected
    assert v == 405000
    assert v != PHASH_THRESHOLD * 10000 + int(DEBOUNCE_SEC * 1000)


# --- _trigger_keep_reason (CompositeStrategy) ---


def test_trigger_keep_reason_none_anchor_returns_none():
    """When anchor_phash is None, never trigger KEEP."""
    img = Image.new("RGB", (10, 10), color="red")
    import imagehash
    h = imagehash.phash(img, hash_size=16)
    assert _trigger_keep_reason(None, 0.0, h, 100.0) is None


def test_trigger_keep_reason_temporal_ceiling_returns_temporal():
    """When elapsed >= 30s, return temporal regardless of debounce."""
    img = Image.new("RGB", (10, 10), color="red")
    import imagehash
    h = imagehash.phash(img, hash_size=16)
    assert _trigger_keep_reason(h, 0.0, h, TEMPORAL_CEILING_SEC + 1.0) is SceneKeepReason.temporal


def test_trigger_keep_reason_debounce_ignores_early_phash_drift():
    """When Hamming > 51 but elapsed < 3s, do not trigger (debounce)."""
    img = Image.new("RGB", (32, 32), color="red")
    import imagehash
    h1 = imagehash.phash(img, hash_size=16)
    h2 = imagehash.phash(img, hash_size=16)
    with patch("src.video.scene_segmenter._hamming_distance", return_value=52):
        assert _trigger_keep_reason(h1, 0.0, h2, DEBOUNCE_SEC - 0.5) is None


def test_trigger_keep_reason_phash_drift_after_debounce_returns_phash():
    """When Hamming > 51 and elapsed >= 3s, return phash."""
    img = Image.new("RGB", (32, 32), color="red")
    import imagehash
    h1 = imagehash.phash(img, hash_size=16)
    h2 = imagehash.phash(img, hash_size=16)
    with patch("src.video.scene_segmenter._hamming_distance", return_value=52):
        assert _trigger_keep_reason(h1, 0.0, h2, DEBOUNCE_SEC + 0.1) is SceneKeepReason.phash


def test_trigger_keep_reason_small_hamming_returns_none():
    """When Hamming <= 51, do not trigger even after debounce."""
    img = Image.new("RGB", (10, 10), color="red")
    import imagehash
    h = imagehash.phash(img, hash_size=16)
    assert _trigger_keep_reason(h, 0.0, h, 10.0) is None


# --- _sharpness ---


def test_sharpness_returns_float():
    """Laplacian variance is a non-negative float."""
    import numpy as np
    w, h = 48, 27
    frame = np.zeros((h, w, 3), dtype=np.uint8)
    frame[:] = 128
    frame_bytes = frame.tobytes()
    s = _sharpness(frame_bytes, w, h)
    assert isinstance(s, float)
    assert s >= 0


# --- SceneSegmenter with mocked iter_frames ---


def _make_frame_bytes(width: int, height: int, color: tuple[int, int, int] = (128, 128, 128)) -> bytes:
    from PIL import Image
    img = Image.new("RGB", (width, height), color=color)
    return img.tobytes()


def test_scene_segmenter_empty_stream_yields_nothing(tmp_path):
    """When iter_frames yields no frames, iter_scenes yields nothing."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    with patch.object(scanner, "iter_frames", return_value=iter([])):
        segmenter = SceneSegmenter(scanner)
        results = list(segmenter.iter_scenes())
    assert results == []  # no (scene, next_state) pairs


def test_scene_segmenter_single_scene_eof_finalizes(tmp_path):
    """One scene: EOF closes it and yields one SceneResult."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height
    frame_size = w * h * 3
    # 5 frames, same content (no phash drift) -> one scene; best frame after skip 2
    frames = [
        (_make_frame_bytes(w, h), 0.0),
        (_make_frame_bytes(w, h), 1.0),
        (_make_frame_bytes(w, h), 2.0),
        (_make_frame_bytes(w, h), 3.0),
        (_make_frame_bytes(w, h), 4.0),
    ]
    with patch.object(scanner, "iter_frames", return_value=iter(frames)):
        segmenter = SceneSegmenter(scanner)
        results = list(segmenter.iter_scenes())
    assert len(results) == 1
    scene, next_state = results[0]
    assert scene is not None
    r = scene
    assert r.scene_start_pts == 0.0
    assert r.scene_end_pts == 4.0
    assert r.keep_reason is SceneKeepReason.forced
    assert next_state is None
    assert len(r.best_frame_bytes) == frame_size


def test_scene_segmenter_accepts_scanner_instance(tmp_path):
    """SceneSegmenter(input_path) and SceneSegmenter(scanner) both work."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
        segmenter_from_scanner = SceneSegmenter(scanner)
        segmenter_from_path = SceneSegmenter(tmp_path / "v.mp4")
    assert segmenter_from_scanner._scanner is scanner
    assert segmenter_from_path._scanner.out_width == 480


def test_scene_segmenter_check_interrupt_raises_on_first_frame(tmp_path):
    """When check_interrupt returns True at the start of the frame loop, InterruptedError is raised."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height
    frames = [(_make_frame_bytes(w, h), 0.0)]
    with patch.object(scanner, "iter_frames", return_value=iter(frames)):
        segmenter = SceneSegmenter(scanner)
        with pytest.raises(InterruptedError, match="Pipeline interrupted by worker shutdown"):
            list(segmenter.iter_scenes(check_interrupt=lambda: True))


def test_scene_segmenter_check_interrupt_false_yields_normally(tmp_path):
    """When check_interrupt returns False, iter_scenes yields as usual (no InterruptedError)."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height
    # Enough frames so we get one closed scene with an eligible best frame (skip 2 then at least one)
    frames = [
        (_make_frame_bytes(w, h), float(i))
        for i in range(5)
    ]
    with patch.object(scanner, "iter_frames", return_value=iter(frames)):
        segmenter = SceneSegmenter(scanner)
        results = list(segmenter.iter_scenes(check_interrupt=lambda: False))
    assert len(results) == 1
    scene, next_state = results[0]
    assert scene is not None
    assert scene.keep_reason is SceneKeepReason.forced


def test_scene_segmenter_short_video_forced_yields_one_scene_using_last_frame(tmp_path):
    """With only 2 frames we never have has_eligible_best; at EOF we yield one scene using last frame."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height
    frame0 = _make_frame_bytes(w, h, (100, 100, 100))
    frame1 = _make_frame_bytes(w, h, (200, 200, 200))
    frames = [(frame0, 0.0), (frame1, 1.0)]
    with patch.object(scanner, "iter_frames", return_value=iter(frames)):
        segmenter = SceneSegmenter(scanner)
        results = list(segmenter.iter_scenes(check_interrupt=lambda: False))
    assert len(results) == 1
    scene, next_state = results[0]
    assert scene is not None
    assert scene.keep_reason is SceneKeepReason.forced
    assert scene.best_frame_bytes == frame1
    assert scene.best_pts == 1.0
    assert scene.scene_start_pts == 0.0
    assert scene.scene_end_pts == 1.0


def test_scene_segmenter_abrupt_eof_yields_final_scene(tmp_path):
    """When iter_frames raises SyncError mid-stream, iter_scenes yields the final scene first, then SyncError propagates."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height

    def frames_then_sync_error():
        yield (_make_frame_bytes(w, h), 0.0)
        yield (_make_frame_bytes(w, h), 1.0)
        yield (_make_frame_bytes(w, h), 2.0)
        raise SyncError("no PTS from stderr within timeout")

    with patch.object(scanner, "iter_frames", return_value=frames_then_sync_error()):
        segmenter = SceneSegmenter(scanner)
        gen = segmenter.iter_scenes()
        results = [next(gen)]
        with pytest.raises(SyncError, match="no PTS from stderr"):
            next(gen)

    assert len(results) == 1
    scene, next_state = results[0]
    assert scene is not None
    assert scene.keep_reason is SceneKeepReason.forced
    assert scene.scene_end_pts == 2.0
    assert next_state is None


def test_scene_segmenter_eof_extends_to_video_duration(tmp_path):
    """At EOF, when video_duration_sec > last_pts, the final scene's end_pts is extended to duration."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height
    frames = [
        (_make_frame_bytes(w, h), float(i))
        for i in range(6)
    ]
    with patch.object(scanner, "iter_frames", return_value=iter(frames)):
        segmenter = SceneSegmenter(scanner, video_duration_sec=8.0)
        results = list(segmenter.iter_scenes())
    assert len(results) == 1
    scene, next_state = results[0]
    assert scene is not None
    assert scene.scene_start_pts == 0.0
    assert scene.scene_end_pts == 8.0


def test_scene_segmenter_eof_duration_less_than_last_pts_uses_last_pts(tmp_path):
    """When video_duration_sec < last_pts (e.g. bad metadata), use last_pts to avoid shortening."""
    (tmp_path / "v.mp4").write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(tmp_path / "v.mp4")
    w, h = scanner.out_width, scanner.out_height
    frames = [
        (_make_frame_bytes(w, h), float(i))
        for i in range(6)
    ]
    with patch.object(scanner, "iter_frames", return_value=iter(frames)):
        segmenter = SceneSegmenter(scanner, video_duration_sec=3.0)
        results = list(segmenter.iter_scenes())
    assert len(results) == 1
    scene, next_state = results[0]
    assert scene is not None
    assert scene.scene_end_pts == 5.0
