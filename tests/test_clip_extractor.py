"""Tests for clip_extractor: FFmpeg-based web-safe clip extraction."""

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from src.video.clip_extractor import (
    extract_clip,
    extract_head_clip_copy,
    extract_video_frame,
    probe_video_duration,
    run_ffmpeg_with_progress,
    transcode_to_720p_h264,
    transcode_to_720p_h264_detailed,
)

pytestmark = [pytest.mark.slow]


def _mock_ffmpeg_success(cmd, **kwargs):
    """Mock subprocess.run that writes non-empty output file and returns success."""
    out_path = Path(cmd[-1])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(b"fake output")
    return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")


@pytest.mark.fast
def test_transcode_to_720p_h264_success(tmp_path):
    """transcode_to_720p_h264 returns True when ffmpeg succeeds."""
    source = tmp_path / "in.mov"
    source.write_bytes(b"fake")
    dest = tmp_path / "out.mp4"
    with patch("src.video.clip_extractor._is_h264_videotoolbox_available", return_value=False):
        with patch(
            "src.video.clip_extractor.subprocess.run",
            side_effect=_mock_ffmpeg_success,
        ):
            assert transcode_to_720p_h264(source, dest) is True


@pytest.mark.fast
def test_transcode_to_720p_h264_failure(tmp_path):
    """transcode_to_720p_h264 returns False when ffmpeg fails."""
    source = tmp_path / "in.mov"
    source.write_bytes(b"fake")
    dest = tmp_path / "out.mp4"
    with patch("src.video.clip_extractor._is_h264_videotoolbox_available", return_value=False):
        with patch(
            "src.video.clip_extractor.subprocess.run",
            return_value=subprocess.CompletedProcess([], 1, stdout="", stderr="Error"),
        ):
            assert transcode_to_720p_h264(source, dest) is False


@pytest.mark.fast
def test_transcode_to_720p_h264_videotoolbox_falls_back_to_linx264(tmp_path):
    """When videotoolbox transcode fails, we retry once with libx264."""
    source = tmp_path / "in.mov"
    source.write_bytes(b"fake")
    dest = tmp_path / "out.mp4"
    call_count = [0]

    def mock_run(cmd, **kwargs):
        call_count[0] += 1
        out = Path(cmd[-1])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(b"fake output")
        # Call 1: vt fail, Call 2: libx264 success. Calls 3-4: same for transcode_to_720p_h264.
        is_success = call_count[0] in (2, 4)
        return subprocess.CompletedProcess(
            cmd, 0 if is_success else 1, stdout="", stderr="vt failed"
        )

    with patch("src.video.clip_extractor._is_h264_videotoolbox_available", return_value=True):
        with patch("src.video.clip_extractor.subprocess.run", side_effect=mock_run):
            attempts = transcode_to_720p_h264_detailed(source, dest)
            assert len(attempts) == 2
            assert "h264_videotoolbox" in attempts[0].cmd
            assert "libx264" in attempts[1].cmd
            assert "h264_videotoolbox" in attempts[0].repro
            assert "libx264" in attempts[1].repro
            assert transcode_to_720p_h264(source, dest) is True


@pytest.mark.fast
def test_extract_head_clip_copy_success(tmp_path):
    """extract_head_clip_copy returns True when ffmpeg succeeds."""
    source = tmp_path / "in.mp4"
    source.write_bytes(b"fake")
    dest = tmp_path / "head_clip.mp4"
    with patch(
        "src.video.clip_extractor.subprocess.run",
        side_effect=_mock_ffmpeg_success,
    ):
        assert extract_head_clip_copy(source, dest, duration=10.0) is True


@pytest.mark.fast
def test_extract_head_clip_copy_failure(tmp_path):
    """extract_head_clip_copy returns False when ffmpeg fails."""
    source = tmp_path / "in.mp4"
    source.write_bytes(b"fake")
    dest = tmp_path / "head_clip.mp4"
    with patch("src.video.clip_extractor.subprocess.run", return_value=subprocess.CompletedProcess([], 1, stdout="", stderr="Error")):
        assert extract_head_clip_copy(source, dest, duration=10.0) is False


@pytest.mark.fast
def test_extract_head_clip_copy_0_byte_returns_false(tmp_path):
    """extract_head_clip_copy returns False when ffmpeg exits 0 but produces 0-byte output."""
    source = tmp_path / "in.mp4"
    source.write_bytes(b"fake")
    dest = tmp_path / "head_clip.mp4"
    with patch(
        "src.video.clip_extractor.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout="", stderr=""),
    ):
        assert extract_head_clip_copy(source, dest, duration=10.0) is False
        assert not dest.exists()  # 0-byte file should be unlinked


@pytest.mark.fast
def test_extract_video_frame_0_byte_returns_false(tmp_path):
    """extract_video_frame returns False when ffmpeg exits 0 but produces 0-byte output."""
    source = tmp_path / "in.mp4"
    source.write_bytes(b"fake")
    dest = tmp_path / "thumb.jpg"
    with patch(
        "src.video.clip_extractor.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout="", stderr=""),
    ):
        assert extract_video_frame(source, dest, timestamp=0.0) is False
        assert not dest.exists()


@pytest.mark.fast
def test_transcode_0_byte_returns_false(tmp_path):
    """transcode_to_720p_h264 returns False when ffmpeg exits 0 but produces 0-byte output."""
    source = tmp_path / "in.mov"
    source.write_bytes(b"fake")
    dest = tmp_path / "out.mp4"
    with patch("src.video.clip_extractor._is_h264_videotoolbox_available", return_value=False):
        with patch(
            "src.video.clip_extractor.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, stdout="", stderr=""),
        ):
            assert transcode_to_720p_h264(source, dest) is False
            assert not dest.exists()


def _create_test_video(tmp_path: Path, duration: float = 5.0) -> Path:
    """Create a minimal test video with ffmpeg. Returns path to video."""
    video_path = tmp_path / "test.mp4"
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration}:size=320x240:rate=30",
        "-t",
        str(duration),
        str(video_path),
    ]
    r = subprocess.run(cmd, capture_output=True, timeout=10)
    assert r.returncode == 0, r.stderr.decode()
    assert video_path.exists()
    return video_path


@pytest.mark.slow
def test_probe_video_duration_returns_positive_value(tmp_path):
    """probe_video_duration returns a positive duration for a real test video."""
    source = _create_test_video(tmp_path, duration=3.0)
    duration = probe_video_duration(source)
    assert duration is not None
    assert duration > 0


@pytest.mark.slow
def test_run_ffmpeg_with_progress_reports_progress(tmp_path):
    """run_ffmpeg_with_progress calls on_progress from 0->100% for a short transcode."""
    source = _create_test_video(tmp_path, duration=2.0)
    dest = tmp_path / "out.mp4"
    total_duration = probe_video_duration(source)
    assert total_duration is not None

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source),
        "-vf",
        "scale=-2:720",
        "-c:v",
        "libx264",
        "-b:v",
        "1M",
        "-pix_fmt",
        "yuv420p",
        "-progress",
        "pipe:2",
        "-nostats",
        str(dest),
    ]

    progresses: list[float] = []

    def _on_progress(p: float) -> None:
        progresses.append(p)

    attempt = run_ffmpeg_with_progress(
        cmd,
        total_duration=total_duration,
        on_progress=_on_progress,
    )
    assert attempt.ok
    assert dest.exists()
    assert progresses, "Expected at least one progress callback"
    assert progresses[-1] <= 1.0


@pytest.mark.asyncio
async def test_extract_clip_produces_valid_mp4(tmp_path):
    """extract_clip with real video produces valid MP4 at dest_path."""
    source = _create_test_video(tmp_path, duration=8.0)
    dest = tmp_path / "clips" / "clip_3.mp4"

    result = await extract_clip(source, dest, start_ts=3.0, duration=5.0)

    assert result is True
    assert dest.exists()
    assert dest.stat().st_size > 0


@pytest.mark.asyncio
async def test_extract_clip_safe_start_clamping(tmp_path):
    """safe_start is clamped to 0 when start_ts is near 0."""
    source = _create_test_video(tmp_path, duration=5.0)
    dest = tmp_path / "clips" / "clip_1.mp4"

    # start_ts=1.0 -> safe_start=max(0, -1)=0
    result = await extract_clip(source, dest, start_ts=1.0, duration=3.0)

    assert result is True
    assert dest.exists()


@pytest.mark.asyncio
async def test_extract_clip_invalid_source_returns_false(tmp_path):
    """extract_clip with non-existent source returns False."""
    source = tmp_path / "nonexistent.mp4"
    dest = tmp_path / "out.mp4"

    result = await extract_clip(source, dest, start_ts=0.0)

    assert result is False
    assert not dest.exists()
