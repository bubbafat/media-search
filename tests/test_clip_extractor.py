"""Tests for clip_extractor: FFmpeg-based web-safe clip extraction."""

import subprocess
from pathlib import Path

import pytest

from src.video.clip_extractor import extract_clip

pytestmark = [pytest.mark.slow]


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
