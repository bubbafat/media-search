"""Tests for VideoScanner and ffprobe/dimension helpers."""

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.video.video_scanner import (
    PTS_REGEX,
    SyncError,
    VideoScanner,
    _get_video_dimensions,
    _output_height_and_frame_size,
)

pytestmark = [pytest.mark.fast]


# --- _output_height_and_frame_size (pure, no subprocess) ---


def test_output_height_1920x1080():
    """1920x1080 -> out_height 270 (even), frame_byte_size 480*270*3."""
    out_height, frame_byte_size = _output_height_and_frame_size(1920, 1080)
    assert out_height == 270
    assert frame_byte_size == 480 * 270 * 3


def test_output_height_even_rounding():
    """Ratio that would give odd number gets rounded to even."""
    # 480 * 271 / 480 = 271 -> round(271/2)*2 = 272
    out_height, _ = _output_height_and_frame_size(480, 271)
    assert out_height == 272


def test_output_height_zero_width_raises():
    """src_width 0 raises ValueError."""
    with pytest.raises(ValueError, match="positive"):
        _output_height_and_frame_size(0, 1080)


# --- _get_video_dimensions (needs ffprobe or mock) ---


def test_get_video_dimensions_success(tmp_path):
    """ffprobe csv output is parsed to (width, height)."""
    fake_video = tmp_path / "f.mp4"
    fake_video.write_bytes(b"x")
    with patch("subprocess.run") as run:
        run.return_value = MagicMock(
            stdout="1920,1080\n",
            stderr="",
            returncode=0,
        )
        w, h = _get_video_dimensions(fake_video)
    assert w == 1920
    assert h == 1080


def test_get_video_dimensions_empty_stream_raises(tmp_path):
    """Empty ffprobe output raises ValueError."""
    fake_video = tmp_path / "f.mp4"
    fake_video.write_bytes(b"x")
    with patch("subprocess.run") as run:
        run.return_value = MagicMock(stdout="", stderr="", returncode=0)
        with pytest.raises(ValueError, match="no stream"):
            _get_video_dimensions(fake_video)


def test_get_video_dimensions_invalid_dimensions_raises(tmp_path):
    """Zero or negative dimensions raise ValueError."""
    fake_video = tmp_path / "f.mp4"
    fake_video.write_bytes(b"x")
    with patch("subprocess.run") as run:
        run.return_value = MagicMock(stdout="0,1080\n", stderr="", returncode=0)
        with pytest.raises(ValueError, match="invalid dimensions"):
            _get_video_dimensions(fake_video)


# --- PTS regex (showinfo lines) ---


def test_pts_regex_extracts_float():
    """Lines with showinfo and pts_time: yield the float."""
    line = "[Parsed_showinfo_0 @ 0x123] n: 0 pts: 0 pts_time:1.234567 ..."
    assert "showinfo" in line and "pts_time:" in line
    m = PTS_REGEX.search(line)
    assert m is not None
    assert float(m.group(1)) == 1.234567


def test_pts_regex_integer_pts():
    """pts_time:0 is valid."""
    line = "[Parsed_showinfo_0 @ 0x0] n: 0 pts_time:0 pos: 0"
    m = PTS_REGEX.search(line)
    assert m is not None
    assert float(m.group(1)) == 0.0


# --- VideoScanner constructor ---


def test_video_scanner_missing_file_raises():
    """VideoScanner(missing_path) raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        VideoScanner(Path("/nonexistent/video.mp4"))


def test_video_scanner_sets_frame_byte_size(tmp_path):
    """VideoScanner computes frame_byte_size from ffprobe dimensions."""
    fake_video = tmp_path / "v.mp4"
    fake_video.write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(1920, 1080)):
        scanner = VideoScanner(fake_video)
    assert scanner.out_width == 480
    assert scanner.out_height == 270
    assert scanner.frame_byte_size == 480 * 270 * 3


# --- iter_frames: EOF and SyncError (mocked Popen) ---


def test_iter_frames_eof_after_partial_stdout(tmp_path):
    """When stdout returns 0 bytes, generator exits without error."""
    fake_video = tmp_path / "v.mp4"
    fake_video.write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(100, 100)):
        scanner = VideoScanner(fake_video)
    # 100x100 -> out_height 480, frame_byte_size 480*480*3
    assert scanner.frame_byte_size == 480 * 480 * 3

    first_chunk = b"x" * scanner.frame_byte_size
    eof_chunk = b""
    chunks = [first_chunk, eof_chunk]
    idx = [0]

    class FakeStdout:
        def readinto(self, buf):
            chunk = chunks[min(idx[0], len(chunks) - 1)]
            idx[0] += 1
            n = len(chunk)
            if n > 0 and n <= len(buf):
                buf[:n] = chunk
            return n

    class FakeStderr:
        def readline(self):
            return b""  # EOF immediately

    mock_proc = MagicMock()
    mock_proc.stdout = FakeStdout()
    mock_proc.stderr = FakeStderr()
    mock_proc.poll = MagicMock(return_value=None)
    mock_proc.terminate = MagicMock()
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.kill = MagicMock()

    with patch("subprocess.Popen", return_value=mock_proc):
        frames = list(scanner.iter_frames())
    # One full frame then EOF
    assert len(frames) == 1
    assert len(frames[0][0]) == scanner.frame_byte_size
    # stderr ended first so fallback PTS used (last_pts -1 + 1.0 = 0.0)
    assert frames[0][1] == 0.0


def test_iter_frames_sync_error_when_no_pts(tmp_path):
    """When stdout yields 6+ frames and stderr never sends PTS, SyncError is raised."""
    fake_video = tmp_path / "v.mp4"
    fake_video.write_bytes(b"x")
    with patch("src.video.video_scanner._get_video_dimensions", return_value=(1920, 1080)):
        scanner = VideoScanner(fake_video)
    frame_size = scanner.frame_byte_size
    full_frame = b"x" * frame_size
    call_count = [0]

    class FakeStdout:
        def readinto(self, buf):
            call_count[0] += 1
            if call_count[0] <= 6:
                buf[:frame_size] = full_frame
                return frame_size
            return 0

    block_stderr = threading.Event()  # never set -> readline blocks

    class FakeStderr:
        def readline(self):
            block_stderr.wait()  # block so stderr thread never exits, so no fallback PTS
            return b""

    mock_proc = MagicMock()
    mock_proc.stdout = FakeStdout()
    mock_proc.stderr = FakeStderr()
    mock_proc.terminate = MagicMock()
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.kill = MagicMock()

    with patch("subprocess.Popen", return_value=mock_proc):
        with pytest.raises(SyncError, match="more than 5 frames without PTS"):
            list(scanner.iter_frames())
