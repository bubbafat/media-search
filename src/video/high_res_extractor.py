"""High-resolution frame extractor: short-lived FFmpeg, MJPEG SOI/EOI parsing, PTS pairing."""

import re
import subprocess
import threading
from pathlib import Path

PTS_REGEX = re.compile(r"pts_time:([\d.]+)")
SOI = bytes([0xFF, 0xD8])
EOI = bytes([0xFF, 0xD9])


def _read_stderr_pts(process: subprocess.Popen[bytes], pts_list: list[tuple[float, str]]) -> None:
    """Read stderr line-by-line; append (pts, full_line) for lines containing showinfo and pts_time."""
    if process.stderr is None:
        return
    try:
        for line in iter(process.stderr.readline, b""):
            try:
                line_str = line.decode("utf-8", errors="replace")
            except Exception:
                continue
            if "showinfo" not in line_str or "pts_time:" not in line_str:
                continue
            m = PTS_REGEX.search(line_str)
            if m:
                try:
                    pts = float(m.group(1))
                    pts_list.append((pts, line_str.strip()))
                except ValueError:
                    pass
    finally:
        pass


def _parse_mjpeg_buffers(stdout_stream: bytes) -> list[bytes]:
    """
    Parse stdout stream into complete MJPEG buffers (SOI ... EOI).
    Incomplete buffer at end is discarded.
    """
    buffers: list[bytes] = []
    i = 0
    while i < len(stdout_stream):
        idx = stdout_stream.find(SOI, i)
        if idx == -1:
            break
        end_idx = stdout_stream.find(EOI, idx + 2)
        if end_idx == -1:
            break
        buffers.append(stdout_stream[idx : end_idx + 2])
        i = end_idx + 2
    return buffers


def extract_frame(
    video_path: str | Path,
    target_pts: float,
    *,
    window_start_offset: float = 0.5,
    window_duration: float = 1.0,
) -> tuple[bytes | None, str | None]:
    """
    Run FFmpeg for a 1s window around target_pts, parse MJPEG buffers and stderr PTS,
    return the single frame whose pts_time is closest to target_pts.
    Returns (mjpeg_bytes, showinfo_line_or_none). If no frame found, returns (None, None).
    Incomplete buffer at EOF is discarded.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise FileNotFoundError(video_path)
    start = max(0.0, target_pts - window_start_offset)
    pts_list: list[tuple[float, str]] = []
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "info",
        "-ss",
        str(start),
        "-t",
        str(window_duration),
        "-i",
        str(video_path),
        "-vf",
        "fps=30,showinfo",
        "-f",
        "image2pipe",
        "-vcodec",
        "mjpeg",
        "-",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )
    stderr_thread = threading.Thread(
        target=_read_stderr_pts,
        args=(proc, pts_list),
        daemon=True,
    )
    stderr_thread.start()
    try:
        stdout_bytes = proc.stdout.read() if proc.stdout else b""
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        except OSError:
            pass
    stderr_thread.join(timeout=2.0)

    buffers = _parse_mjpeg_buffers(stdout_bytes)
    if not buffers or not pts_list:
        return (None, None)
    # Pair by index (FFmpeg emits one showinfo line per frame in order)
    n = min(len(buffers), len(pts_list))
    frames_with_pts: list[tuple[bytes, float, str | None]] = [
        (buffers[i], pts_list[i][0], pts_list[i][1] if pts_list[i][1] else None)
        for i in range(n)
    ]
    best = min(
        frames_with_pts,
        key=lambda x: abs(x[1] - target_pts),
    )
    return (best[0], best[2])


def parse_mjpeg_stream_for_test(
    stdout_stream: bytes,
    pts_list: list[float],
) -> list[tuple[bytes, float]]:
    """
    Test helper: parse MJPEG stream and pair with PTS list by order.
    Returns list of (buffer, pts). Incomplete final buffer discarded.
    """
    buffers = _parse_mjpeg_buffers(stdout_stream)
    n = min(len(buffers), len(pts_list))
    return [(buffers[i], pts_list[i]) for i in range(n)]
