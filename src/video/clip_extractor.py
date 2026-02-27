"""FFmpeg-based extraction of web-safe MP4 clips and single frames for thumbnails."""

import asyncio
import logging
import subprocess
import sys
from pathlib import Path

_log = logging.getLogger(__name__)


def _is_h264_videotoolbox_available() -> bool:
    """Return True if FFmpeg reports h264_videotoolbox encoder (macOS)."""
    if sys.platform != "darwin":
        return False
    result = subprocess.run(
        ["ffmpeg", "-hide_banner", "-encoders"],
        capture_output=True,
        text=True,
        check=False,
    )
    return "h264_videotoolbox" in (result.stdout or "") if result.returncode == 0 else False


def transcode_to_720p_h264(source: Path, dest: Path) -> bool:
    """
    Create a temporary 720p 8-bit H.264 MP4 from a source video.

    Uses h264_videotoolbox on macOS when available, otherwise libx264.
    Scale: -2:720 (height 720, width auto even), -b:v 3M, -pix_fmt yuv420p.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    use_vt = _is_h264_videotoolbox_available()
    if use_vt:
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
            "h264_videotoolbox",
            "-b:v",
            "3M",
            "-pix_fmt",
            "yuv420p",
            str(dest),
        ]
    else:
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
            "-preset",
            "veryfast",
            "-b:v",
            "3M",
            "-pix_fmt",
            "yuv420p",
            str(dest),
        ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True
    if result.stderr:
        _log.warning("FFmpeg 720p transcode failed: %s", result.stderr.strip())
    return False


def extract_head_clip_copy(source: Path, dest: Path, duration: float = 10.0) -> bool:
    """
    Extract the first N seconds from an MP4 using stream copy (no re-encode).

    Uses -ss 0 -i source -t duration -c copy -movflags +faststart for speed.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        "0",
        "-i",
        str(source),
        "-t",
        str(duration),
        "-c",
        "copy",
        "-movflags",
        "+faststart",
        str(dest),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True
    if result.stderr:
        _log.warning("FFmpeg head-clip copy failed: %s", result.stderr.strip())
    return False


def extract_video_frame(source: Path, dest: Path, timestamp: float = 0.0) -> bool:
    """
    Extract a single high-quality JPEG frame from a video at the given timestamp.

    Uses fast-seeking (-ss before -i). Reads only from source; writes only to dest.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        str(timestamp),
        "-i",
        str(source),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        "-vf",
        "scale='min(1280,iw)':-2",
        str(dest),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True
    if result.stderr:
        _log.warning("FFmpeg frame extraction failed: %s", result.stderr.strip())
    return False


def extract_video_clip(
    source: Path,
    dest: Path,
    start_ts: float,
    duration: float = 10.0,
    *,
    context_seconds: float = 0,
) -> bool:
    """
    Extract a web-safe H.264/AAC MP4 clip from a video.

    Uses fast-seeking (-ss before -i) and -movflags +faststart for instant web playback.
    For head clips, pass context_seconds=0 so the clip starts exactly at start_ts.
    For search-hit clips, pass context_seconds=2 to include ~2s context before the hit.
    """
    safe_start = max(0.0, start_ts - context_seconds)
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        str(safe_start),
        "-i",
        str(source),
        "-t",
        str(duration),
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-vf",
        "scale='min(1280,iw)':-2",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(dest),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True
    if result.stderr:
        _log.warning("FFmpeg clip extraction failed: %s", result.stderr.strip())
    return False


async def extract_clip(
    source_path: Path,
    dest_path: Path,
    start_ts: float,
    duration: float = 10.0,
) -> bool:
    """
    Extract a web-safe MP4 clip from a video file using FFmpeg.

    Uses fast-seeking (-ss before -i) and transcodes to H.264/AAC for streaming.
    Reads only from source_path; writes only to dest_path.

    Args:
        source_path: Path to source video (read-only).
        dest_path: Path for output MP4 (writable under data_dir).
        start_ts: Desired start timestamp in seconds; clip starts ~2s before for context.
        duration: Clip length in seconds (default 10.0).

    Returns:
        True if extraction succeeded, False otherwise.
    """
    safe_start = max(0.0, start_ts - 2.0)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-ss",
        str(safe_start),
        "-i",
        str(source_path),
        "-t",
        str(duration),
        "-map",
        "0:v:0",
        "-map",
        "0:a:0?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-vf",
        "scale='min(1280,iw)':-2",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(dest_path),
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr_bytes = await process.communicate()

    if process.returncode == 0:
        return True
    stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
    if stderr:
        _log.warning("FFmpeg clip extraction failed: %s", stderr)
    return False
