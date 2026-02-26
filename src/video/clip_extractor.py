"""FFmpeg-based extraction of web-safe MP4 clips for search hit verification."""

import asyncio
import logging
from pathlib import Path

_log = logging.getLogger(__name__)


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
