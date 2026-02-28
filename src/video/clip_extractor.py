"""FFmpeg-based extraction of web-safe MP4 clips and single frames for thumbnails."""

import asyncio
import logging
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from src.core.io_utils import file_non_empty

_log = logging.getLogger(__name__)

_ZERO_BYTE_STDERR_SUFFIX = "\n[media-search] Output file is 0 bytes; treating as failure"
_DEFAULT_STDERR_TAIL_LINES = 40


def _cmd_to_repro(cmd: list[str]) -> str:
    """Render a shell-safe repro command line for copy/paste."""
    return " ".join(shlex.quote(str(c)) for c in cmd)


def _stderr_tail(stderr: str, *, max_lines: int = _DEFAULT_STDERR_TAIL_LINES) -> str:
    if not stderr:
        return ""
    lines = stderr.strip().splitlines()
    tail = lines[-max_lines:] if len(lines) > max_lines else lines
    return "\n".join(tail).strip()


@dataclass(frozen=True)
class FFmpegAttempt:
    cmd: list[str]
    returncode: int
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def repro(self) -> str:
        return _cmd_to_repro(self.cmd)

    def stderr_tail(self, *, max_lines: int = _DEFAULT_STDERR_TAIL_LINES) -> str:
        return _stderr_tail(self.stderr, max_lines=max_lines)


def _run_ffmpeg(cmd: list[str]) -> FFmpegAttempt:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return FFmpegAttempt(cmd=cmd, returncode=int(result.returncode), stderr=result.stderr or "")


def run_ffmpeg_with_progress(
    cmd: list[str],
    *,
    total_duration: float | None = None,
    on_progress: Callable[[float], None] | None = None,
) -> FFmpegAttempt:
    """
    Run FFmpeg while streaming stderr and optionally reporting progress via -progress pipe:2 output.

    Expects the command to include FFmpeg's `-progress pipe:2` option when on_progress is used.
    """
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    stderr_lines: list[str] = []
    last_percent: float | None = None

    try:
        assert process.stderr is not None
        for line in process.stderr:
            stderr_lines.append(line)
            if (
                on_progress is not None
                and total_duration is not None
                and "out_time_ms=" in line
            ):
                try:
                    _, value = line.strip().split("=", 1)
                    out_time_ms = float(value)
                except ValueError:
                    continue
                if total_duration <= 0:
                    continue
                seconds = out_time_ms / 1_000_000.0
                percent = max(0.0, min(1.0, seconds / total_duration))
                if last_percent is None or percent - last_percent >= 0.01:
                    last_percent = percent
                    on_progress(percent)
        process.wait()
    finally:
        if process.stderr is not None:
            process.stderr.close()
    stderr = "".join(stderr_lines).strip()
    return FFmpegAttempt(
        cmd=cmd,
        returncode=int(process.returncode),
        stderr=stderr,
    )


def probe_video_duration(source: Path) -> float | None:
    """
    Return video duration in seconds using ffprobe, or None on failure.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(source),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        if result.stderr:
            _log.warning(
                "ffprobe duration probe failed for %s: %s",
                source,
                result.stderr.strip(),
            )
        return None
    stdout = (result.stdout or "").strip()
    if not stdout:
        return None
    try:
        duration = float(stdout)
    except ValueError:
        _log.warning(
            "ffprobe returned non-numeric duration for %s: %r",
            source,
            stdout,
        )
        return None
    if duration <= 0:
        return None
    return duration


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


def transcode_to_720p_h264_detailed(
    source: Path,
    dest: Path,
    *,
    duration: float | None = None,
    on_progress: Callable[[float], None] | None = None,
) -> list[FFmpegAttempt]:
    """
    Transcode to 720p H.264 MP4, returning a list of FFmpeg attempts (for diagnostics).

    Behavior:
    - On macOS, try `h264_videotoolbox` first (when available).
    - If that fails, retry once with `libx264` (fallback).
    - On other platforms, use `libx264` only.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    attempts: list[FFmpegAttempt] = []

    def _cmd_for(encoder: str) -> list[str]:
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
            encoder,
            "-b:v",
            "3M",
            "-pix_fmt",
            "yuv420p",
        ]
        if encoder == "libx264":
            cmd += ["-preset", "veryfast"]
        cmd.append(str(dest))
        return cmd

    def _run_cmd(encoder: str) -> FFmpegAttempt:
        cmd = _cmd_for(encoder)
        if on_progress is not None and duration is not None:
            cmd = cmd[:-1] + ["-progress", "pipe:2", "-nostats", cmd[-1]]
            return run_ffmpeg_with_progress(
                cmd,
                total_duration=duration,
                on_progress=on_progress,
            )
        return _run_ffmpeg(cmd)

    def _validate_dest() -> None:
        """If last attempt ok but dest is 0-byte, treat as failure."""
        if not attempts or not attempts[-1].ok:
            return
        if file_non_empty(dest):
            return
        dest.unlink(missing_ok=True)
        last = attempts[-1]
        attempts[-1] = FFmpegAttempt(
            cmd=last.cmd,
            returncode=1,
            stderr=(last.stderr or "") + _ZERO_BYTE_STDERR_SUFFIX,
        )

    use_vt = _is_h264_videotoolbox_available()
    if use_vt:
        _log.info(
            "FFmpeg 720p transcode: attempting h264_videotoolbox for %s",
            source,
        )
        attempts.append(_run_cmd("h264_videotoolbox"))
        if attempts[-1].ok:
            _validate_dest()
            if attempts[-1].ok:
                _log.info(
                    "FFmpeg 720p transcode succeeded with h264_videotoolbox for %s",
                    source,
                )
                if on_progress is not None and duration is not None:
                    on_progress(1.0)
                return attempts
        _log.info(
            "FFmpeg 720p transcode with h264_videotoolbox failed for %s, falling back to libx264",
            source,
        )
        attempts.append(_run_cmd("libx264"))
        _validate_dest()
        return attempts

    _log.info("FFmpeg 720p transcode: using libx264 for %s", source)
    attempts.append(_run_cmd("libx264"))
    _validate_dest()
    return attempts


def transcode_to_720p_h264(source: Path, dest: Path) -> bool:
    """
    Create a temporary 720p 8-bit H.264 MP4 from a source video.

    Uses h264_videotoolbox on macOS when available, otherwise libx264.
    Scale: -2:720 (height 720, width auto even), -b:v 3M, -pix_fmt yuv420p.
    """
    attempts = transcode_to_720p_h264_detailed(source, dest)
    if attempts and attempts[-1].ok:
        return True
    if attempts:
        last = attempts[-1]
        _log.warning(
            "FFmpeg 720p transcode failed. Repro: %s\n%s",
            last.repro,
            last.stderr_tail(),
        )
    return False


def extract_head_clip_copy(source: Path, dest: Path, duration: float = 10.0) -> bool:
    """
    Extract the first N seconds from an MP4 using stream copy (no re-encode).

    Uses -ss 0 -i source -t duration -c copy -movflags +faststart for speed.
    """
    attempt = extract_head_clip_copy_detailed(source, dest, duration=duration)
    if attempt.ok:
        return True
    _log.warning(
        "FFmpeg head-clip copy failed. Repro: %s\n%s",
        attempt.repro,
        attempt.stderr_tail(),
    )
    return False


def extract_head_clip_copy_detailed(
    source: Path, dest: Path, duration: float = 10.0
) -> FFmpegAttempt:
    """Like extract_head_clip_copy(), but returns cmd+stderr for diagnostics."""
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
    attempt = _run_ffmpeg(cmd)
    if attempt.ok and not file_non_empty(dest):
        dest.unlink(missing_ok=True)
        return FFmpegAttempt(
            cmd=cmd,
            returncode=1,
            stderr=(attempt.stderr or "") + _ZERO_BYTE_STDERR_SUFFIX,
        )
    return attempt


def extract_video_frame(source: Path, dest: Path, timestamp: float = 0.0) -> bool:
    """
    Extract a single high-quality JPEG frame from a video at the given timestamp.

    Uses fast-seeking (-ss before -i). Reads only from source; writes only to dest.
    """
    attempt = extract_video_frame_detailed(source, dest, timestamp=timestamp)
    if attempt.ok:
        return True
    _log.warning(
        "FFmpeg frame extraction failed. Repro: %s\n%s",
        attempt.repro,
        attempt.stderr_tail(),
    )
    return False


def extract_video_frame_detailed(
    source: Path, dest: Path, timestamp: float = 0.0
) -> FFmpegAttempt:
    """Like extract_video_frame(), but returns cmd+stderr for diagnostics."""
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
    attempt = _run_ffmpeg(cmd)
    if attempt.ok and not file_non_empty(dest):
        dest.unlink(missing_ok=True)
        return FFmpegAttempt(
            cmd=cmd,
            returncode=1,
            stderr=(attempt.stderr or "") + _ZERO_BYTE_STDERR_SUFFIX,
        )
    return attempt


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
        if not file_non_empty(dest):
            dest.unlink(missing_ok=True)
            if result.stderr:
                _log.warning(
                    "FFmpeg clip extraction produced 0-byte output: %s",
                    result.stderr.strip(),
                )
            return False
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
        if not file_non_empty(dest_path):
            dest_path.unlink(missing_ok=True)
            stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
            if stderr:
                _log.warning(
                    "FFmpeg clip extraction produced 0-byte output: %s",
                    stderr,
                )
            return False
        return True
    stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
    if stderr:
        _log.warning("FFmpeg clip extraction failed: %s", stderr)
    return False
