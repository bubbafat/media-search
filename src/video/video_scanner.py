"""VideoScanner: persistent FFmpeg pipe for synchronized frame extraction with PTS."""

import re
import subprocess
import threading
from pathlib import Path
from queue import Empty, Queue
from typing import Iterator

PTS_REGEX = re.compile(r"pts_time:([\d.]+)")
PTS_QUEUE_TIMEOUT = 10.0
OUT_WIDTH = 480


class SyncError(Exception):
    """Raised when PTS for the current frame is not received from stderr within the timeout (FFmpeg hung or stderr thread died)."""

    pass


def _get_video_dimensions(input_path: Path) -> tuple[int, int]:
    """Run ffprobe to get source width and height. Returns (width, height)."""
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0",
            str(input_path),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    line = (result.stdout or "").strip()
    if not line:
        raise ValueError(f"ffprobe returned no stream for {input_path}")
    parts = line.split(",")
    if len(parts) != 2:
        raise ValueError(f"ffprobe unexpected output: {line!r}")
    try:
        width = int(parts[0])
        height = int(parts[1])
    except ValueError as e:
        raise ValueError(f"ffprobe unexpected output: {line!r}") from e
    if width <= 0 or height <= 0:
        raise ValueError(f"ffprobe invalid dimensions: {width}x{height}")
    return width, height


def _output_height_and_frame_size(src_width: int, src_height: int) -> tuple[int, int]:
    """Compute even out_height and frame_byte_size for the low-res stream.

    Height is forced to even (round down) so Python and FFmpeg agree; the scanner
    passes explicit dimensions to FFmpeg (scale=w:h) rather than scale=-2.
    """
    if src_width <= 0:
        raise ValueError("source width must be positive")
    scaled = OUT_WIDTH * src_height / src_width
    out_height = (int(scaled) // 2) * 2
    frame_byte_size = OUT_WIDTH * out_height * 3
    return out_height, frame_byte_size


class VideoScanner:
    """
    High-performance frame extractor using a persistent FFmpeg pipe.
    Yields (frame_bytes, pts_seconds) with 1:1 pairing via a dedicated stderr-parsing thread.
    """

    def __init__(
        self,
        input_path: str | Path,
        *,
        start_pts: float | None = None,
    ) -> None:
        self._input_path = Path(input_path)
        if not self._input_path.exists():
            raise FileNotFoundError(self._input_path)
        self._start_pts = start_pts
        src_width, src_height = _get_video_dimensions(self._input_path)
        self._out_height, self._frame_byte_size = _output_height_and_frame_size(
            src_width, src_height
        )
        self._out_width = OUT_WIDTH

    @property
    def frame_byte_size(self) -> int:
        """Exact number of bytes per frame (width * height * 3)."""
        return self._frame_byte_size

    @property
    def out_width(self) -> int:
        """Output frame width (480)."""
        return self._out_width

    @property
    def out_height(self) -> int:
        """Output frame height (even, aspect-preserving)."""
        return self._out_height

    def iter_frames(self) -> Iterator[tuple[bytes, float]]:
        """Iterate over (frame_bytes, pts_seconds). Uses one FFmpeg process and a stderr thread."""
        pts_queue: Queue[float] = Queue()
        stderr_finished = threading.Event()

        def read_stderr(process: subprocess.Popen[bytes]) -> None:
            if process.stderr is None:
                stderr_finished.set()
                return
            try:
                for line in iter(process.stderr.readline, b""):
                    try:
                        line_str = line.decode("utf-8", errors="replace")
                    except Exception:
                        continue
                    if "showinfo" in line_str and "pts_time:" in line_str:
                        m = PTS_REGEX.search(line_str)
                        if m:
                            try:
                                pts_queue.put(float(m.group(1)))
                            except ValueError:
                                pass
            finally:
                stderr_finished.set()

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info",
            "-hwaccel",
            "auto",
        ]
        if self._start_pts is not None:
            cmd.extend(["-ss", str(self._start_pts)])
        cmd.extend(
            [
                "-i",
                str(self._input_path),
                "-vf",
                f"fps=1,scale={self._out_width}:{self._out_height},showinfo",
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgb24",
                "pipe:1",
            ]
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        stderr_thread = threading.Thread(target=read_stderr, args=(proc,), daemon=True)
        stderr_thread.start()

        try:
            buffer = bytearray(self._frame_byte_size)
            last_pts: float = -1.0

            while True:
                n = proc.stdout.readinto(buffer)  # type: ignore[union-attr]
                if n == 0 or n < self._frame_byte_size:
                    break

                frame_bytes = bytes(buffer)

                if stderr_finished.is_set() and pts_queue.empty():
                    last_pts += 1.0
                    yield (frame_bytes, last_pts)
                    continue

                try:
                    pts = pts_queue.get(timeout=PTS_QUEUE_TIMEOUT)
                except Empty:
                    raise SyncError(
                        "no PTS from stderr within timeout (FFmpeg hung or stderr thread died)"
                    )
                yield (frame_bytes, pts)
                last_pts = pts
        finally:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            except OSError:
                pass
