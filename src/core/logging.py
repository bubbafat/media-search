"""Structured logging setup and FlightLogger circular-buffer handler for forensics."""

import logging
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from src.core.config import get_config

FLIGHT_LOG_CAPACITY = 50_000
# Relative to cwd when no config is provided; avoids hardcoded absolute paths (e.g. on Windows/local dev).
DEFAULT_FORENSICS_DIR = Path.cwd() / "logs" / "forensics"


_flight_logger: "FlightLogger | None" = None


class FlightLogger(logging.Handler):
    """
    Circular buffer handler: keeps last 50,000 log records (all levels) in memory.
    dump(worker_id, asset_id=None) writes buffer to logs/forensics/{worker_id}_{timestamp}.log.
    """

    def __init__(
        self,
        capacity: int = FLIGHT_LOG_CAPACITY,
        forensics_dir: str | Path | None = None,
    ) -> None:
        super().__init__(level=logging.DEBUG)
        self._buffer: deque[logging.LogRecord] = deque(maxlen=capacity)
        self._forensics_dir = Path(forensics_dir if forensics_dir is not None else DEFAULT_FORENSICS_DIR)

    def emit(self, record: logging.LogRecord) -> None:
        self._buffer.append(record)

    def dump(
        self,
        worker_id: str,
        asset_id: int | None = None,
    ) -> str:
        """Write buffer to forensics dir; return path to the written file."""
        self._forensics_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        if asset_id is not None:
            name = f"{worker_id}_{asset_id}_{timestamp}.log"
        else:
            name = f"{worker_id}_{timestamp}.log"
        filepath = self._forensics_dir / name
        formatter = self.formatter or logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        with open(filepath, "w") as f:
            for record in self._buffer:
                f.write(formatter.format(record) + "\n")
        return str(filepath)

    def __len__(self) -> int:
        return len(self._buffer)


def get_flight_logger() -> FlightLogger | None:
    """
    Return the global FlightLogger handler created by setup_logging(), if any.

    This is intended primarily for worker lifecycle code (e.g. forensic_dump handling)
    and for tests; application code should generally not mutate handlers directly.
    """
    return _flight_logger


def setup_logging() -> None:
    """
    Configure application logging.

    Invariants:
    - The root logger is set to DEBUG so that all records reach handlers.
    - Console handler logs at WARNING or higher only (no DEBUG/INFO to stdout) to preserve
      SSD IOPS and prevent log bloat; DEBUG/INFO are captured only in the FlightLogger buffer.
    - A FlightLogger handler captures all levels at DEBUG into an in-memory circular buffer.
    """
    global _flight_logger
    cfg = get_config()

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    # Remove existing handlers so we don't duplicate when called again
    for h in root.handlers[:]:
        root.removeHandler(h)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.WARNING)
    console.setFormatter(formatter)
    root.addHandler(console)

    flight = FlightLogger(
        capacity=FLIGHT_LOG_CAPACITY,
        forensics_dir=cfg.forensics_dir,
    )
    flight.setLevel(logging.DEBUG)
    flight.setFormatter(formatter)
    root.addHandler(flight)
    _flight_logger = flight
