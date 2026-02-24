"""In-memory flight log (deque, max 50k entries). Dump to disk on forensic_dump or unhandled exception."""

import os
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any

FLIGHT_LOG_CAPACITY = 50_000
FORENSICS_DIR = "/logs/forensics"


class FlightLogger:
    """In-memory ring buffer for worker observability. Capacity 50,000 entries."""

    __slots__ = ("_buffer", "_worker_id")

    def __init__(self, worker_id: str, capacity: int = FLIGHT_LOG_CAPACITY) -> None:
        self._buffer: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._worker_id = worker_id

    def append(self, level: str, message: str, **extra: Any) -> None:
        """Append one log entry (level, message, optional extra fields)."""
        entry = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "message": message,
            **extra,
        }
        self._buffer.append(entry)

    def dump_forensics(self, base_dir: str | Path | None = None) -> str:
        """Write entire buffer to /logs/forensics/{worker_id}_{timestamp}.log. Returns path."""
        base_dir = base_dir or FORENSICS_DIR
        path = Path(base_dir)
        path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filepath = path / f"{self._worker_id}_{timestamp}.log"
        with open(filepath, "w") as f:
            for entry in self._buffer:
                line = f"{entry.get('ts', '')} [{entry.get('level', '')}] {entry.get('message', '')}"
                extras = {k: v for k, v in entry.items() if k not in ("ts", "level", "message")}
                if extras:
                    line += " " + str(extras)
                f.write(line + "\n")
        return str(filepath)

    def __len__(self) -> int:
        return len(self._buffer)
