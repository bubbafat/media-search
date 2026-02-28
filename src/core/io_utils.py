"""IO utilities shared across the codebase."""

from pathlib import Path


def file_non_empty(path: Path, *, min_bytes: int = 1) -> bool:
    """Return True if path exists and has at least min_bytes. Catches OSError."""
    try:
        return path.exists() and path.stat().st_size >= min_bytes
    except OSError:
        return False
