"""Resolve library-relative paths to absolute filesystem paths."""

from pathlib import Path

from src.core.config import get_config


def resolve_path(library_slug: str, rel_path: str) -> Path:
    """
    Resolve a library-relative path to an absolute Path. Verifies the file exists
    and is under the library root (no path traversal).
    """
    settings = get_config()
    absolute_root = settings.library_roots.get(library_slug)
    if absolute_root is None:
        raise ValueError(f"Unknown library slug: {library_slug}")

    root = Path(absolute_root).resolve()
    combined = (root / rel_path).resolve()

    # Prevent path traversal: combined must be under root
    try:
        combined.relative_to(root)
    except ValueError:
        raise ValueError(f"Path escapes library root: {rel_path!r}") from None

    if not combined.exists():
        raise FileNotFoundError(f"Path does not exist: {combined}")

    return combined
