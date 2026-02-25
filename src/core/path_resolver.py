"""Resolve library-relative paths to absolute filesystem paths."""

from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.core.config import get_config

_engine = None
_session_factory = None


def _reset_session_factory_for_tests() -> None:
    """Clear cached engine/session factory (for tests that switch DATABASE_URL)."""
    global _engine, _session_factory
    _engine = None
    _session_factory = None


def _get_session_factory():
    """Lazy session factory from config database_url."""
    global _engine, _session_factory
    if _session_factory is None:
        _engine = create_engine(get_config().database_url, pool_pre_ping=True)
        _session_factory = sessionmaker(
            _engine, autocommit=False, autoflush=False, expire_on_commit=False
        )
    return _session_factory


def get_library_root(library_slug: str) -> Path:
    """
    Return the absolute path of the library root for the given slug.
    Fetches absolute_path from the library table (deleted_at IS NULL).
    Raises ValueError if the slug is not found or library is deleted.
    """
    session_factory = _get_session_factory()
    with session_factory() as session:
        row = session.execute(
            text(
                "SELECT absolute_path FROM library WHERE slug = :slug AND deleted_at IS NULL"
            ),
            {"slug": library_slug},
        ).fetchone()
    if row is None or row[0] is None:
        raise ValueError(f"Unknown library slug: {library_slug}")
    return Path(row[0]).resolve()


def resolve_path(library_slug: str, rel_path: str) -> Path:
    """
    Resolve a library-relative path to an absolute Path. Verifies the file exists
    and is under the library root (no path traversal).
    """
    root = get_library_root(library_slug)
    combined = (root / rel_path).resolve()

    # Prevent path traversal: combined must be under root
    try:
        combined.relative_to(root)
    except ValueError:
        raise ValueError(f"Path escapes library root: {rel_path!r}") from None

    if not combined.exists():
        raise FileNotFoundError(f"Path does not exist: {combined}")

    return combined
