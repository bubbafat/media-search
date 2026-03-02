"""Repository for library_model_policy: Quickwit index promotion and rollback."""
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterator

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.models.entities import LibraryModelPolicy


class LibraryModelPolicyRepository:

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self, write: bool = False) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            if write:
                session.commit()
        finally:
            session.close()

    def get(self, library_slug: str) -> LibraryModelPolicy | None:
        """Return the policy for a library, or None if not found."""
        with self._session_scope() as s:
            return s.get(LibraryModelPolicy, library_slug)

    def list_all(self) -> list[LibraryModelPolicy]:
        """Return all library_model_policy rows."""
        with self._session_scope() as s:
            result = s.execute(select(LibraryModelPolicy))
            return list(result.scalars().all())

    def get_active_index_names_for_libraries(
        self, library_slugs: list[str] | None
    ) -> list[str]:
        """Return active_index_name for libraries that have a policy with a non-null active index.

        When library_slugs is None, return index names for all such libraries (All Media).
        When library_slugs is a list, return only index names for those slugs; libraries
        with no policy or null active_index_name are omitted. Order is stable (by library_slug).
        """
        with self._session_scope() as s:
            if library_slugs is None:
                result = s.execute(
                    text("""
                        SELECT active_index_name FROM library_model_policy
                        WHERE active_index_name IS NOT NULL
                        ORDER BY library_slug
                    """)
                )
            else:
                result = s.execute(
                    text("""
                        SELECT active_index_name FROM library_model_policy
                        WHERE library_slug = ANY(:slugs) AND active_index_name IS NOT NULL
                        ORDER BY library_slug
                    """),
                    {"slugs": library_slugs},
                )
            return [row[0] for row in result.fetchall()]

    def delete(self, library_slug: str) -> bool:
        """Delete the policy row for the given library_slug. Return True if deleted, False if not found."""
        with self._session_scope(write=True) as s:
            row = s.get(LibraryModelPolicy, library_slug)
            if row is None:
                return False
            s.delete(row)
            return True

    def upsert(self, policy: LibraryModelPolicy) -> None:
        """Insert or fully replace the policy for a library."""
        with self._session_scope(write=True) as s:
            existing = s.get(LibraryModelPolicy, policy.library_slug)
            if existing is None:
                s.add(policy)
            else:
                existing.active_index_name    = policy.active_index_name
                existing.shadow_index_name    = policy.shadow_index_name
                existing.previous_index_name  = policy.previous_index_name
                existing.locked               = policy.locked
                existing.locked_since         = policy.locked_since
                existing.promotion_progress   = policy.promotion_progress

    def promote(self, library_slug: str, shadow_index_name: str) -> None:
        """Promote shadow_index_name to active.

        Moves the current active to previous_index_name.
        Clears shadow, clears lock, sets progress to 1.0.
        """
        with self._session_scope(write=True) as s:
            s.execute(
                text("""
                    UPDATE library_model_policy
                    SET previous_index_name = active_index_name,
                        active_index_name   = :shadow,
                        shadow_index_name   = NULL,
                        locked              = false,
                        locked_since        = NULL,
                        promotion_progress  = 1.0
                    WHERE library_slug = :slug
                """),
                {"shadow": shadow_index_name, "slug": library_slug},
            )

    def rollback(self, library_slug: str) -> None:
        """Restore previous_index_name to active.

        Moves the current active to shadow_index_name so it is not lost.
        Clears lock and resets progress to 1.0.
        Raises ValueError if previous_index_name is NULL.
        """
        with self._session_scope(write=True) as s:
            row = s.get(LibraryModelPolicy, library_slug)
            if row is None or row.previous_index_name is None:
                raise ValueError(
                    f"No previous index to roll back to for library '{library_slug}'"
                )
            s.execute(
                text("""
                    UPDATE library_model_policy
                    SET shadow_index_name   = active_index_name,
                        active_index_name   = previous_index_name,
                        previous_index_name = NULL,
                        locked              = false,
                        locked_since        = NULL,
                        promotion_progress  = 1.0
                    WHERE library_slug = :slug
                """),
                {"slug": library_slug},
            )

    def begin_shadow_indexing(
        self, library_slug: str, shadow_index_name: str
    ) -> None:
        """Set shadow_index_name and lock the library.

        While locked, the API serves active_index_name exclusively.
        """
        with self._session_scope(write=True) as s:
            s.execute(
                text("""
                    UPDATE library_model_policy
                    SET shadow_index_name  = :shadow,
                        locked             = true,
                        locked_since       = :now,
                        promotion_progress = 0.0
                    WHERE library_slug = :slug
                """),
                {
                    "shadow": shadow_index_name,
                    "slug": library_slug,
                    "now": datetime.now(timezone.utc),
                },
            )

    def update_progress(self, library_slug: str, progress: float) -> None:
        """Update promotion_progress. Value is clamped to 0.0–1.0."""
        with self._session_scope(write=True) as s:
            s.execute(
                text("""
                    UPDATE library_model_policy
                    SET promotion_progress = :p
                    WHERE library_slug = :slug
                """),
                {"p": max(0.0, min(1.0, progress)), "slug": library_slug},
            )
