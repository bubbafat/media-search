"""Library CRUD: add, list, soft delete, restore, path lookup."""

import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterator

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.models.entities import Library, ScanStatus


def _slugify(name: str) -> str:
    """URL-safe slug from name: lowercase, non-alphanumeric to hyphen, strip."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "library"


class LibraryRepository:
    """
    Database access for library CRUD and path lookup.
    """

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

    def _and_deleted_clause(self, include_deleted: bool) -> str:
        return "" if include_deleted else " AND deleted_at IS NULL"

    def _where_deleted_clause(self, include_deleted: bool) -> str:
        return "" if include_deleted else " WHERE deleted_at IS NULL"

    def get_absolute_path(self, slug: str, include_deleted: bool = False) -> str | None:
        """Return absolute_path for the library, or None if not found."""
        clause = self._and_deleted_clause(include_deleted)
        with self._session_scope() as session:
            row = session.execute(
                text(f"SELECT absolute_path FROM library WHERE slug = :slug{clause}"),
                {"slug": slug},
            ).fetchone()
        return row[0] if row and row[0] is not None else None

    def get_by_slug(self, slug: str, include_deleted: bool = False) -> Library | None:
        """Return Library for the slug, or None if not found."""
        clause = self._and_deleted_clause(include_deleted)
        with self._session_scope() as session:
            row = session.execute(
                text(
                    "SELECT slug, name, absolute_path, is_active, scan_status, "
                    "target_tagger_id, sampling_limit, deleted_at FROM library WHERE slug = :slug" + clause
                ),
                {"slug": slug},
            ).fetchone()
        if row is None:
            return None
        return self._row_to_library(row)

    def list_libraries(self, include_deleted: bool = False) -> list[Library]:
        """Return libraries, optionally including soft-deleted."""
        clause = self._where_deleted_clause(include_deleted)
        with self._session_scope() as session:
            rows = session.execute(
                text(
                    "SELECT slug, name, absolute_path, is_active, scan_status, "
                    "target_tagger_id, sampling_limit, deleted_at FROM library" + clause + " ORDER BY slug"
                )
            ).fetchall()
        return [self._row_to_library(row) for row in rows]

    def _row_to_library(self, row: tuple) -> Library:
        scan_status = row[4]
        if isinstance(scan_status, str):
            try:
                scan_status = ScanStatus(scan_status)
            except ValueError:
                scan_status = ScanStatus.idle
        return Library(
            slug=row[0],
            name=row[1] or "",
            absolute_path=row[2] or "",
            is_active=row[3],
            scan_status=scan_status,
            target_tagger_id=row[5],
            sampling_limit=row[6] or 100,
            deleted_at=row[7],
        )

    def add(self, name: str, absolute_path: str) -> str:
        """
        Insert a new library. Slug is derived from name (URL-safe).
        Returns the slug. Raises if slug already exists.
        """
        slug = _slugify(name)
        with self._session_scope(write=True) as session:
            row = session.execute(
                text("SELECT deleted_at FROM library WHERE slug = :slug"),
                {"slug": slug},
            ).fetchone()
            if row is not None:
                deleted_at = row[0]
                if deleted_at is None:
                    raise ValueError(f"An active library with the slug '{slug}' already exists.")
                raise ValueError(
                    f"A deleted library with the slug '{slug}' exists in the trash. "
                    "Please restore it or use a different name."
                )
            session.execute(
                text(
                    "INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit) "
                    "VALUES (:slug, :name, :absolute_path, true, 'idle', 100)"
                ),
                {
                    "slug": slug,
                    "name": name,
                    "absolute_path": absolute_path,
                },
            )
        return slug

    def soft_delete(self, slug: str) -> None:
        """Set deleted_at to now (UTC) for the library."""
        now = datetime.now(timezone.utc)
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE library SET deleted_at = :now WHERE slug = :slug"),
                {"now": now, "slug": slug},
            )

    def restore(self, slug: str) -> None:
        """Clear deleted_at for the library."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE library SET deleted_at = NULL WHERE slug = :slug"),
                {"slug": slug},
            )

    def list_trashed(self) -> list[Library]:
        """Return all libraries where deleted_at IS NOT NULL."""
        with self._session_scope() as session:
            rows = session.execute(
                text(
                    "SELECT slug, name, absolute_path, is_active, scan_status, "
                    "target_tagger_id, sampling_limit, deleted_at FROM library "
                    "WHERE deleted_at IS NOT NULL ORDER BY slug"
                )
            ).fetchall()
        return [self._row_to_library(row) for row in rows]

    def hard_delete(self, slug: str) -> None:
        """
        Permanently delete a soft-deleted library and all its assets.
        Raises ValueError if the library does not exist or is not in trash.
        Uses chunked asset deletion to avoid long-held locks.
        """
        with self._session_scope() as session:
            row = session.execute(
                text("SELECT slug, deleted_at FROM library WHERE slug = :slug"),
                {"slug": slug},
            ).fetchone()
        if row is None:
            raise ValueError(f"Library not found: '{slug}'.")
        if row[1] is None:
            raise ValueError(f"Library '{slug}' is not in trash (soft-delete it first).")
        # Delete child rows that reference assets (avoids FK violation when deleting assets)
        with self._session_scope(write=True) as session:
            session.execute(
                text(
                    "DELETE FROM video_scenes WHERE asset_id IN ("
                    "SELECT id FROM asset WHERE library_id = :slug)"
                ),
                {"slug": slug},
            )
            session.execute(
                text(
                    "DELETE FROM video_active_state WHERE asset_id IN ("
                    "SELECT id FROM asset WHERE library_id = :slug)"
                ),
                {"slug": slug},
            )
            session.execute(
                text(
                    "DELETE FROM videoframe WHERE asset_id IN ("
                    "SELECT id FROM asset WHERE library_id = :slug)"
                ),
                {"slug": slug},
            )
        chunk_size = 5000
        while True:
            with self._session_scope(write=True) as session:
                result = session.execute(
                    text(
                        "DELETE FROM asset WHERE id IN ("
                        "SELECT id FROM asset WHERE library_id = :slug LIMIT :limit)"
                    ),
                    {"slug": slug, "limit": chunk_size},
                )
                if result.rowcount == 0:
                    break
        with self._session_scope(write=True) as session:
            session.execute(text("DELETE FROM library WHERE slug = :slug"), {"slug": slug})

    def hard_delete_all_trashed(self) -> int:
        """Permanently delete all soft-deleted libraries and their assets. Returns count deleted."""
        trashed = self.list_trashed()
        for lib in trashed:
            self.hard_delete(lib.slug)
        return len(trashed)

    def get_orphaned_library_slugs(self) -> list[str]:
        """Return distinct library_id values from asset that have no matching library row (orphaned assets)."""
        with self._session_scope() as session:
            rows = session.execute(
                text(
                    "SELECT DISTINCT a.library_id FROM asset a "
                    "WHERE NOT EXISTS (SELECT 1 FROM library l WHERE l.slug = a.library_id) ORDER BY a.library_id"
                )
            ).fetchall()
        return [row[0] for row in rows]

    def get_orphaned_asset_count_for_library(self, library_id: str) -> int:
        """Return number of assets with the given library_id (used for orphan reporting)."""
        with self._session_scope() as session:
            row = session.execute(
                text("SELECT COUNT(*) FROM asset WHERE library_id = :library_id"),
                {"library_id": library_id},
            ).fetchone()
        return row[0] if row else 0

    def delete_orphaned_assets_for_library(self, library_id: str) -> int:
        """
        Delete all assets (and their child rows) for a library_id that has no library row.
        Same order as hard_delete: video_scenes, video_active_state, videoframe, then asset.
        Returns the number of assets deleted.
        """
        with self._session_scope() as session:
            count_row = session.execute(
                text("SELECT COUNT(*) FROM asset WHERE library_id = :library_id"),
                {"library_id": library_id},
            ).fetchone()
            asset_count = count_row[0] if count_row else 0
        if asset_count == 0:
            return 0
        with self._session_scope(write=True) as session:
            session.execute(
                text(
                    "DELETE FROM video_scenes WHERE asset_id IN ("
                    "SELECT id FROM asset WHERE library_id = :library_id)"
                ),
                {"library_id": library_id},
            )
            session.execute(
                text(
                    "DELETE FROM video_active_state WHERE asset_id IN ("
                    "SELECT id FROM asset WHERE library_id = :library_id)"
                ),
                {"library_id": library_id},
            )
            session.execute(
                text(
                    "DELETE FROM videoframe WHERE asset_id IN ("
                    "SELECT id FROM asset WHERE library_id = :library_id)"
                ),
                {"library_id": library_id},
            )
        chunk_size = 5000
        total_deleted = 0
        while True:
            with self._session_scope(write=True) as session:
                result = session.execute(
                    text(
                        "DELETE FROM asset WHERE id IN ("
                        "SELECT id FROM asset WHERE library_id = :library_id LIMIT :limit)"
                    ),
                    {"library_id": library_id, "limit": chunk_size},
                )
                total_deleted += result.rowcount
                if result.rowcount == 0:
                    break
        return total_deleted
