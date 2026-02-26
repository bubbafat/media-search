"""Asset and library scan repository: upsert assets, claim library for scanning, set scan status."""

import re
from collections.abc import Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.core.file_extensions import IMAGE_EXTENSION_SUFFIXES
from src.models.entities import Asset, AssetStatus, AssetType, Library, ScanStatus

DEFAULT_LEASE_SECONDS = 300

# Image extensions matching proxy worker (for repair: assets that should have proxy files)
_REPAIR_IMAGE_PATTERN = r"\." + "|".join(re.escape(s) for s in IMAGE_EXTENSION_SUFFIXES) + r"$"


class AssetRepository:
    """
    Database access for assets and library scan lifecycle.

    Implements upsert_asset with conditional status reset on mtime/size change,
    and claim_library_for_scanning with FOR UPDATE SKIP LOCKED.
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

    def upsert_asset(
        self,
        library_id: str,
        rel_path: str,
        type: AssetType,
        mtime: float,
        size: int,
    ) -> None:
        """
        Insert or update an asset. On conflict (library_id, rel_path), update mtime/size/type.
        Only reset status to 'pending' and clear tags_model_id when mtime or size differs.
        """
        type_val = type.value
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count)
                    VALUES (:library_id, :rel_path, :type, :mtime, :size, 'pending', 0)
                    ON CONFLICT (library_id, rel_path)
                    DO UPDATE SET
                        type = EXCLUDED.type,
                        mtime = EXCLUDED.mtime,
                        size = EXCLUDED.size,
                        status = CASE
                            WHEN asset.mtime IS DISTINCT FROM EXCLUDED.mtime
                                 OR asset.size IS DISTINCT FROM EXCLUDED.size
                            THEN 'pending'
                            ELSE asset.status
                        END,
                        tags_model_id = CASE
                            WHEN asset.mtime IS DISTINCT FROM EXCLUDED.mtime
                                 OR asset.size IS DISTINCT FROM EXCLUDED.size
                            THEN NULL
                            ELSE asset.tags_model_id
                        END
                """),
                {
                    "library_id": library_id,
                    "rel_path": rel_path,
                    "type": type_val,
                    "mtime": mtime,
                    "size": size,
                },
            )

    def claim_library_for_scanning(self, slug: str | None = None) -> Library | None:
        """
        Find a library with is_active=True, deleted_at IS NULL, and scan_status in
        ('full_scan_requested', 'fast_scan_requested'), optionally for a specific slug.
        Lock with FOR UPDATE SKIP LOCKED, set scan_status='scanning', and return it.
        """
        with self._session_scope(write=True) as session:
            if slug is not None:
                row = session.execute(
                    text("""
                        SELECT slug, name, absolute_path, is_active, scan_status, target_tagger_id, sampling_limit
                        FROM library
                        WHERE slug = :slug AND is_active = true AND deleted_at IS NULL
                          AND scan_status IN ('full_scan_requested', 'fast_scan_requested')
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    """),
                    {"slug": slug},
                ).fetchone()
            else:
                row = session.execute(
                    text("""
                        SELECT slug, name, absolute_path, is_active, scan_status, target_tagger_id, sampling_limit
                        FROM library
                        WHERE is_active = true AND deleted_at IS NULL
                          AND scan_status IN ('full_scan_requested', 'fast_scan_requested')
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    """)
                ).fetchone()
            if row is None:
                return None
            session.execute(
                text("UPDATE library SET scan_status = 'scanning' WHERE slug = :slug"),
                {"slug": row[0]},
            )
            return Library(
                slug=row[0],
                name=row[1] or "",
                absolute_path=row[2] or "",
                is_active=row[3],
                scan_status=ScanStatus.scanning,
                target_tagger_id=row[5],
                sampling_limit=row[6] or 100,
            )

    def set_library_scan_status(self, library_slug: str, status: ScanStatus) -> None:
        """Set library scan_status (e.g. back to idle after scan completes)."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE library SET scan_status = :status WHERE slug = :slug"),
                {"status": status.value, "slug": library_slug},
            )

    def count_pending(self, library_slug: str | None = None) -> int:
        """Return count of assets with status pending in non-deleted libraries, optionally for one library."""
        with self._session_scope(write=False) as session:
            q = """
                SELECT COUNT(*) FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL AND a.status = 'pending'
            """
            params: dict = {}
            if library_slug is not None:
                q += " AND a.library_id = :library_slug"
                params["library_slug"] = library_slug
            val = session.execute(text(q), params).scalar()
        return int(val) if val is not None else 0

    def count_pending_proxyable(self, library_slug: str | None = None) -> int:
        """Return count of pending assets that are proxyable (image extensions only)."""
        with self._session_scope(write=False) as session:
            q = """
                SELECT COUNT(*) FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL AND a.status = 'pending'
                  AND a.rel_path ~* :pattern
            """
            params: dict = {"pattern": _REPAIR_IMAGE_PATTERN}
            if library_slug is not None:
                q += " AND a.library_id = :library_slug"
                params["library_slug"] = library_slug
            val = session.execute(text(q), params).scalar()
        return int(val) if val is not None else 0

    def get_asset_ids_expecting_proxy(
        self,
        library_slug: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> Sequence[tuple[int, str]]:
        """
        Return (asset_id, library_slug) for assets that should have proxy/thumbnail files:
        status in (proxied, completed, extracting, analyzing), image extensions only.
        Used by proxy --repair. Optionally filter by library_slug. Ordered by id for stable batching.
        """
        with self._session_scope(write=False) as session:
            q = """
                SELECT a.id, a.library_id
                FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL
                  AND a.status IN ('proxied', 'completed', 'extracting', 'analyzing')
                  AND a.rel_path ~* :pattern
            """
            params: dict = {"pattern": _REPAIR_IMAGE_PATTERN}
            if library_slug is not None:
                q += " AND a.library_id = :library_slug"
                params["library_slug"] = library_slug
            q += " ORDER BY a.id LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset
            rows = session.execute(text(q), params).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]

    def count_assets_by_library(
        self,
        library_id: str,
        status: AssetStatus | None = None,
    ) -> int:
        """Return total count of assets for a library, optionally filtered by status."""
        with self._session_scope(write=False) as session:
            query = select(func.count()).select_from(Asset).where(Asset.library_id == library_id)
            if status is not None:
                query = query.where(Asset.status == status)
            val = session.execute(query).scalar()
        return int(val) if val is not None else 0

    def get_assets_by_library(
        self,
        library_id: str,
        limit: int = 50,
        status: AssetStatus | None = None,
    ) -> Sequence[Asset]:
        """Return assets for a library, optionally filtered by status, ordered by id desc."""
        with self._session_scope(write=False) as session:
            query = select(Asset).where(Asset.library_id == library_id)
            if status is not None:
                query = query.where(Asset.status == status)
            query = query.order_by(Asset.id.desc()).limit(limit)
            return session.execute(query).scalars().all()

    def get_asset(self, library_id: str, rel_path: str) -> Asset | None:
        """
        Return a single asset by library_id and rel_path, or None if not found.
        Only returns assets in non-deleted libraries (join library where deleted_at IS NULL).
        """
        with self._session_scope(write=False) as session:
            query = (
                select(Asset)
                .join(Library, Asset.library_id == Library.slug)
                .where(Asset.library_id == library_id)
                .where(Asset.rel_path == rel_path)
                .where(Library.deleted_at.is_(None))
            )
            return session.execute(query).scalars().unique().one_or_none()

    def get_asset_by_id(self, asset_id: int) -> Asset | None:
        """
        Return a single asset by id, or None if not found.
        Only returns assets in non-deleted libraries (join library where deleted_at IS NULL).
        """
        with self._session_scope(write=False) as session:
            query = (
                select(Asset)
                .join(Library, Asset.library_id == Library.slug)
                .where(Asset.id == asset_id)
                .where(Library.deleted_at.is_(None))
            )
            return session.execute(query).scalars().unique().one_or_none()

    def get_video_asset_ids_by_library(self, library_slug: str) -> list[int]:
        """
        Return asset IDs for all video assets in the library (non-deleted libraries only).
        Used for library-wide video reindex.
        """
        with self._session_scope(write=False) as session:
            rows = session.execute(
                text("""
                    SELECT a.id FROM asset a
                    JOIN library l ON a.library_id = l.slug
                    WHERE l.deleted_at IS NULL AND a.library_id = :slug AND a.type = 'video'
                """),
                {"slug": library_slug},
            ).fetchall()
            return [int(r[0]) for r in rows]

    def get_asset_ids_expecting_reanalysis(
        self,
        effective_target_model_id: int,
        library_slug: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> Sequence[tuple[int, str]]:
        """
        Return (asset_id, library_slug) for assets that should be re-analyzed: status in
        (completed, analyzing), image extensions, and analysis_model_id IS DISTINCT FROM
        effective_target_model_id. Used by AI worker --repair. Optionally filter by library_slug.
        Ordered by id for stable batching.
        """
        with self._session_scope(write=False) as session:
            q = """
                SELECT a.id, a.library_id
                FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL
                  AND a.status IN ('completed', 'analyzing')
                  AND a.rel_path ~* :pattern
                  AND a.analysis_model_id IS DISTINCT FROM :effective_target_model_id
            """
            params: dict = {
                "pattern": _REPAIR_IMAGE_PATTERN,
                "effective_target_model_id": effective_target_model_id,
            }
            if library_slug is not None:
                q += " AND a.library_id = :library_slug"
                params["library_slug"] = library_slug
            q += " ORDER BY a.id LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset
            rows = session.execute(text(q), params).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]

    def claim_asset_by_status(
        self,
        worker_id: str,
        current_status: AssetStatus,
        supported_exts: list[str],
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        library_slug: str | None = None,
        *,
        target_model_id: int | None = None,
        system_default_model_id: int | None = None,
    ) -> Asset | None:
        """
        Claim one asset with status == current_status in a non-deleted library,
        with rel_path ending (case-insensitive) in supported_exts.
        When library_slug is set, only assets from that library are considered.
        When target_model_id and system_default_model_id are set, only claim assets whose
        library's effective target model (COALESCE(library.target_tagger_id, system_default))
        equals target_model_id.
        Uses FOR UPDATE SKIP LOCKED. Sets status=processing, worker_id, lease_expires_at.
        Returns the Asset with library (slug, absolute_path) populated.
        """
        if not supported_exts:
            return None
        # Build regex for rel_path suffix: \.(jpg|jpeg|png|...)$
        suffixes = "|".join(re.escape(ext.lstrip(".")) for ext in supported_exts)
        pattern = r"\." + suffixes + r"$"
        params: dict = {"status": current_status.value, "pattern": pattern}
        library_clause = ""
        if library_slug is not None:
            library_clause = " AND a.library_id = :library_slug"
            params["library_slug"] = library_slug
        effective_target_clause = ""
        if target_model_id is not None and system_default_model_id is not None:
            effective_target_clause = " AND COALESCE(l.target_tagger_id, :system_default_model_id) = :target_model_id"
            params["target_model_id"] = target_model_id
            params["system_default_model_id"] = system_default_model_id
        with self._session_scope(write=True) as session:
            row = session.execute(
                text(f"""
                    SELECT a.id, a.library_id, a.rel_path, a.type, a.mtime, a.size,
                           a.tags_model_id, a.analysis_model_id, a.retry_count, a.error_message,
                           l.slug AS lib_slug, l.absolute_path AS lib_absolute_path
                    FROM asset a
                    JOIN library l ON a.library_id = l.slug
                    WHERE l.deleted_at IS NULL
                      AND a.status = :status
                      AND a.rel_path ~* :pattern
                      {library_clause}
                      {effective_target_clause}
                    FOR UPDATE OF a SKIP LOCKED
                    LIMIT 1
                """),
                params,
            ).fetchone()
            if row is None:
                return None
            asset_id = row[0]
            session.execute(
                text("""
                    UPDATE asset
                    SET status = 'processing', worker_id = :worker_id,
                        lease_expires_at = (NOW() AT TIME ZONE 'UTC') + (:lease_seconds || ' seconds')::interval
                    WHERE id = :id
                """),
                {
                    "worker_id": worker_id,
                    "lease_seconds": lease_seconds,
                    "id": asset_id,
                },
            )
            lease_expires = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
            library = Library(
                slug=row[10],
                name="",
                absolute_path=row[11] or "",
                is_active=True,
                scan_status=ScanStatus.idle,
                target_tagger_id=None,
                sampling_limit=100,
            )
            asset = Asset(
                id=row[0],
                library_id=row[1],
                rel_path=row[2],
                type=AssetType(row[3]),
                mtime=row[4],
                size=row[5],
                status=AssetStatus.processing,
                tags_model_id=row[6],
                analysis_model_id=row[7],
                worker_id=worker_id,
                lease_expires_at=lease_expires,
                retry_count=row[8],
                error_message=row[9],
            )
            asset.library = library
            return asset

    def update_asset_status(
        self,
        asset_id: int,
        status: AssetStatus,
        error_message: str | None = None,
    ) -> None:
        """Set asset status, clear worker_id and lease_expires_at; optionally set error_message."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    UPDATE asset
                    SET status = :status, worker_id = NULL, lease_expires_at = NULL,
                        error_message = :error_message
                    WHERE id = :id
                """),
                {
                    "status": status.value,
                    "error_message": error_message,
                    "id": asset_id,
                },
            )

    def set_preview_path(self, asset_id: int, path: str | None) -> None:
        """Set or clear asset.preview_path (relative to data_dir). Single source of truth for video preview."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE asset SET preview_path = :path WHERE id = :asset_id"),
                {"path": path, "asset_id": asset_id},
            )

    def renew_asset_lease(self, asset_id: int, lease_seconds: int = 300) -> None:
        """Bump the lease_expires_at for an asset currently being processed."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    UPDATE asset
                    SET lease_expires_at = (NOW() AT TIME ZONE 'UTC') + (:lease_seconds || ' seconds')::interval
                    WHERE id = :id
                """),
                {"lease_seconds": lease_seconds, "id": asset_id},
            )

    def mark_completed(self, asset_id: int, analysis_model_id: int) -> None:
        """Set asset to completed, set analysis_model_id, clear worker_id and lease_expires_at."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    UPDATE asset
                    SET status = 'completed', analysis_model_id = :analysis_model_id,
                        worker_id = NULL, lease_expires_at = NULL
                    WHERE id = :id
                """),
                {"analysis_model_id": analysis_model_id, "id": asset_id},
            )
