"""Asset and library scan repository: upsert assets, claim library for scanning, set scan status."""

import logging
import re
from collections.abc import Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator, Literal

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.core.file_extensions import (
    IMAGE_EXTENSION_SUFFIXES,
    VIDEO_EXTENSION_SUFFIXES,
)
from src.models.entities import Asset, AssetStatus, AssetType, Library, ScanStatus

_log = logging.getLogger(__name__)

DEFAULT_LEASE_SECONDS = 300

# Image extensions matching proxy worker (for repair)
_REPAIR_IMAGE_PATTERN = rf"\.({'|'.join(re.escape(s) for s in IMAGE_EXTENSION_SUFFIXES)})$"
# Image + video extensions (ProxyWorker handles both)
_REPAIR_PROXYABLE_PATTERN = rf"\.({'|'.join(re.escape(s) for s in IMAGE_EXTENSION_SUFFIXES + VIDEO_EXTENSION_SUFFIXES)})$"
# Video extensions only (for segmentation invalidation)
_REPAIR_VIDEO_PATTERN = rf"\.({'|'.join(re.escape(s) for s in VIDEO_EXTENSION_SUFFIXES)})$"


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
        Only reset status to 'pending', clear tags_model_id, worker_id, and lease_expires_at
        when mtime or size differs (evicts current worker).
        When mtime or size changes for an existing video asset, clear video_scenes,
        video_active_state, preview_path, and video_preview_path to avoid Frankenstein video
        (old scene metadata merged with new content after file replacement).
        """
        type_val = type.value
        params = {
            "library_id": library_id,
            "rel_path": rel_path,
            "type": type_val,
            "mtime": mtime,
            "size": size,
        }
        with self._session_scope(write=True) as session:
            old_row = session.execute(
                text("""
                    SELECT id, type, mtime, size FROM asset
                    WHERE library_id = :library_id AND rel_path = :rel_path
                """),
                params,
            ).fetchone()
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
                        END,
                        worker_id = CASE
                            WHEN asset.mtime IS DISTINCT FROM EXCLUDED.mtime
                                 OR asset.size IS DISTINCT FROM EXCLUDED.size
                            THEN NULL
                            ELSE asset.worker_id
                        END,
                        lease_expires_at = CASE
                            WHEN asset.mtime IS DISTINCT FROM EXCLUDED.mtime
                                 OR asset.size IS DISTINCT FROM EXCLUDED.size
                            THEN NULL
                            ELSE asset.lease_expires_at
                        END
                """),
                params,
            )
            if (
                old_row is not None
                and str(old_row[1]) == "video"
                and (old_row[2] != mtime or old_row[3] != size)
            ):
                asset_id = old_row[0]
                session.execute(
                    text("DELETE FROM video_active_state WHERE asset_id = :asset_id"),
                    {"asset_id": asset_id},
                )
                session.execute(
                    text("DELETE FROM video_scenes WHERE asset_id = :asset_id"),
                    {"asset_id": asset_id},
                )
                session.execute(
                    text("""
                        UPDATE asset SET preview_path = NULL, video_preview_path = NULL
                        WHERE id = :asset_id
                    """),
                    {"asset_id": asset_id},
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

    def count_pending(
        self,
        library_slug: str | None = None,
        *,
        global_scope: bool = False,
    ) -> int:
        """Return count of assets with status pending in non-deleted libraries.
        Pass library_slug for one library, or library_slug=None with global_scope=True for all."""
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
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

    def count_pending_proxyable(
        self,
        library_slug: str | None = None,
        *,
        global_scope: bool = False,
    ) -> int:
        """Return count of pending assets that are proxyable (image + video extensions).
        Pass library_slug for one library, or library_slug=None with global_scope=True for all."""
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        with self._session_scope(write=False) as session:
            q = """
                SELECT COUNT(*) FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL AND a.status = 'pending'
                  AND a.rel_path ~* :pattern
            """
            params: dict = {"pattern": _REPAIR_PROXYABLE_PATTERN}
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
        *,
        global_scope: bool = False,
    ) -> Sequence[tuple[int, str, str]]:
        """
        Return (asset_id, library_slug, type_str) for assets that should have proxy/thumbnail files:
        status in (proxied, analyzed_light, completed, extracting, analyzing), image + video extensions.
        Pass library_slug for one library, or library_slug=None with global_scope=True for all.
        Ordered by id for stable batching.
        """
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        with self._session_scope(write=False) as session:
            q = """
                SELECT a.id, a.library_id, a.type::text
                FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL
                  AND a.status IN ('proxied', 'analyzed_light', 'completed', 'extracting', 'analyzing')
                  AND a.rel_path ~* :pattern
            """
            params: dict = {"pattern": _REPAIR_PROXYABLE_PATTERN}
            if library_slug is not None:
                q += " AND a.library_id = :library_slug"
                params["library_slug"] = library_slug
            q += " ORDER BY a.id LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset
            rows = session.execute(text(q), params).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]

    def get_proxied_video_asset_ids_with_stale_segmentation(
        self,
        current_version: int,
        library_slug: str | None = None,
        limit: int = 50,
        *,
        global_scope: bool = False,
    ) -> list[int]:
        """
        Return asset IDs for video assets with status in (proxied, analyzed_light, completed,
        extracting, analyzing) whose segmentation_version is not NULL and differs from current_version.
        Pass library_slug for one library, or library_slug=None with global_scope=True for all.
        Ordered by id for stable batching.
        """
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        with self._session_scope(write=False) as session:
            q = """
                SELECT a.id FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL
                  AND a.status IN ('proxied', 'analyzed_light', 'completed', 'extracting', 'analyzing')
                  AND a.type = 'video'
                  AND a.rel_path ~* :pattern
                  AND a.segmentation_version IS NOT NULL
                  AND a.segmentation_version != :current_version
            """
            params: dict = {"pattern": _REPAIR_VIDEO_PATTERN, "current_version": current_version, "limit": limit}
            if library_slug is not None:
                q += " AND a.library_id = :library_slug"
                params["library_slug"] = library_slug
            q += " ORDER BY a.id LIMIT :limit"
            rows = session.execute(text(q), params).fetchall()
        return [int(r[0]) for r in rows]

    def set_segmentation_version(self, asset_id: int, version: int) -> None:
        """Set segmentation_version for an asset (e.g. after scene indexing completes)."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE asset SET segmentation_version = :version WHERE id = :asset_id"),
                {"version": version, "asset_id": asset_id},
            )

    def get_all_asset_paths(
        self,
        limit: int = 1000,
        offset: int = 0,
    ) -> Sequence[tuple[int, str, str, bool]]:
        """
        Return (id, library_slug, rel_path, is_in_project) for all assets in non-deleted libraries.
        Ordered by id for stable batching.
        is_in_project is True when the asset is linked to at least one project.
        """
        with self._session_scope(write=False) as session:
            rows = session.execute(
                text("""
                    SELECT a.id, a.library_id, a.rel_path,
                           EXISTS(SELECT 1 FROM project_assets pa WHERE pa.asset_id = a.id) AS is_in_project
                    FROM asset a
                    JOIN library l ON a.library_id = l.slug
                    WHERE l.deleted_at IS NULL
                    ORDER BY a.id
                    LIMIT :limit OFFSET :offset
                """),
                {"limit": limit, "offset": offset},
            ).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2]), bool(r[3])) for r in rows]

    def delete_asset_cascade(self, asset_id: int) -> None:
        """
        Delete an asset and all dependent rows in strict FK order:
        video_active_state, video_scenes, videoframe, project_assets, asset.
        Raises RuntimeError if the asset is linked to a project (prevents maintenance from deleting).
        """
        with self._session_scope(write=True) as session:
            is_in_project = session.execute(
                text("SELECT EXISTS(SELECT 1 FROM project_assets WHERE asset_id = :asset_id)"),
                {"asset_id": asset_id},
            ).scalar()
            if is_in_project:
                raise RuntimeError(
                    f"Asset {asset_id} is linked to a project and cannot be cascade-deleted."
                )
            session.execute(
                text("DELETE FROM video_active_state WHERE asset_id = :asset_id"),
                {"asset_id": asset_id},
            )
            session.execute(
                text("DELETE FROM video_scenes WHERE asset_id = :asset_id"),
                {"asset_id": asset_id},
            )
            session.execute(
                text("DELETE FROM videoframe WHERE asset_id = :asset_id"),
                {"asset_id": asset_id},
            )
            session.execute(
                text("DELETE FROM project_assets WHERE asset_id = :asset_id"),
                {"asset_id": asset_id},
            )
            session.execute(
                text("DELETE FROM asset WHERE id = :asset_id"),
                {"asset_id": asset_id},
            )

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

    def list_assets_for_library(
        self,
        library_slug: str,
        *,
        limit: int = 50,
        offset: int = 0,
        sort_by: Literal["name", "date", "size", "added", "type"] = "date",
        order: Literal["asc", "desc"] = "desc",
        asset_types: list[str] | None = None,
    ) -> Sequence[Asset]:
        """
        Return paginated assets for a library with sort and type filter.
        Joins library and excludes deleted libraries.
        """
        order_dir = "ASC" if order == "asc" else "DESC"
        order_col = {
            "name": "a.rel_path",
            "date": "a.mtime",
            "size": "a.size",
            "added": "a.id",
            "type": "a.type, a.rel_path",
        }[sort_by]
        type_filter = ""
        params: dict = {"slug": library_slug, "limit": limit, "offset": offset}
        if asset_types:
            type_filter = " AND a.type::text = ANY(:asset_types)"
            params["asset_types"] = asset_types

        with self._session_scope(write=False) as session:
            sql = text(
                f"""
                SELECT a.id, a.library_id, a.rel_path, a.type, a.mtime, a.size,
                       a.status, a.tags_model_id, a.analysis_model_id, a.worker_id,
                       a.lease_expires_at, a.retry_count, a.error_message,
                       a.visual_analysis, a.preview_path, a.video_preview_path,
                       a.segmentation_version
                FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL AND a.library_id = :slug
                {type_filter}
                ORDER BY {order_col} {order_dir}
                LIMIT :limit OFFSET :offset
                """
            )
            rows = session.execute(sql, params).fetchall()
            return [
                Asset(
                    id=r[0],
                    library_id=r[1],
                    rel_path=r[2],
                    type=AssetType(r[3]),
                    mtime=float(r[4]) if r[4] is not None else 0.0,
                    size=int(r[5]) if r[5] is not None else 0,
                    status=AssetStatus(r[6]),
                    tags_model_id=r[7],
                    analysis_model_id=r[8],
                    worker_id=r[9],
                    lease_expires_at=r[10],
                    retry_count=r[11] or 0,
                    error_message=r[12],
                    visual_analysis=r[13],
                    preview_path=r[14],
                    video_preview_path=r[15],
                    segmentation_version=r[16],
                )
                for r in rows
            ]

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
        *,
        global_scope: bool = False,
    ) -> Sequence[tuple[int, str]]:
        """
        Return (asset_id, library_slug) for assets that should be re-analyzed: status in
        (completed, analyzed_light, analyzing), image extensions, and analysis_model_id
        IS DISTINCT FROM effective_target_model_id. Pass library_slug for one library,
        or library_slug=None with global_scope=True for all. Ordered by id for stable batching.
        """
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        with self._session_scope(write=False) as session:
            q = """
                SELECT a.id, a.library_id
                FROM asset a
                JOIN library l ON a.library_id = l.slug
                WHERE l.deleted_at IS NULL
                  AND a.status IN ('completed', 'analyzed_light', 'analyzing')
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
        *,
        library_slug: str | None,
        global_scope: bool = False,
        target_model_id: int | None = None,
        system_default_model_id: int | None = None,
    ) -> Asset | None:
        """
        Claim one asset with status == current_status (or expired processing lease) in a non-deleted library,
        with rel_path ending (case-insensitive) in supported_exts.

        library_slug: Restrict to this library. Pass None with global_scope=True for global worker mode.
        target_model_id / system_default_model_id: when both set, restrict to assets whose library's
        effective target model matches.
        Uses FOR UPDATE SKIP LOCKED. Sets status=processing, worker_id, lease_expires_at.
        Returns the Asset with library (slug, absolute_path) populated.
        """
        if not supported_exts:
            return None
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        # Build regex for rel_path suffix: \.(jpg|jpeg|png|...)$
        escaped_exts = [re.escape(ext.lstrip(".")) for ext in supported_exts]
        pattern = rf"\.({'|'.join(escaped_exts)})$"
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
                      AND (a.status = :status OR (a.status = 'processing' AND a.lease_expires_at < (NOW() AT TIME ZONE 'UTC')))
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
                    SET status = 'processing',
                        worker_id = :worker_id,
                        lease_expires_at = (NOW() AT TIME ZONE 'UTC') + (:lease_seconds || ' seconds')::interval
                    WHERE id = :id
                """),
                {
                    "worker_id": worker_id,
                    "lease_seconds": lease_seconds,
                    "id": asset_id,
                },
            )
            retry_count = int(row[8] or 0)
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
                retry_count=retry_count,
                error_message=row[9],
            )
            asset.library = library
            return asset

    def claim_assets_by_status(
        self,
        worker_id: str,
        current_status: AssetStatus,
        supported_exts: list[str],
        limit: int = 1,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        *,
        library_slug: str | None,
        global_scope: bool = False,
        target_model_id: int | None = None,
        system_default_model_id: int | None = None,
    ) -> list[Asset]:
        """
        Claim up to `limit` assets with status == current_status.
        Same filtering and locking as claim_asset_by_status; returns a list of Assets.
        library_slug: Restrict to this library. Pass None with global_scope=True for global mode.
        """
        if not supported_exts or limit < 1:
            return []
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        escaped_exts = [re.escape(ext.lstrip(".")) for ext in supported_exts]
        pattern = rf"\.({'|'.join(escaped_exts)})$"
        params: dict = {"status": current_status.value, "pattern": pattern, "limit": limit}
        library_clause = ""
        if library_slug is not None:
            library_clause = " AND a.library_id = :library_slug"
            params["library_slug"] = library_slug
        effective_target_clause = ""
        if target_model_id is not None and system_default_model_id is not None:
            effective_target_clause = " AND COALESCE(l.target_tagger_id, :system_default_model_id) = :target_model_id"
            params["target_model_id"] = target_model_id
            params["system_default_model_id"] = system_default_model_id
        params["worker_id"] = worker_id
        params["lease_seconds"] = lease_seconds
        with self._session_scope(write=True) as session:
            rows = session.execute(
                text(f"""
                    SELECT a.id, a.library_id, a.rel_path, a.type, a.mtime, a.size,
                           a.tags_model_id, a.analysis_model_id, a.retry_count, a.error_message,
                           l.slug AS lib_slug, l.absolute_path AS lib_absolute_path
                    FROM asset a
                    JOIN library l ON a.library_id = l.slug
                    WHERE l.deleted_at IS NULL
                      AND (a.status = :status OR (a.status = 'processing' AND a.lease_expires_at < (NOW() AT TIME ZONE 'UTC')))
                      AND a.rel_path ~* :pattern
                      {library_clause}
                      {effective_target_clause}
                    FOR UPDATE OF a SKIP LOCKED
                    LIMIT :limit
                """),
                params,
            ).fetchall()
            if not rows:
                return []
            ids = [r[0] for r in rows]
            session.execute(
                text("""
                    UPDATE asset
                    SET status = 'processing',
                        worker_id = :worker_id,
                        lease_expires_at = (NOW() AT TIME ZONE 'UTC') + (:lease_seconds || ' seconds')::interval
                    WHERE id = ANY(:ids)
                """),
                {"worker_id": worker_id, "lease_seconds": lease_seconds, "ids": ids},
            )
            lease_expires = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
            assets: list[Asset] = []
            for r in rows:
                retry_count = int(r[8] or 0)
                library = Library(
                    slug=r[10],
                    name="",
                    absolute_path=r[11] or "",
                    is_active=True,
                    scan_status=ScanStatus.idle,
                    target_tagger_id=None,
                    sampling_limit=100,
                )
                asset = Asset(
                    id=r[0],
                    library_id=r[1],
                    rel_path=r[2],
                    type=AssetType(r[3]),
                    mtime=r[4],
                    size=r[5],
                    status=AssetStatus.processing,
                    tags_model_id=r[6],
                    analysis_model_id=r[7],
                    worker_id=worker_id,
                    lease_expires_at=lease_expires,
                    retry_count=retry_count,
                    error_message=r[9],
                )
                asset.library = library
                assets.append(asset)
            return assets

    def update_asset_status(
        self,
        asset_id: int,
        status: AssetStatus,
        error_message: str | None = None,
        *,
        owned_by: str | None = None,
    ) -> bool:
        """Set asset status, clear worker_id and lease_expires_at; optionally set error_message.
        Increments retry_count on failed/poisoned; resets to 0 on proxied/analyzed_light/completed.
        When owned_by is set, update only if worker_id = owned_by (strict lease ownership).
        Returns True if a row was updated, False otherwise (e.g. evicted worker)."""
        params: dict = {
            "status": status.value,
            "error_message": error_message,
            "id": asset_id,
        }
        where_clause = "WHERE id = :id"
        if owned_by is not None:
            where_clause += " AND worker_id = :owned_by"
            params["owned_by"] = owned_by
        with self._session_scope(write=True) as session:
            result = session.execute(
                text(f"""
                    UPDATE asset
                    SET status = :status, worker_id = NULL, lease_expires_at = NULL,
                        error_message = :error_message,
                        retry_count = CASE
                            WHEN :status IN ('failed', 'poisoned') THEN retry_count + 1
                            WHEN :status IN ('proxied', 'analyzed_light', 'completed') THEN 0
                            ELSE retry_count
                        END
                    {where_clause}
                """),
                params,
            )
            return (result.rowcount or 0) > 0

    def set_preview_path(self, asset_id: int, path: str | None) -> None:
        """Set or clear asset.preview_path (relative to data_dir). Single source of truth for video preview."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE asset SET preview_path = :path WHERE id = :asset_id"),
                {"path": path, "asset_id": asset_id},
            )

    def set_video_preview_path(self, asset_id: int, path: str | None) -> None:
        """Set or clear asset.video_preview_path (relative to data_dir). Path to 10s head-clip MP4."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE asset SET video_preview_path = :path WHERE id = :asset_id"),
                {"path": path, "asset_id": asset_id},
            )

    def get_all_video_preview_paths_excluding_trash(self) -> list[str]:
        """Return video_preview_path for all assets in non-deleted libraries where path is not null."""
        with self._session_scope(write=False) as session:
            rows = session.execute(
                text("""
                    SELECT a.video_preview_path
                    FROM asset a
                    JOIN library l ON l.slug = a.library_id AND l.deleted_at IS NULL
                    WHERE a.video_preview_path IS NOT NULL
                """),
            ).fetchall()
        return [str(r[0]) for r in rows if r[0]]

    def renew_asset_lease(
        self, asset_id: int, lease_seconds: int = 300, *, worker_id: str
    ) -> bool:
        """Bump the lease_expires_at for an asset currently being processed.
        Only renews if worker_id matches (strict lease ownership).
        Returns True if a row was updated, False otherwise (e.g. evicted worker)."""
        with self._session_scope(write=True) as session:
            result = session.execute(
                text("""
                    UPDATE asset
                    SET lease_expires_at = (NOW() AT TIME ZONE 'UTC') + (:lease_seconds || ' seconds')::interval
                    WHERE id = :id AND worker_id = :worker_id
                """),
                {"lease_seconds": lease_seconds, "id": asset_id, "worker_id": worker_id},
            )
            return (result.rowcount or 0) > 0

    def count_stale_leases(
        self,
        *,
        library_slug: str | None = None,
        global_scope: bool = False,
    ) -> int:
        """Count assets stuck in processing with expired leases. Pass library_slug for one library,
        or library_slug=None with global_scope=True for all. Read-only."""
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        with self._session_scope(write=False) as session:
            extra = " AND library_id = :library_slug" if library_slug else ""
            params: dict = {}
            if library_slug:
                params["library_slug"] = library_slug
            val = session.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM asset
                    WHERE status = 'processing'
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at < (NOW() AT TIME ZONE 'UTC')
                    {extra}
                """
                ),
                params,
            ).scalar()
        return int(val) if val is not None else 0

    def reclaim_stale_leases(
        self,
        *,
        library_slug: str | None = None,
        global_scope: bool = False,
    ) -> int:
        """Reset assets stuck in processing with expired leases. Pass library_slug for one library,
        or library_slug=None with global_scope=True for all. Returns count updated.
        Note: asset.status is VARCHAR (per migrations). Do not cast to assetstatus enum."""
        if library_slug is None and not global_scope:
            raise ValueError("Pass library_slug or global_scope=True; implicit global scope is not allowed.")
        with self._session_scope(write=True) as session:
            extra = " AND library_id = :library_slug" if library_slug else ""
            params: dict = {}
            if library_slug:
                params["library_slug"] = library_slug
            result = session.execute(
                text(
                    f"""
                    UPDATE asset
                    SET status = (CASE WHEN retry_count > 5 THEN 'poisoned' ELSE 'pending' END),
                        worker_id = NULL, lease_expires_at = NULL,
                        retry_count = CASE WHEN retry_count > 5 THEN retry_count + 1 ELSE retry_count END,
                        error_message = CASE WHEN retry_count > 5 THEN 'Lease expired (reclaimed)' ELSE error_message END
                    WHERE status = 'processing'
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at < (NOW() AT TIME ZONE 'UTC')
                    {extra}
                """
                ),
                params,
            )
            return result.rowcount or 0

    def reset_poisoned_assets(self, library_slug: str | None = None) -> int:
        """Reset poisoned assets to pending (retry_count=0, error_message=NULL).
        When library_slug is provided, only update assets in that library.
        Returns the count of assets updated."""
        with self._session_scope(write=True) as session:
            extra = " AND library_id = :library_slug" if library_slug else ""
            params: dict = {}
            if library_slug:
                params["library_slug"] = library_slug
            result = session.execute(
                text(
                    f"""
                    UPDATE asset
                    SET status = 'pending', retry_count = 0, error_message = NULL
                    WHERE status = 'poisoned'
                    {extra}
                """
                ),
                params,
            )
            return result.rowcount or 0

    def mark_completed(
        self, asset_id: int, analysis_model_id: int, *, owned_by: str
    ) -> bool:
        """Set asset to completed, set analysis_model_id, clear worker_id, lease_expires_at, reset retry_count.
        Only updates if worker_id = owned_by (strict lease ownership).
        Returns True if a row was updated, False otherwise (e.g. evicted worker)."""
        with self._session_scope(write=True) as session:
            result = session.execute(
                text("""
                    UPDATE asset
                    SET status = 'completed', analysis_model_id = :analysis_model_id,
                        worker_id = NULL, lease_expires_at = NULL, retry_count = 0
                    WHERE id = :id AND worker_id = :owned_by
                """),
                {
                    "analysis_model_id": analysis_model_id,
                    "id": asset_id,
                    "owned_by": owned_by,
                },
            )
            return (result.rowcount or 0) > 0

    def mark_analyzed_light(
        self, asset_id: int, analysis_model_id: int, *, owned_by: str
    ) -> bool:
        """Set asset to analyzed_light, set analysis_model_id, clear worker_id, lease_expires_at, reset retry_count.
        Only updates if worker_id = owned_by (strict lease ownership).
        Returns True if a row was updated, False otherwise (e.g. evicted worker)."""
        with self._session_scope(write=True) as session:
            result = session.execute(
                text("""
                    UPDATE asset
                    SET status = 'analyzed_light', analysis_model_id = :analysis_model_id,
                        worker_id = NULL, lease_expires_at = NULL, retry_count = 0
                    WHERE id = :id AND worker_id = :owned_by
                """),
                {
                    "analysis_model_id": analysis_model_id,
                    "id": asset_id,
                    "owned_by": owned_by,
                },
            )
            return (result.rowcount or 0) > 0
