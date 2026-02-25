"""Asset and library scan repository: upsert assets, claim library for scanning, set scan status."""

from collections.abc import Sequence
from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.models.entities import Asset, AssetStatus, AssetType, Library, ScanStatus


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
