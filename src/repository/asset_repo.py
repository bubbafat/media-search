"""Asset and library scan repository: upsert assets, claim library for scanning, set scan status."""

from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.models.entities import AssetType, Library, ScanStatus


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

    def claim_library_for_scanning(self) -> Library | None:
        """
        Find a library with is_active=True and scan_status='scan_req', lock it with
        FOR UPDATE SKIP LOCKED, set scan_status='scanning', and return it.
        """
        with self._session_scope(write=True) as session:
            row = session.execute(
                text("""
                    SELECT slug, name, is_active, scan_status, target_tagger_id, sampling_limit
                    FROM library
                    WHERE is_active = true AND scan_status = 'scan_req'
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
                is_active=row[2],
                scan_status=ScanStatus.scanning,
                target_tagger_id=row[4],
                sampling_limit=row[5] or 100,
            )

    def set_library_scan_status(self, library_slug: str, status: ScanStatus) -> None:
        """Set library scan_status (e.g. back to idle after scan completes)."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("UPDATE library SET scan_status = :status WHERE slug = :slug"),
                {"status": status.value, "slug": library_slug},
            )
