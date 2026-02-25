"""Search repository: full-text search on asset visual_analysis (vibe and OCR)."""

from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.models.entities import Asset, Library


class SearchRepository:
    """
    Database access for full-text search over asset visual_analysis JSONB.
    Supports global (vibe) search and OCR-specific search via websearch_to_tsquery.
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

    def search_assets(
        self,
        query_string: str | None = None,
        ocr_query: str | None = None,
        library_slug: str | None = None,
        limit: int = 50,
    ) -> list[Asset]:
        """
        Search assets by full-text query on visual_analysis (global and/or OCR).
        When library_slug is set, only assets from that non-deleted library are returned.
        Results are ordered by mtime descending and limited to `limit`.
        """
        stmt = select(Asset)

        if library_slug is not None:
            stmt = stmt.join(Library, Asset.library_id == Library.slug).where(
                Library.slug == library_slug,
                Library.deleted_at.is_(None),
            )

        if query_string is not None or ocr_query is not None:
            stmt = stmt.where(Asset.visual_analysis.isnot(None))

        if query_string is not None:
            vector = func.to_tsvector("english", Asset.visual_analysis)
            ts_query = func.websearch_to_tsquery("english", query_string)
            stmt = stmt.where(vector.op("@@")(ts_query))

        if ocr_query is not None:
            ocr_text_col = Asset.visual_analysis["ocr_text"].astext
            ocr_vector = func.to_tsvector("english", func.coalesce(ocr_text_col, ""))
            ocr_ts_query = func.websearch_to_tsquery("english", ocr_query)
            stmt = stmt.where(ocr_vector.op("@@")(ocr_ts_query))

        stmt = stmt.order_by(Asset.mtime.desc()).limit(limit)

        with self._session_scope(write=False) as session:
            return list(session.execute(stmt).scalars().unique().all())
