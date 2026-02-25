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
    ) -> list[tuple[Asset, float]]:
        """
        Search assets by full-text query on visual_analysis (global and/or OCR).
        When library_slug is set, only assets from that non-deleted library are returned.
        When a query is present, results are ordered by relevance (ts_rank_cd); otherwise by mtime descending.
        Returns list of (Asset, rank) tuples; rank is 0.0 when no FTS query is used.
        """
        has_fts = query_string is not None or ocr_query is not None
        rank_expr = None
        if query_string is not None:
            vector = func.to_tsvector("english", Asset.visual_analysis)
            ts_query = func.websearch_to_tsquery("english", query_string)
            rank_expr = func.ts_rank_cd(vector, ts_query, 1)
        if ocr_query is not None:
            ocr_text_col = Asset.visual_analysis["ocr_text"].astext
            ocr_vector = func.to_tsvector("english", func.coalesce(ocr_text_col, ""))
            ocr_ts_query = func.websearch_to_tsquery("english", ocr_query)
            rank_ocr = func.ts_rank_cd(ocr_vector, ocr_ts_query, 1)
            rank_expr = rank_ocr if rank_expr is None else rank_expr + rank_ocr

        if has_fts:
            stmt = select(Asset, rank_expr.label("rank"))
        else:
            stmt = select(Asset)

        if library_slug is not None:
            stmt = stmt.join(Library, Asset.library_id == Library.slug).where(
                Library.slug == library_slug,
                Library.deleted_at.is_(None),
            )

        if has_fts:
            stmt = stmt.where(Asset.visual_analysis.isnot(None))

        if query_string is not None:
            stmt = stmt.where(vector.op("@@")(ts_query))

        if ocr_query is not None:
            stmt = stmt.where(ocr_vector.op("@@")(ocr_ts_query))

        if has_fts:
            stmt = stmt.order_by(rank_expr.desc())
        else:
            stmt = stmt.order_by(Asset.mtime.desc())
        stmt = stmt.limit(limit)

        with self._session_scope(write=False) as session:
            rows = session.execute(stmt).all()
            if has_fts:
                return [(row[0], float(row[1])) for row in rows]
            return [(row[0], 0.0) for row in rows]
