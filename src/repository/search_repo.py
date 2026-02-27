"""Search repository: full-text search on asset visual_analysis (vibe and OCR) and video_scenes."""

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterator

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.models.entities import Asset


@dataclass
class SearchResultItem:
    """Single search result: asset, rank, optional best scene timestamp, and match density."""

    asset: Asset
    final_rank: float
    best_scene_ts: float | None  # exact timestamp to jump to for videos
    match_ratio: float  # 1.0 for images, 0.0 to 1.0 for videos
    best_scene_rep_frame_path: str | None  # rep frame path for best-match scene (videos only)


class SearchRepository:
    """
    Database access for full-text search over asset visual_analysis and video_scenes.
    Supports global (vibe) search and OCR-specific search via websearch_to_tsquery.
    Scene-aware: searches both images and video scenes; videos ranked by density (match_ratio).
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
        library_slugs: list[str] | None = None,
        asset_types: list[str] | None = None,
        tag: str | None = None,
        limit: int = 50,
    ) -> list[SearchResultItem]:
        """
        Search assets by full-text query and/or tag.
        When only tag is set, returns assets that have that tag (no FTS). When q/ocr and tag
        are set, restricts FTS results to that tag. library_slugs filters to 1-N libraries.
        asset_types restricts to image and/or video (None = both).
        """
        tag_only = tag is not None and not query_string and not ocr_query
        if tag_only:
            return self._search_assets_by_tag(
                library_slugs=library_slugs,
                asset_types=asset_types,
                tag=tag,
                limit=limit,
            )
        if not query_string and not ocr_query:
            return []

        query = query_string or ocr_query
        if ocr_query is not None:
            image_search_target = "COALESCE(a.visual_analysis->>'ocr_text', '')"
            video_search_target = "COALESCE(s.metadata->'moondream'->>'ocr_text', '')"
        else:
            image_search_target = "a.visual_analysis::text"
            video_search_target = "s.metadata::text"

        if library_slugs:
            library_join = "JOIN library l ON a.library_id = l.slug"
            library_filter = "AND a.library_id = ANY(:lib_slugs) AND l.deleted_at IS NULL"
        else:
            library_join = ""
            library_filter = ""

        type_filter = "AND a.type::text = ANY(:asset_types)" if asset_types else ""

        tag_filter_img = "AND a.visual_analysis->'tags' @> jsonb_build_array(:tag)" if tag else ""
        tag_filter_vid = (
            "AND s.metadata->'moondream'->'tags' @> jsonb_build_array(:tag)" if tag else ""
        )

        sql = f"""
        WITH fts_query AS (
            SELECT websearch_to_tsquery('english', :query) AS q
        ),
        image_hits AS (
            SELECT
                a.id AS asset_id,
                NULL::float AS best_scene_ts,
                1.0::float AS match_ratio,
                ts_rank_cd(to_tsvector('english', {image_search_target}), f.q) AS base_rank,
                NULL::text AS best_rep_frame_path
            FROM asset a CROSS JOIN fts_query f
            {library_join}
            WHERE a.type = 'image' AND a.visual_analysis IS NOT NULL
              AND to_tsvector('english', {image_search_target}) @@ f.q
              {tag_filter_img}
              {library_filter}
              {type_filter}
        ),
        video_hits AS (
            SELECT
                s.asset_id,
                s.start_ts,
                s.end_ts,
                MAX(s.end_ts) OVER (PARTITION BY s.asset_id) AS total_duration,
                ts_rank_cd(to_tsvector('english', {video_search_target}), f.q) AS scene_rank,
                s.rep_frame_path
            FROM video_scenes s CROSS JOIN fts_query f
            JOIN asset a ON s.asset_id = a.id
            {library_join}
            WHERE s.metadata IS NOT NULL
              AND a.type = 'video'
              AND to_tsvector('english', {video_search_target}) @@ f.q
              {tag_filter_vid}
              {library_filter}
              {type_filter}
        ),
        video_agg AS (
            SELECT
                asset_id,
                (ARRAY_AGG(start_ts ORDER BY scene_rank DESC))[1] AS best_scene_ts,
                SUM(end_ts - start_ts) / NULLIF(MAX(total_duration), 0) AS match_ratio,
                MAX(scene_rank) AS base_rank,
                (ARRAY_AGG(rep_frame_path ORDER BY scene_rank DESC))[1] AS best_rep_frame_path
            FROM video_hits
            GROUP BY asset_id
        ),
        combined AS (
            SELECT asset_id, best_scene_ts, match_ratio, base_rank, best_rep_frame_path FROM image_hits
            UNION ALL
            SELECT asset_id, best_scene_ts, match_ratio, base_rank, best_rep_frame_path FROM video_agg
        )
        SELECT
            c.asset_id,
            c.best_scene_ts,
            c.match_ratio,
            (c.base_rank * (1.0 + COALESCE(c.match_ratio, 0) * 2.0)) AS final_rank,
            c.best_rep_frame_path
        FROM combined c
        ORDER BY final_rank DESC
        LIMIT :limit
        """

        params: dict = {"query": query, "limit": limit}
        if library_slugs:
            params["lib_slugs"] = library_slugs
        if asset_types:
            params["asset_types"] = asset_types
        if tag:
            params["tag"] = tag

        return self._execute_search_sql(session_factory_ctx=self._session_scope, sql=sql, params=params)

    def _search_assets_by_tag(
        self,
        library_slugs: list[str] | None = None,
        asset_types: list[str] | None = None,
        tag: str | None = None,
        limit: int = 50,
    ) -> list[SearchResultItem]:
        """Return assets that have the given tag (images: visual_analysis.tags, videos: scene metadata)."""
        if not tag:
            return []

        if library_slugs:
            library_join = "JOIN library l ON a.library_id = l.slug"
            library_filter = "AND a.library_id = ANY(:lib_slugs) AND l.deleted_at IS NULL"
        else:
            library_join = ""
            library_filter = ""

        type_filter = "AND a.type::text = ANY(:asset_types)" if asset_types else ""

        sql = f"""
        WITH image_hits AS (
            SELECT
                a.id AS asset_id,
                NULL::float AS best_scene_ts,
                1.0::float AS match_ratio,
                1.0::float AS base_rank,
                NULL::text AS best_rep_frame_path
            FROM asset a
            {library_join}
            WHERE a.type = 'image' AND a.visual_analysis IS NOT NULL
              AND a.visual_analysis->'tags' @> jsonb_build_array(:tag)
              {library_filter}
              {type_filter}
        ),
        video_hits AS (
            SELECT
                s.asset_id,
                s.start_ts,
                s.end_ts,
                MAX(s.end_ts) OVER (PARTITION BY s.asset_id) AS total_duration,
                1.0::float AS scene_rank,
                s.rep_frame_path
            FROM video_scenes s
            JOIN asset a ON s.asset_id = a.id
            {library_join}
            WHERE s.metadata IS NOT NULL
              AND a.type = 'video'
              AND s.metadata->'moondream'->'tags' @> jsonb_build_array(:tag)
              {library_filter}
              {type_filter}
        ),
        video_agg AS (
            SELECT
                asset_id,
                (ARRAY_AGG(start_ts ORDER BY start_ts))[1] AS best_scene_ts,
                SUM(end_ts - start_ts) / NULLIF(MAX(total_duration), 0) AS match_ratio,
                1.0::float AS base_rank,
                (ARRAY_AGG(rep_frame_path ORDER BY start_ts))[1] AS best_rep_frame_path
            FROM video_hits
            GROUP BY asset_id
        ),
        combined AS (
            SELECT asset_id, best_scene_ts, match_ratio, base_rank, best_rep_frame_path FROM image_hits
            UNION ALL
            SELECT asset_id, best_scene_ts, match_ratio, base_rank, best_rep_frame_path FROM video_agg
        )
        SELECT
            c.asset_id,
            c.best_scene_ts,
            c.match_ratio,
            (c.base_rank * (1.0 + COALESCE(c.match_ratio, 0) * 2.0)) AS final_rank,
            c.best_rep_frame_path
        FROM combined c
        ORDER BY final_rank DESC
        LIMIT :limit
        """
        params: dict = {"tag": tag, "limit": limit}
        if library_slugs:
            params["lib_slugs"] = library_slugs
        if asset_types:
            params["asset_types"] = asset_types
        return self._execute_search_sql(session_factory_ctx=self._session_scope, sql=sql, params=params)

    def _execute_search_sql(
        self,
        *,
        session_factory_ctx,
        sql: str,
        params: dict,
    ) -> list[SearchResultItem]:
        """Run search SQL and map rows to SearchResultItem list."""
        results: list[SearchResultItem] = []
        with session_factory_ctx(write=False) as session:
            rows = session.execute(text(sql), params).fetchall()
            if not rows:
                return []
            asset_ids = [r[0] for r in rows]
            assets = session.execute(select(Asset).where(Asset.id.in_(asset_ids))).scalars().all()
            asset_map = {a.id: a for a in assets}
            for r in rows:
                asset_id, best_ts, ratio, rank, best_rep_path = r
                if asset_id in asset_map:
                    results.append(
                        SearchResultItem(
                            asset=asset_map[asset_id],
                            final_rank=float(rank),
                            best_scene_ts=float(best_ts) if best_ts is not None else None,
                            match_ratio=float(ratio) if ratio is not None else 0.0,
                            best_scene_rep_frame_path=str(best_rep_path) if best_rep_path else None,
                        )
                    )
        return results
