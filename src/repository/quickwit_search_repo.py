"""Quickwit-backed search repository.

Communicates with the Quickwit REST API over HTTP using httpx.
The active_index_name is set at construction time and routes all search()
calls to that specific index. search_shadow() bypasses the active index
and queries any named index directly.

search_shadow() is FOR ADMIN/DIAGNOSTIC USE ONLY. It must never be called
from user-facing search endpoints. It exists to compare result quality
between model versions before promotion.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from src.models.similarity import SimilarityScope
from src.repository.search_repo import SearchResultItem

_log = logging.getLogger(__name__)

_SEARCH_FIELDS = ["description", "ocr_text", "tags"]


class QuickwitSearchRepository:

    def __init__(
        self,
        base_url: str,
        active_index_name: str,
        timeout_seconds: float = 5.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._active_index_name = active_index_name
        self._timeout = timeout_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(
        self,
        query_string: str | None,
        library_slugs: list[str] | None = None,
        asset_types: list[str] | None = None,
        limit: int = 50,
    ) -> list[SearchResultItem]:
        """Search the active index.

        Always routes to active_index_name. Returns empty list for
        empty or None query_string.
        """
        if not query_string:
            return []
        return self._query(
            index_name=self._active_index_name,
            query_string=query_string,
            library_slugs=library_slugs,
            asset_types=asset_types,
            limit=limit,
        )

    def search_shadow(
        self,
        index_name: str,
        query_string: str,
        library_slugs: list[str] | None = None,
        limit: int = 50,
    ) -> list[SearchResultItem]:
        """Query a specific index by name regardless of active policy.

        FOR ADMIN/DIAGNOSTIC USE ONLY. Must not be called from
        user-facing search endpoints. Used to compare result quality
        between model versions before promotion.
        """
        return self._query(
            index_name=index_name,
            query_string=query_string,
            library_slugs=library_slugs,
            limit=limit,
        )

    def index_document(self, index_name: str, doc: dict[str, Any]) -> None:
        """Append a single document to the named index.

        Uses NDJSON format (one JSON object per line).
        Documents are written once and never updated.
        """
        url = f"{self._base_url}/api/v1/{index_name}/ingest"
        payload = json.dumps(doc) + "\n"
        resp = httpx.post(
            url,
            content=payload,
            headers={"Content-Type": "application/json"},
            timeout=self._timeout,
        )
        resp.raise_for_status()

    def create_index(self, index_name: str, schema_path: str) -> None:
        """Create a Quickwit index from the schema file at schema_path.

        Reads the schema JSON, replaces index_id with index_name, and
        POSTs to the Quickwit indexes API.
        Raises httpx.HTTPStatusError if the index already exists (HTTP 400).
        """
        with open(schema_path) as f:
            schema = json.load(f)
        schema["index_id"] = index_name
        url = f"{self._base_url}/api/v1/indexes"
        resp = httpx.post(url, json=schema, timeout=self._timeout)
        resp.raise_for_status()

    def delete_index(self, index_name: str) -> None:
        """Delete a Quickwit index by name.

        Used for garbage collection of superseded model indexes.
        """
        url = f"{self._base_url}/api/v1/indexes/{index_name}"
        resp = httpx.delete(url, timeout=self._timeout)
        resp.raise_for_status()

    def is_healthy(self) -> bool:
        """Return True if Quickwit is reachable and reports healthy."""
        try:
            resp = httpx.get(
                f"{self._base_url}/health/livez", timeout=2.0
            )
            return resp.status_code == 200
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _query(
        self,
        index_name: str,
        query_string: str,
        library_slugs: list[str] | None = None,
        asset_types: list[str] | None = None,
        limit: int = 50,
    ) -> list[SearchResultItem]:
        filters: list[str] = []

        if library_slugs:
            slug_list = " OR ".join(
                f"library_slug:{s}" for s in library_slugs
            )
            filters.append(f"({slug_list})")

        if asset_types:
            # asset_type is not a schema field. Video documents have a
            # scene_id; image documents do not. Use a range query to
            # distinguish — Quickwit has no IS NULL operator.
            if asset_types == ["image"]:
                filters.append("NOT scene_id:[1 TO *]")
            elif asset_types == ["video"]:
                filters.append("scene_id:[1 TO *]")

        filter_str = " AND ".join(filters)
        full_query = (
            f"({query_string}) AND {filter_str}"
            if filter_str
            else query_string
        )

        url = f"{self._base_url}/api/v1/{index_name}/search"
        payload = {
            "query": full_query,
            "max_hits": limit,
            "search_field": ",".join(_SEARCH_FIELDS),
        }
        resp = httpx.post(url, json=payload, timeout=self._timeout)
        resp.raise_for_status()
        data = resp.json()
        return [self._hit_to_result(h) for h in data.get("hits", [])]

    @staticmethod
    def _hit_to_result(hit: dict[str, Any]) -> SearchResultItem:
        from src.models.entities import Asset, AssetType, AssetStatus

        # Construct a minimal Asset shell for SearchResultItem compatibility.
        # Full asset details are fetched by the API layer from PostgreSQL.
        # asset.id must be set to asset_id for the API to build URLs.
        # library_id must be set to library_slug for library name resolution.
        asset = Asset(
            id=hit.get("asset_id"),
            library_id=hit.get("library_slug", ""),
            rel_path="",
            type=AssetType.video if hit.get("scene_id") else AssetType.image,
            status=AssetStatus.completed,
            video_preview_path=hit.get("head_clip_path"),
        )
        return SearchResultItem(
            asset=asset,
            final_rank=1.0,
            best_scene_ts=hit.get("scene_start_ts"),
            match_ratio=1.0,
            best_scene_rep_frame_path=hit.get("rep_frame_path"),
        )

    # ------------------------------------------------------------------
    # Similarity search
    # ------------------------------------------------------------------

    def find_similar(
        self,
        description: str,
        exclude_asset_id: int,
        scope: SimilarityScope,
        max_results: int,
        min_score: float,
        floor: float,
        step: float,
        min_results: int,
    ) -> tuple[list[SearchResultItem], float]:
        """Adaptive-threshold similarity search against the active index.

        Implements the retry loop described in the NLE Companion Phase 2 spec:

            threshold = min_score
            while threshold >= floor:
                query with score >= threshold
                if len(results) >= min_results: break
                threshold -= step

        Scope and exclusion filters are applied at the Quickwit query level on
        every attempt. Score filtering is applied client-side using the score
        field returned by Quickwit.
        """
        if not description:
            return [], floor

        threshold = float(min_score)
        floor = float(floor)
        step = float(step)

        if step <= 0.0:
            _log.warning("find_similar called with non-positive step=%s; using single attempt", step)
            step = min_score  # single-iteration safeguard

        base_filter = self._build_scope_filter(scope=scope, exclude_asset_id=exclude_asset_id)

        index_name = self._active_index_name
        last_results: list[SearchResultItem] = []
        last_threshold_used: float = floor

        while threshold >= floor:
            full_query = description
            if base_filter:
                full_query = f"({description}) AND {base_filter}"

            url = f"{self._base_url}/api/v1/{index_name}/search"
            payload = {
                "query": full_query,
                "max_hits": max_results,
                "search_field": ",".join(_SEARCH_FIELDS),
            }
            resp = httpx.post(url, json=payload, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("hits", []) or []

            filtered_hits = [
                h for h in hits
                if float(h.get("score", 0.0)) >= threshold
            ]
            results = [self._hit_to_result(h) for h in filtered_hits]
            last_results = results
            last_threshold_used = threshold

            if len(results) >= min_results:
                break

            threshold -= step

        if not last_results:
            return [], floor
        return last_results, last_threshold_used

    @staticmethod
    def _escape_term(value: str) -> str:
        """Escape a term for Quickwit raw tokenizer fields.

        Wraps the value in double quotes when it contains whitespace or
        reserved characters, escaping internal quotes.
        """
        if not value:
            return value
        if any(ch.isspace() for ch in value) or any(ch in value for ch in '":()'):
            escaped = value.replace('"', '\\"')
            return f'"{escaped}"'
        return value

    def _build_scope_filter(self, scope: SimilarityScope, exclude_asset_id: int) -> str:
        """Build Quickwit filter expression for similarity scope and exclusion."""
        filters: list[str] = []

        # Always exclude the source asset at query level.
        filters.append(f"NOT asset_id:{exclude_asset_id}")

        # Library restriction (when not "all"/None).
        if scope.library and scope.library != "all":
            lib_term = self._escape_term(scope.library)
            filters.append(f"library_slug:{lib_term}")

        # Asset type restriction (image/video) based on scene_id presence.
        at = scope.asset_types
        if isinstance(at, list) and at:
            unique = sorted(set(at))
            if unique == ["image"]:
                filters.append("NOT scene_id:[1 TO *]")
            elif unique == ["video"]:
                filters.append("scene_id:[1 TO *]")

        # Capture timestamp range.
        if scope.date_range is not None:
            dr = scope.date_range
            if dr.from_ts is not None or dr.to_ts is not None:
                start = "*" if dr.from_ts is None else dr.from_ts
                end = "*" if dr.to_ts is None else dr.to_ts
                filters.append(f"capture_ts:[{start} TO {end}]")

        # Minimum sharpness.
        if scope.min_sharpness is not None:
            filters.append(f"sharpness_score:[{scope.min_sharpness} TO *]")

        # Face presence.
        if scope.has_face is not None:
            val = "true" if scope.has_face else "false"
            filters.append(f"has_face:{val}")

        # Camera constraints: OR across items, AND within each.
        if scope.cameras:
            camera_clauses: list[str] = []
            for cam in scope.cameras:
                make = (cam.make or "").strip()
                model = (cam.model or "").strip()
                parts: list[str] = []
                if make:
                    parts.append(f"camera_make:{self._escape_term(make)}")
                if model:
                    parts.append(f"camera_model:{self._escape_term(model)}")
                if parts:
                    camera_clauses.append("(" + " AND ".join(parts) + ")")
            if camera_clauses:
                filters.append("(" + " OR ".join(camera_clauses) + ")")

        return " AND ".join(filters)
