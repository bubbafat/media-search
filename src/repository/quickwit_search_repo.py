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
