"""Pydantic models for similarity search scope."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class CameraSpec(BaseModel):
    make: str | None = None
    model: str | None = None


class DateRange(BaseModel):
    from_ts: float | None = None
    to_ts: float | None = None


class SimilarityScope(BaseModel):
    """Scope for similarity search.

    All fields are optional with permissive defaults. When a field is unset or
    in its permissive state, it should not further restrict the search.
    """

    model_config = {"extra": "ignore"}

    # When "all" or None: no library restriction (all libraries).
    library: str | None = "all"

    # When "all" or None: include both images and videos.
    asset_types: list[Literal["image", "video"]] | Literal["all"] | None = "all"

    # Capture-time range (Unix timestamp seconds), inclusive bounds.
    date_range: DateRange | None = None

    # Minimum sharpness threshold (0.0–1.0) when set.
    min_sharpness: float | None = None

    # Filter by presence of faces when set (True/False).
    has_face: bool | None = None

    # Optional list of camera constraints; OR across items, AND within each.
    cameras: list[CameraSpec] | None = None

