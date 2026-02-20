"""Unit tests for app.py helpers."""

from __future__ import annotations


# Import app module; avoid launching Gradio
import app


def test_build_score_view_empty() -> None:
    assert app._build_score_view([]) == "_No results._"


def test_build_score_view_with_results() -> None:
    meta_list = [
        {"path": "/a.jpg", "display_path": "/a.jpg", "type": "IMAGE", "distance": 0.12},
        {"path": "/b.jpg", "display_path": "/b.jpg", "type": "IMAGE", "distance": 0.45},
    ]
    text = app._build_score_view(meta_list)
    assert "a.jpg" in text
    assert "b.jpg" in text
    assert "0.1200" in text or "0.12" in text
    assert "0.4500" in text or "0.45" in text
    assert "0.9" not in text or "All distances" not in text  # No warning for good matches


def test_build_score_view_warns_when_all_distances_high() -> None:
    meta_list = [
        {"path": "/x.jpg", "display_path": "/x.jpg", "type": "IMAGE", "distance": 0.95},
        {"path": "/y.jpg", "display_path": "/y.jpg", "type": "IMAGE", "distance": 0.92},
    ]
    text = app._build_score_view(meta_list)
    assert "0.9" in text
    assert "Indexing Incomplete" not in text  # that's _catalog_stats_text
    assert "normalization" in text or "strong match" in text


def test_build_score_view_handles_none_distance() -> None:
    meta_list = [
        {"path": "/a.jpg", "display_path": "/a.jpg", "type": "IMAGE", "distance": None},
    ]
    text = app._build_score_view(meta_list)
    assert "a.jpg" in text
    assert "—" in text  # placeholder for missing distance


def test_catalog_stats_text_complete() -> None:
    text = app._catalog_stats_text(assets_count=100, vec_count=100, missing_thumbnails=0)
    assert "100" in text
    assert "Indexing Incomplete" not in text


def test_catalog_stats_text_incomplete() -> None:
    text = app._catalog_stats_text(assets_count=100, vec_count=50, missing_thumbnails=2)
    assert "100" in text
    assert "50" in text
    assert "2" in text
    assert "Indexing Incomplete" in text


def test_catalog_stats_text_empty_db() -> None:
    text = app._catalog_stats_text(assets_count=0, vec_count=0, missing_thumbnails=0)
    assert "Indexing Incomplete" not in text
