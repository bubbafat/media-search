"""Unit tests for app.py helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

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


def test_fast_sync_directory_invalid_path(tmp_path: Path) -> None:
    """fast_sync_directory yields 'Invalid or empty path.' for empty or non-directory path."""
    import sqlite3
    import sqlite_vec

    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    conn.executescript("CREATE TABLE IF NOT EXISTS assets (id INTEGER PRIMARY KEY, path TEXT, hash TEXT, mtime REAL, type TEXT); CREATE TABLE IF NOT EXISTS indexed_directories (id INTEGER PRIMARY KEY, path TEXT, added_at REAL);")
    conn.commit()
    conn.close()

    with patch("app.DEFAULT_DB_PATH", db_path), patch("mediasearch.DEFAULT_DB_PATH", db_path):
        # Empty path
        out = list(app.fast_sync_directory(""))
        assert len(out) >= 1
        log, status, df, paths = out[-1]
        assert "Invalid or empty path" in log

        # Non-directory path (file)
        f = tmp_path / "file.txt"
        f.write_bytes(b"x")
        out2 = list(app.fast_sync_directory(str(f)))
        assert len(out2) >= 1
        log2, _, _, _ = out2[-1]
        assert "Invalid or empty path" in log2
