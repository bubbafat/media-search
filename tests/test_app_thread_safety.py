"""
Tests to ensure Gradio handlers do not hit SQLite thread-safety errors.

Gradio runs button clicks, generators, and load callbacks in worker threads.
SQLite connections must not be shared across threads. This module runs each
DB-touching handler in a worker thread to verify no ProgrammingError.

Pattern:
- Handlers that use _db_instance() must only call methods that use _fresh_connection()
  (search, fetch_asset_rows_by_ids, get_all_assets, get_vec_index_count).
- Handlers that need connect() (rebuild_schema, batch_upsert, etc.) must create a
  fresh MediaDatabase() in the worker so the connection is created in that thread.

When adding new Gradio handlers that touch the DB: add a test here that runs
the handler in a worker thread and asserts no sqlite3.ProgrammingError.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import sqlite_vec

from mediasearch import EMBEDDING_DIM, MediaDatabase


def _init_test_db(path: Path) -> None:
    """Create a real file-based DB with sqlite_vec at path."""
    conn = sqlite3.connect(str(path))
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            hash TEXT NOT NULL,
            mtime REAL NOT NULL,
            type TEXT NOT NULL,
            capture_date TEXT,
            lat REAL,
            lon REAL
        );
        CREATE INDEX IF NOT EXISTS idx_assets_path ON assets(path);
        CREATE INDEX IF NOT EXISTS idx_assets_hash ON assets(hash);
    """)
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_index'"
    ).fetchone()
    if row is None:
        conn.execute("""
            CREATE VIRTUAL TABLE vec_index USING vec0(
                asset_id INTEGER PRIMARY KEY,
                embedding FLOAT[512] distance_metric=cosine
            )
        """)
    conn.commit()
    conn.close()


def _run_in_worker(fn, *args, **kwargs):
    """Run fn in a worker thread and return (result, exception)."""
    result = [None]
    exc = [None]

    def target():
        try:
            out = fn(*args, **kwargs)
            if hasattr(out, "__iter__") and not isinstance(out, (str, bytes, dict)):
                try:
                    result[0] = list(out)
                except TypeError:
                    result[0] = out
            else:
                result[0] = out
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=target)
    t.start()
    t.join()
    return result[0], exc[0]


def test_clear_database_thread_safe(tmp_path: Path) -> None:
    """clear_database must run without SQLite thread error when invoked from a worker."""
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)
    with patch("mediasearch.DEFAULT_DB_PATH", db_path), patch(
        "app.DEFAULT_DB_PATH", db_path
    ):
        import app
        out, exc = _run_in_worker(app.clear_database)
    assert exc is None, f"clear_database raised: {exc}"
    assert out[0] == "Database cleared."


def test_catalog_load_thread_safe(tmp_path: Path) -> None:
    """catalog_stats_load and catalog_direct_load must run without SQLite thread error in a worker."""
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)
    with patch("mediasearch.DEFAULT_DB_PATH", db_path), patch(
        "app.DEFAULT_DB_PATH", db_path
    ):
        import app
        app._db = None
        stats_out, exc1 = _run_in_worker(app.catalog_stats_load)
        direct_out, exc2 = _run_in_worker(app.catalog_direct_load)
    assert exc1 is None, f"catalog_stats_load raised: {exc1}"
    assert exc2 is None, f"catalog_direct_load raised: {exc2}"
    assert isinstance(stats_out, str)
    df_data, assets = direct_out
    assert isinstance(df_data, list)


def test_semantic_search_thread_safe(tmp_path: Path) -> None:
    """semantic_search must run without SQLite thread error when invoked from a worker."""
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)
    db = MediaDatabase(db_path)
    db.init_schema()
    aid = db.upsert_asset("/a.jpg", "h", 1000.0, "IMAGE")
    db.set_embedding(aid, [0.1] * EMBEDDING_DIM)
    db.close()

    mock_embedder = MagicMock()
    mock_embedder.get_text_embedding.return_value = [0.1] * EMBEDDING_DIM

    with (
        patch("mediasearch.DEFAULT_DB_PATH", db_path),
        patch("app.DEFAULT_DB_PATH", db_path),
        patch("app._embedder_instance", return_value=mock_embedder),
        patch("app._db_instance", side_effect=lambda: MediaDatabase(db_path)),
    ):
        import app
        app._db = None
        out, exc = _run_in_worker(app.semantic_search, "test query")
    assert exc is None, f"semantic_search raised: {exc}"
    gallery, meta, status, score_view = out
    assert isinstance(gallery, list)
    assert "Found" in status or "No results" in status


def test_visual_similarity_thread_safe(tmp_path: Path) -> None:
    """visual_similarity must run without SQLite thread error when invoked from a worker."""
    img_path = tmp_path / "ref.jpg"
    img_path.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")  # minimal jpeg header
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)
    db = MediaDatabase(db_path)
    db.init_schema()
    aid = db.upsert_asset("/a.jpg", "h", 1000.0, "IMAGE")
    db.set_embedding(aid, [0.1] * EMBEDDING_DIM)
    db.close()

    mock_embedder = MagicMock()
    mock_embedder.get_image_embedding.return_value = [0.1] * EMBEDDING_DIM

    with (
        patch("mediasearch.DEFAULT_DB_PATH", db_path),
        patch("app.DEFAULT_DB_PATH", db_path),
        patch("app._embedder_instance", return_value=mock_embedder),
        patch("app._db_instance", side_effect=lambda: MediaDatabase(db_path)),
    ):
        import app
        app._db = None
        out, exc = _run_in_worker(app.visual_similarity, str(img_path))
    assert exc is None, f"visual_similarity raised: {exc}"
    gallery, meta, status, score_view = out
    assert isinstance(gallery, list)


def test_library_load_directories_thread_safe(tmp_path: Path) -> None:
    """library_load_directories must run without SQLite thread error when invoked from a worker."""
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)
    with patch("mediasearch.DEFAULT_DB_PATH", db_path), patch(
        "app.DEFAULT_DB_PATH", db_path
    ):
        import app
        app._db = None
        out, exc = _run_in_worker(app.library_load_directories)
    assert exc is None, f"library_load_directories raised: {exc}"
    df_data, paths = out
    assert isinstance(df_data, list)
    assert isinstance(paths, list)


def test_validate_paths_thread_safe(tmp_path: Path) -> None:
    """validate_paths must run without SQLite thread error when invoked from a worker."""
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)
    with patch("mediasearch.DEFAULT_DB_PATH", db_path), patch(
        "app.DEFAULT_DB_PATH", db_path
    ):
        import app
        app._db = None
        out, exc = _run_in_worker(app.validate_paths)
    assert exc is None, f"validate_paths raised: {exc}"
    assert isinstance(out, str)


def test_scan_and_index_thread_safe(tmp_path: Path) -> None:
    """scan_and_index must run without SQLite thread error when invoked from a worker."""
    (tmp_path / "photo.jpg").write_bytes(b"\xff\xd8\xff")
    db_path = tmp_path / "test.db"
    _init_test_db(db_path)

    mock_embedder = MagicMock()
    mock_embedder.get_image_embeddings_batch.return_value = [[0.1] * EMBEDDING_DIM]

    with (
        patch("mediasearch.DEFAULT_DB_PATH", db_path),
        patch("app.DEFAULT_DB_PATH", db_path),
        patch("mediasearch.ImageEmbedder", return_value=mock_embedder),
    ):
        import app
        app._db = None
        out, exc = _run_in_worker(
            lambda: list(app.scan_and_index(str(tmp_path)))
        )
    assert exc is None, f"scan_and_index raised: {exc}"
    assert len(out) >= 1
    all_msgs = " ".join(str(m) for pair in out for m in (pair if isinstance(pair, (list, tuple)) else [pair]))
    assert "Indexing" in all_msgs or "Initializing" in all_msgs or "Found" in all_msgs
