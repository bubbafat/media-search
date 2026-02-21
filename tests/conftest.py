"""
Pytest configuration and shared fixtures for mediasearch tests.

Uses a session-scoped in-memory SQLite DB with sqlite-vec loaded so that
no mediasearch.db file is ever created on disk during tests.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import sqlite_vec

# Project root (parent of tests/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MEDIASEARCH_DB_PATH = PROJECT_ROOT / "mediasearch.db"


def _memory_conn_with_sqlite_vec() -> sqlite3.Connection:
    """Create a new in-memory connection with sqlite-vec extension loaded (same as MediaDatabase)."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)  # uses package-bundled extension, not /opt/homebrew/lib
        conn.enable_load_extension(False)
    except AttributeError:
        conn.close()
        pytest.skip("SQLite load_extension not available")
    conn.row_factory = sqlite3.Row
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    """Create all tables via database module (single source of truth)."""
    from database import _create_schema
    _create_schema(conn)
    conn.commit()


@pytest.fixture(scope="session")
def db_conn() -> sqlite3.Connection:
    """
    Session-scoped in-memory SQLite connection with sqlite-vec loaded
    and schema (assets, vec_index) initialized once for the entire run.
    """
    conn = _memory_conn_with_sqlite_vec()
    _init_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def clear_db(db_conn: sqlite3.Connection) -> None:
    """Clear assets, vec_index, and indexed_directories before each test."""
    db_conn.execute("DELETE FROM vec_index")
    db_conn.execute("DELETE FROM assets")
    try:
        db_conn.execute("DELETE FROM indexed_directories")
    except sqlite3.OperationalError:
        pass  # table may not exist
    db_conn.commit()


@pytest.fixture(scope="session", autouse=True)
def assert_mediasearch_db_never_created() -> None:
    """
    Verify that mediasearch.db is never created on disk during the test session.
    Remove it at session start if present (e.g. from a previous run), then assert
    it does not exist at session end.
    """
    if MEDIASEARCH_DB_PATH.exists():
        MEDIASEARCH_DB_PATH.unlink()
    yield
    if MEDIASEARCH_DB_PATH.exists():
        pytest.fail(
            f"mediasearch.db must not be created during tests; found at {MEDIASEARCH_DB_PATH}"
        )
