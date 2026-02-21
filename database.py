"""
Database schema and constants for media search.
All schema operations (create, drop, rebuild, indexes, migrations) live here.
The unified database layer is MediaDatabase in media_database.py.
"""

from __future__ import annotations

import sqlite3

import sqlite_vec

EMBEDDING_DIM = 1152


def _create_schema(conn: sqlite3.Connection) -> None:
    """
    Create all tables and indexes if they do not exist.
    Run migrations for existing databases (add missing columns).
    Caller must have sqlite_vec loaded and must commit.
    """
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

        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE,
            UNIQUE(asset_id, tag)
        );
        CREATE INDEX IF NOT EXISTS idx_tags_asset_id ON tags(asset_id);
        CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);

        CREATE TABLE IF NOT EXISTS indexed_directories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL UNIQUE,
            added_at REAL NOT NULL,
            last_scanned REAL,
            last_scan_duration REAL
        );
    """)
    # vec0 virtual table: must be created with explicit schema
    if conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_index'"
    ).fetchone() is None:
        conn.execute("""
            CREATE VIRTUAL TABLE vec_index USING vec0(
                asset_id INTEGER PRIMARY KEY,
                embedding FLOAT[1152] distance_metric=cosine
            )
        """)
    # Migrations: add missing columns to existing tables
    col_names = {r[1] for r in conn.execute("PRAGMA table_info(assets)").fetchall()}
    for col, typ in [("capture_date", "TEXT"), ("lat", "REAL"), ("lon", "REAL")]:
        if col not in col_names:
            conn.execute(f"ALTER TABLE assets ADD COLUMN {col} {typ}")
    dir_cols = {r[1] for r in conn.execute("PRAGMA table_info(indexed_directories)").fetchall()}
    for col, typ in [("last_scanned", "REAL"), ("last_scan_duration", "REAL")]:
        if col not in dir_cols:
            conn.execute(f"ALTER TABLE indexed_directories ADD COLUMN {col} {typ}")


def _rebuild_schema(conn: sqlite3.Connection) -> None:
    """
    Drop all tables and recreate from scratch.
    Caller must have sqlite_vec loaded and must commit.
    """
    conn.execute("DROP TABLE IF EXISTS vec_index")
    conn.execute("DROP TABLE IF EXISTS tags")
    conn.execute("DROP TABLE IF EXISTS assets")
    conn.execute("DROP TABLE IF EXISTS indexed_directories")
    conn.commit()
    _create_schema(conn)
