"""
Database layer for media tagging and vector search.
All schema operations (create, drop, rebuild, indexes, migrations) live here.
Handles SQLite + sqlite-vec storage; no model inference.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

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


class DatabaseManager:
    """
    Manages SQLite connection to mediasearch.db with sqlite-vec.
    WAL mode for better concurrent performance on Apple Silicon.
    """

    def __init__(self, db_path: str | Path = "mediasearch.db") -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def _load_extension(self, conn: sqlite3.Connection) -> None:
        """Load sqlite_vec and set WAL mode."""
        conn.enable_load_extension(True)
        try:
            sqlite_vec.load(conn)
        except AttributeError:
            conn.close()
            raise RuntimeError(
                "SQLite does not support load_extension. "
                "Use a Python build with extension support (e.g. Homebrew)."
            ) from None
        finally:
            conn.enable_load_extension(False)
        conn.execute("PRAGMA journal_mode=WAL")

    def connect(self) -> sqlite3.Connection:
        """Open connection, load sqlite_vec, set WAL. Idempotent."""
        if self._conn is not None:
            return self._conn
        self._conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
        self._load_extension(self._conn)
        self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> DatabaseManager:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @contextmanager
    def cursor(self) -> Iterator[sqlite3.Cursor]:
        """Context manager for a cursor. Ensures rollback on exception."""
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def create_tables(self) -> None:
        """Create all tables (assets, tags, vec_index, indexed_directories) if they do not exist."""
        conn = self.connect()
        _create_schema(conn)
        conn.commit()

    def rebuild_schema(self) -> None:
        """Drop all tables and recreate from scratch."""
        conn = self.connect()
        _rebuild_schema(conn)
        conn.commit()

    def add_asset(
        self,
        path: str,
        file_type: str,
        *,
        file_hash: str = "",
        mtime: float = 0.0,
        capture_date: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ) -> int:
        """Insert asset. Returns asset_id. On path conflict, updates and returns existing id."""
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO assets (path, hash, mtime, type, capture_date, lat, lon)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                hash = excluded.hash,
                mtime = excluded.mtime,
                type = excluded.type,
                capture_date = excluded.capture_date,
                lat = excluded.lat,
                lon = excluded.lon
            """,
            (path, file_hash, mtime, file_type, capture_date, lat, lon),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM assets WHERE path = ?", (path,)).fetchone()
        assert row is not None
        return row["id"]

    def link_tags(self, asset_id: int, tags: list[str]) -> None:
        """Replace tags for asset_id. Clears existing tags, inserts new ones."""
        conn = self.connect()
        conn.execute("DELETE FROM tags WHERE asset_id = ?", (asset_id,))
        for tag in tags:
            tag_clean = tag.strip().lower()
            if tag_clean:
                conn.execute(
                    "INSERT OR IGNORE INTO tags (asset_id, tag) VALUES (?, ?)",
                    (asset_id, tag_clean),
                )
        conn.commit()

    def add_embedding(self, asset_id: int, embedding: list[float]) -> None:
        """Store or replace embedding for asset_id. Length must be EMBEDDING_DIM."""
        if len(embedding) != EMBEDDING_DIM:
            raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(embedding)}")
        conn = self.connect()
        blob = sqlite_vec.serialize_float32(embedding)
        conn.execute(
            "INSERT OR REPLACE INTO vec_index (asset_id, embedding) VALUES (?, ?)",
            (asset_id, blob),
        )
        conn.commit()

    def upsert_vector(self, asset_id: int, embedding: list[float]) -> None:
        """Alias for add_embedding."""
        self.add_embedding(asset_id, embedding)

    def add_tags(self, asset_id: int, tags: list[str]) -> None:
        """Alias for link_tags."""
        self.link_tags(asset_id, tags)

    def ingest_asset(
        self,
        file_path: str,
        file_type: str,
        tags: list[str],
        embedding: list[float],
        *,
        file_hash: str = "",
        mtime: float = 0.0,
    ) -> int:
        """
        Insert asset, tags, and embedding in a single transaction.
        Returns asset_id. On failure, rolls back (no partial insert).
        """
        if len(embedding) != EMBEDDING_DIM:
            raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(embedding)}")
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO assets (path, hash, mtime, type, capture_date, lat, lon)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL)
                ON CONFLICT(path) DO UPDATE SET
                    hash = excluded.hash,
                    mtime = excluded.mtime,
                    type = excluded.type
                """,
                (file_path, file_hash, mtime, file_type),
            )
            row = cur.execute(
                "SELECT id FROM assets WHERE path = ?", (file_path,)
            ).fetchone()
            assert row is not None
            asset_id = row[0]

            cur.execute("DELETE FROM tags WHERE asset_id = ?", (asset_id,))
            for tag in tags:
                tag_clean = tag.strip().lower()
                if tag_clean:
                    cur.execute(
                        "INSERT OR IGNORE INTO tags (asset_id, tag) VALUES (?, ?)",
                        (asset_id, tag_clean),
                    )

            blob = sqlite_vec.serialize_float32(embedding)
            cur.execute(
                "INSERT OR REPLACE INTO vec_index (asset_id, embedding) VALUES (?, ?)",
                (asset_id, blob),
            )

        return asset_id
