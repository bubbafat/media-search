"""
Database layer for media tagging and vector search.
Handles SQLite + sqlite-vec storage; no model inference.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import sqlite_vec

EMBEDDING_DIM = 1152


class DatabaseManager:
    """
    Manages SQLite connection to media.db with sqlite-vec.
    WAL mode for better concurrent performance on Apple Silicon.
    """

    def __init__(self, db_path: str | Path = "media.db") -> None:
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
        """Create assets, tags, and vec_index tables if they do not exist."""
        conn = self.connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL UNIQUE,
                file_type TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_assets_file_path ON assets(file_path);

            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE,
                UNIQUE(asset_id, tag)
            );
            CREATE INDEX IF NOT EXISTS idx_tags_asset_id ON tags(asset_id);
            CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);
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
        conn.commit()

    def add_asset(self, file_path: str, file_type: str) -> int:
        """Insert asset. Returns asset_id. On path conflict, updates file_type and returns existing id."""
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO assets (file_path, file_type) VALUES (?, ?)
            ON CONFLICT(file_path) DO UPDATE SET file_type = excluded.file_type
            """,
            (file_path, file_type),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM assets WHERE file_path = ?", (file_path,)).fetchone()
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
