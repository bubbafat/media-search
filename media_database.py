"""
MediaDatabase — Unified SQLite + sqlite-vec layer for asset metadata, tags, and vector embeddings.
Used by mediasearch, the Gradio app, and ingest.py for indexing, tagging, and semantic search.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import sqlite_vec

from database import EMBEDDING_DIM

# Row for batch upsert: (path, hash, mtime, type, capture_date, lat, lon).
AssetRow = tuple[str, str, float, str, str | None, float | None, float | None]


class MediaDatabase:
    """
    SQLite database with sqlite-vec for asset metadata and vector embeddings.
    Loads sqlite-vec from the uv environment.

    Schema:
      - assets: id, path, hash (unique), mtime, type, capture_date, lat, lon
      - vec_index: vec0 virtual table (asset_id integer, embedding float[1152] distance_metric=cosine)
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._injected_conn = False

    @classmethod
    def from_connection(cls, conn: sqlite3.Connection) -> MediaDatabase:
        """Use an existing connection (e.g. session-scoped in-memory DB). Caller owns the connection."""
        self = cls.__new__(cls)
        self.db_path = Path(":memory:")
        self._conn = conn
        self._injected_conn = True
        return self

    def connect(self) -> sqlite3.Connection:
        """Open connection and enable vector extension. Idempotent."""
        if self._conn is not None:
            return self._conn
        self._conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
        try:
            self._conn.enable_load_extension(True)
            sqlite_vec.load(self._conn)
            self._conn.enable_load_extension(False)
            self._conn.execute("PRAGMA journal_mode=WAL")
        except AttributeError:
            self._conn.close()
            self._conn = None
            raise RuntimeError(
                "This Python's SQLite does not support load_extension (common on macOS). "
                "Use a Python build with extension support, e.g. Homebrew: brew install python"
            ) from None
        self._conn.row_factory = sqlite3.Row
        return self._conn

    def _fresh_connection(self) -> sqlite3.Connection:
        """Open a new connection with sqlite_vec. Use for thread-safe reads from worker threads."""
        if self._injected_conn or str(self.db_path) == ":memory:":
            return self.connect()
        conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def close(self) -> None:
        """Close the database connection. No-op when using from_connection (caller owns conn)."""
        if self._injected_conn:
            self._conn = None
            return
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> MediaDatabase:
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

    def init_schema(self) -> None:
        """Create all tables via database module. Idempotent."""
        from database import _create_schema
        conn = self.connect()
        _create_schema(conn)
        conn.commit()

    def rebuild_schema(self) -> None:
        """Drop and recreate all tables via database module. Use for full re-index."""
        from database import _rebuild_schema
        conn = self.connect()
        _rebuild_schema(conn)
        conn.commit()

    def upsert_asset(
        self,
        path: str,
        file_hash: str,
        mtime: float,
        asset_type: str,
        *,
        capture_date: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ) -> int:
        """
        Insert or replace an asset by path. Returns asset id.
        """
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
            (path, file_hash, mtime, asset_type, capture_date, lat, lon),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM assets WHERE path = ?", (path,)).fetchone()
        assert row is not None
        return row["id"]

    def batch_upsert_assets(self, rows: list[AssetRow]) -> None:
        """
        Insert or replace multiple assets in one transaction. Use chunks of ~100
        to reduce disk I/O; call with rows of length up to BATCH_UPSERT_SIZE.
        """
        if not rows:
            return
        conn = self.connect()
        conn.executemany(
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
            rows,
        )
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
        """
        Insert asset. Returns asset_id. On path conflict, updates and returns existing id.
        Supports optional metadata (hash, mtime, capture_date, lat, lon) for flexible ingestion.
        """
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
        capture_date: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ) -> int:
        """
        Insert asset, tags, and embedding in a single transaction.
        Returns asset_id. On failure, rolls back (no partial insert).
        Supports optional metadata for future unified ingestion pipeline.
        """
        if len(embedding) != EMBEDDING_DIM:
            raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(embedding)}")
        with self.cursor() as cur:
            cur.execute(
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
                (file_path, file_hash, mtime, file_type, capture_date, lat, lon),
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

    def get_assets_count(self) -> int:
        """Return COUNT(*) FROM assets. Thread-safe."""
        conn = self._fresh_connection()
        try:
            row = conn.execute("SELECT COUNT(*) FROM assets").fetchone()
            return row[0] if row else 0
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def get_fragmentation_stats(self) -> dict[str, float]:
        """
        Return database fragmentation stats for UI display.
        Keys: page_count, freelist_count, page_size, free_mb, free_percent.
        For in-memory DB, returns zeros.
        """
        if str(self.db_path) == ":memory:":
            return {
                "page_count": 0.0,
                "freelist_count": 0.0,
                "page_size": 4096.0,
                "free_mb": 0.0,
                "free_percent": 0.0,
            }
        conn = self._fresh_connection()
        try:
            page_count = conn.execute("PRAGMA page_count").fetchone()[0] or 0
            freelist_count = conn.execute("PRAGMA freelist_count").fetchone()[0] or 0
            page_size = conn.execute("PRAGMA page_size").fetchone()[0] or 4096
            free_mb = (freelist_count * page_size) / (1024 * 1024)
            free_percent = (freelist_count / page_count * 100) if page_count > 0 else 0.0
            return {
                "page_count": float(page_count),
                "freelist_count": float(freelist_count),
                "page_size": float(page_size),
                "free_mb": free_mb,
                "free_percent": free_percent,
            }
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def smart_vacuum(self, logger: logging.Logger | None = None) -> str:
        """
        Run VACUUM if fragmentation exceeds thresholds (free_percent > 20 and free_mb > 50).
        After VACUUM, runs wal_checkpoint(TRUNCATE) to reclaim WAL space.
        Returns message describing action taken.
        """
        if str(self.db_path) == ":memory:":
            return "In-memory database: vacuum not applicable."
        stats = self.get_fragmentation_stats()
        free_percent = stats["free_percent"]
        free_mb = stats["free_mb"]
        if free_percent > 20 and free_mb > 50:
            if logger:
                logger.info("Fragmented database detected (%.1f%%). Running background vacuum...", free_percent)
            conn = self.connect()
            conn.execute("VACUUM")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.commit()
            return f"Vacuum complete. Reclaimed ~{free_mb:.1f} MB."
        if logger:
            logger.info("Database health is good. Skipping vacuum.")
        return "Database health is good. Skipping vacuum."

    def get_all_assets(self, limit: int = 50) -> list[dict[str, object]]:
        """Return last N assets (path, type, capture_date, hash). Thread-safe for Gradio workers."""
        conn = self._fresh_connection()
        try:
            rows = conn.execute(
                "SELECT path, type, capture_date, hash FROM assets ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "path": r["path"],
                    "type": r["type"],
                    "capture_date": r["capture_date"],
                    "hash": r["hash"],
                }
                for r in rows
            ]
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def get_assets_with_id(self, limit: int = 100) -> list[dict[str, object]]:
        """Return last N assets (id, path, type, capture_date) for Direct View. Thread-safe."""
        conn = self._fresh_connection()
        try:
            rows = conn.execute(
                "SELECT id, path, type, capture_date, hash FROM assets ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "id": r["id"],
                    "path": r["path"],
                    "type": r["type"],
                    "capture_date": r["capture_date"],
                    "hash": r["hash"],
                }
                for r in rows
            ]
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def get_assets_for_thumb_check(self, limit: int = 500) -> list[dict[str, object]]:
        """Return VIDEO/RAW assets (path, hash, type) for missing-thumbnail audit. Thread-safe."""
        conn = self._fresh_connection()
        try:
            rows = conn.execute(
                "SELECT path, hash, type FROM assets WHERE type IN ('VIDEO', 'RAW') LIMIT ?",
                (limit,),
            ).fetchall()
            return [{"path": r["path"], "hash": r["hash"], "type": r["type"]} for r in rows]
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def get_first_paths(self, limit: int = 10) -> list[str]:
        """Return first N asset paths (by id) for path validation. Thread-safe."""
        conn = self._fresh_connection()
        try:
            rows = conn.execute(
                "SELECT path FROM assets ORDER BY id ASC LIMIT ?",
                (limit,),
            ).fetchall()
            return [r["path"] for r in rows]
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def get_vec_index_count(self) -> int:
        """Return COUNT(*) FROM vec_index. Thread-safe. If 0, semantic search will return no results."""
        conn = self._fresh_connection()
        try:
            row = conn.execute("SELECT COUNT(*) FROM vec_index").fetchone()
            return row[0] if row else 0
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def get_asset_by_path(self, path: str) -> sqlite3.Row | None:
        """Return the asset row for path, or None."""
        conn = self.connect()
        return conn.execute(
            "SELECT id, path, hash, mtime, type, capture_date, lat, lon FROM assets WHERE path = ?",
            (path,),
        ).fetchone()

    def update_asset_mtime(self, asset_id: int, new_mtime: float) -> None:
        """Update the mtime for an asset by id."""
        conn = self.connect()
        conn.execute("UPDATE assets SET mtime = ? WHERE id = ?", (new_mtime, asset_id))
        conn.commit()

    def fetch_asset_rows_by_ids(
        self, pairs: list[tuple[int, float]]
    ) -> list[tuple[sqlite3.Row | None, float]]:
        """
        Fetch asset rows (path, hash, type, capture_date, lat, lon) for given (asset_id, distance) pairs.
        Uses a fresh connection for thread safety when called from Gradio workers.
        Returns list of (row, distance) where row is None if asset not found.
        """
        if not pairs:
            return []
        conn = self._fresh_connection()
        try:
            result: list[tuple[sqlite3.Row | None, float]] = []
            for asset_id, distance in pairs:
                row = conn.execute(
                    "SELECT path, hash, type, capture_date, lat, lon FROM assets WHERE id = ?",
                    (asset_id,),
                ).fetchone()
                result.append((row, distance))
            return result
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def set_embedding(self, asset_id: int, embedding: list[float]) -> None:
        """Store or replace the embedding for an asset (length must be EMBEDDING_DIM)."""
        if len(embedding) != EMBEDDING_DIM:
            raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(embedding)}")
        conn = self.connect()
        blob = sqlite_vec.serialize_float32(embedding)
        conn.execute(
            "INSERT OR REPLACE INTO vec_index (asset_id, embedding) VALUES (?, ?)",
            (asset_id, blob),
        )
        conn.commit()

    def batch_save_embeddings(self, pairs: list[tuple[int, list[float]]]) -> None:
        """Insert or replace embeddings for multiple assets in a single transaction."""
        if not pairs:
            return
        conn = self.connect()
        for asset_id, embedding in pairs:
            if len(embedding) != EMBEDDING_DIM:
                raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(embedding)}")
            blob = sqlite_vec.serialize_float32(embedding)
            conn.execute(
                "INSERT OR REPLACE INTO vec_index (asset_id, embedding) VALUES (?, ?)",
                (asset_id, blob),
            )
        conn.commit()

    def save_embedding(self, asset_id: int, vector: list[float]) -> None:
        """Insert or replace the embedding for an asset using sqlite_vec.serialize_float32."""
        self.set_embedding(asset_id, vector)

    def add_embedding(self, asset_id: int, embedding: list[float]) -> None:
        """Store or replace embedding for asset_id. Alias for set_embedding."""
        self.set_embedding(asset_id, embedding)

    def upsert_vector(self, asset_id: int, embedding: list[float]) -> None:
        """Alias for add_embedding."""
        self.add_embedding(asset_id, embedding)

    def search(
        self,
        query_embedding: list[float],
        k: int = 10,
        threshold: float | None = None,
    ) -> list[tuple[int, float]]:
        """
        KNN search using cosine distance. Returns list of (asset_id, distance)
        for the k nearest vectors. Cosine distance ranges from 0.0 (identical)
        to 2.0 (opposite), but normalized CLIP vectors typically stay in 0.0–1.0.
        If threshold is set, only returns results with distance < threshold.
        Uses a fresh connection for thread safety when called from Gradio workers.
        """
        import numpy as np

        if len(query_embedding) != EMBEDDING_DIM:
            raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(query_embedding)}")
        vec = np.array(query_embedding, dtype=np.float32)
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        normalized = vec.tolist()

        fetch_k = k * 5 if threshold is not None else k
        conn = self._fresh_connection()
        try:
            blob = sqlite_vec.serialize_float32(normalized)
            rows = conn.execute(
                "SELECT asset_id, distance FROM vec_index WHERE embedding MATCH ? AND k = ?",
                (blob, fetch_k),
            ).fetchall()
            results = [(r["asset_id"], r["distance"]) for r in rows]
            if threshold is not None:
                results = [(aid, d) for aid, d in results if d < threshold][:k]
            return results
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def delete_asset_by_path(self, path: str) -> None:
        """Remove asset, its tags, and embedding by path."""
        conn = self.connect()
        row = conn.execute("SELECT id FROM assets WHERE path = ?", (path,)).fetchone()
        if row is None:
            return
        asset_id = row["id"]
        conn.execute("DELETE FROM tags WHERE asset_id = ?", (asset_id,))
        conn.execute("DELETE FROM vec_index WHERE asset_id = ?", (asset_id,))
        conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
        conn.commit()

    def add_directory(self, path: str) -> None:
        """Save a root directory to indexed_directories. Path is normalized (resolved, trailing /)."""
        p = str(Path(path).expanduser().resolve())
        if not p.endswith("/"):
            p = p + "/"
        conn = self.connect()
        conn.execute(
            "INSERT OR IGNORE INTO indexed_directories (path, added_at) VALUES (?, ?)",
            (p, time.time()),
        )
        conn.commit()

    def get_directories(self) -> list[tuple[str, int, float | None, float | None]]:
        """
        Return list of (path, item_count, last_scanned, last_scan_duration) for each indexed directory.
        last_scanned: Unix timestamp or None. last_scan_duration: seconds or None.
        """
        conn = self._fresh_connection()
        try:
            rows = conn.execute(
                "SELECT path, last_scanned, last_scan_duration FROM indexed_directories ORDER BY added_at ASC"
            ).fetchall()
            result: list[tuple[str, int, float | None, float | None]] = []
            for row in rows:
                p = row["path"]
                count_row = conn.execute(
                    "SELECT COUNT(*) FROM assets WHERE path LIKE ? OR path = ?",
                    (p + "%", p.rstrip("/")),
                ).fetchone()
                count = count_row[0] if count_row else 0
                try:
                    ls = row["last_scanned"]
                    lsd = row["last_scan_duration"]
                except (KeyError, IndexError):
                    ls, lsd = None, None
                if ls is not None:
                    try:
                        ls = float(ls)
                    except (TypeError, ValueError):
                        ls = None
                if lsd is not None:
                    try:
                        lsd = float(lsd)
                    except (TypeError, ValueError):
                        lsd = None
                result.append((p.rstrip("/"), count, ls, lsd))
            return result
        finally:
            if not self._injected_conn and conn is not self._conn:
                conn.close()

    def update_directory_scan_stats(self, path: str, duration_seconds: float) -> None:
        """Set last_scanned and last_scan_duration for the directory. Path normalized."""
        p = str(Path(path).expanduser().resolve())
        if not p.endswith("/"):
            p = p + "/"
        conn = self.connect()
        conn.execute(
            "UPDATE indexed_directories SET last_scanned = ?, last_scan_duration = ? WHERE path = ?",
            (time.time(), duration_seconds, p),
        )
        conn.commit()

    def remove_directory(self, path: str) -> int:
        """
        Recursively delete all assets and vectors under this path.
        Path is normalized; matches assets WHERE path LIKE normalized_path||'%'.
        Also removes the directory from indexed_directories.
        Returns number of assets deleted.
        """
        p = str(Path(path).expanduser().resolve())
        if not p.endswith("/"):
            p = p + "/"
        conn = self.connect()
        rows = conn.execute(
            "SELECT id FROM assets WHERE path LIKE ? OR path = ?",
            (p + "%", p.rstrip("/")),
        ).fetchall()
        ids = [r["id"] for r in rows]
        for aid in ids:
            conn.execute("DELETE FROM tags WHERE asset_id = ?", (aid,))
            conn.execute("DELETE FROM vec_index WHERE asset_id = ?", (aid,))
        conn.execute("DELETE FROM assets WHERE path LIKE ? OR path = ?", (p + "%", p.rstrip("/")))
        conn.execute("DELETE FROM indexed_directories WHERE path = ?", (p,))
        conn.commit()
        return len(ids)
