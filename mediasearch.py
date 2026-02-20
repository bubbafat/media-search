#!/usr/bin/env python3
"""
MediaSearch — Local-first semantic search engine for media files.

Supports JPG, RAW (ARW), and MP4/MOV. Uses SQLite with sqlite-vec for
vector search and Apple MLX for GPU-accelerated embeddings (M4 Mac Studio).
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Iterator

import sqlite_vec

# Default database path (project-local).
DEFAULT_DB_PATH = Path(__file__).resolve().parent / "mediasearch.db"

# Extension (lowercase, with dot) -> category for coding branches (IMAGE, RAW, VIDEO).
# Add more RAW types (e.g. .nef, .cr3) here with category "RAW".
MEDIA_EXTENSIONS: dict[str, str] = {
    ".jpg": "IMAGE",
    ".jpeg": "IMAGE",
    ".arw": "RAW",
    ".mp4": "VIDEO",
    ".mov": "VIDEO",
}

# Vector dimension for embeddings (e.g. MLX VLM or similar).
EMBEDDING_DIM = 512


def setup_logging(verbose: bool = False) -> None:
    """Configure logging with clear progress-style messages."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Suppress noisy third-party loggers
    logging.getLogger("sqlite_vec").setLevel(logging.WARNING)


class MediaDatabase:
    """
    SQLite database with sqlite-vec for asset metadata and vector embeddings.

    Schema:
      - assets: id, path (unique), hash, mtime, type
      - vec_index: vec0 virtual table (asset_id, embedding float[512])
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        """Open connection and enable vector extension. Idempotent."""
        if self._conn is not None:
            return self._conn
        self._conn = sqlite3.connect(str(self.db_path))
        try:
            self._conn.enable_load_extension(True)
            sqlite_vec.load(self._conn)
            self._conn.enable_load_extension(False)
        except AttributeError:
            self._conn.close()
            self._conn = None
            raise RuntimeError(
                "This Python's SQLite does not support load_extension (common on macOS). "
                "Use a Python build with extension support, e.g. Homebrew: brew install python"
            ) from None
        self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> MediaDatabase:
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def init_schema(self) -> None:
        """Create assets table and vec_index virtual table if they do not exist."""
        conn = self.connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                hash TEXT NOT NULL,
                mtime REAL NOT NULL,
                type TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_assets_path ON assets(path);
            CREATE INDEX IF NOT EXISTS idx_assets_hash ON assets(hash);
        """)
        # vec0 virtual table: must be created with explicit schema
        if conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_index'"
        ).fetchone() is None:
            conn.execute("""
                CREATE VIRTUAL TABLE vec_index USING vec0(
                    asset_id INTEGER PRIMARY KEY,
                    embedding FLOAT[512]
                )
            """)
        conn.commit()

    def rebuild_schema(self) -> None:
        """Drop and recreate assets and vec_index. Use for full re-index."""
        conn = self.connect()
        conn.execute("DROP TABLE IF EXISTS vec_index")
        conn.execute("DROP TABLE IF EXISTS assets")
        conn.commit()
        self.init_schema()

    def upsert_asset(self, path: str, file_hash: str, mtime: float, asset_type: str) -> int:
        """
        Insert or replace an asset by path. Returns asset id.
        """
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO assets (path, hash, mtime, type)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET hash = excluded.hash, mtime = excluded.mtime, type = excluded.type
            """,
            (path, file_hash, mtime, asset_type),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM assets WHERE path = ?", (path,)).fetchone()
        assert row is not None
        return row["id"]

    def get_asset_by_path(self, path: str) -> sqlite3.Row | None:
        """Return the asset row for path, or None."""
        conn = self.connect()
        return conn.execute(
            "SELECT id, path, hash, mtime, type FROM assets WHERE path = ?",
            (path,),
        ).fetchone()

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

    def search(self, query_embedding: list[float], k: int = 10) -> list[tuple[int, float]]:
        """
        KNN search. Returns list of (asset_id, distance) for the k nearest vectors.
        """
        if len(query_embedding) != EMBEDDING_DIM:
            raise ValueError(f"embedding length must be {EMBEDDING_DIM}, got {len(query_embedding)}")
        conn = self.connect()
        blob = sqlite_vec.serialize_float32(query_embedding)
        rows = conn.execute(
            "SELECT asset_id, distance FROM vec_index WHERE embedding MATCH ? AND k = ?",
            (blob, k),
        ).fetchall()
        return [(r["asset_id"], r["distance"]) for r in rows]

    def delete_asset_by_path(self, path: str) -> None:
        """Remove asset and its embedding by path."""
        conn = self.connect()
        row = conn.execute("SELECT id FROM assets WHERE path = ?", (path,)).fetchone()
        if row is None:
            return
        asset_id = row["id"]
        conn.execute("DELETE FROM vec_index WHERE asset_id = ?", (asset_id,))
        conn.execute("DELETE FROM assets WHERE id = ?", (asset_id,))
        conn.commit()


class FileCrawler:
    """
    Recursively discovers media files (JPG, ARW, MP4, MOV) and supports
    content hashing for deduplication.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()

    def _is_media(self, path: Path) -> bool:
        return path.suffix.lower() in MEDIA_EXTENSIONS

    def crawl(self) -> Iterator[Path]:
        """Yield each supported media file under root (recursive)."""
        if not self.root.is_dir():
            raise NotADirectoryError(str(self.root))
        for p in self.root.rglob("*"):
            if p.is_file() and self._is_media(p):
                yield p

    def get_hash(self, path: Path, *, chunk_size: int = 65536) -> str:
        """
        Compute SHA-256 hash of file contents for deduplication.
        """
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(str(path))
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def path_to_type(path: Path) -> str:
        """Map file path to category: IMAGE, RAW, VIDEO, or unknown."""
        ext = path.suffix.lower()
        return MEDIA_EXTENSIONS.get(ext, "unknown")


def run_rebuild(db: MediaDatabase, path: Path, logger: logging.Logger) -> None:
    """Full scan: wipe index, then index every file under path."""
    db.rebuild_schema()
    crawler = FileCrawler(path)
    files = list(crawler.crawl())
    total = len(files)
    logger.info("Rebuild: indexing %d files under %s", total, path)

    for i, fp in enumerate(files, start=1):
        try:
            file_hash = crawler.get_hash(fp)
            mtime = fp.stat().st_mtime
            asset_type = FileCrawler.path_to_type(fp)
            db.upsert_asset(str(fp), file_hash, mtime, asset_type)
        except OSError as e:
            logger.warning("Skip %s: %s", fp, e)
        if i % 50 == 0 or i == total:
            logger.info("Scanning... [%d/%d]", i, total)

    logger.info("Rebuild complete. %d assets indexed.", total)


def run_update(db: MediaDatabase, path: Path, logger: logging.Logger) -> None:
    """Incremental scan: add new files, update changed (mtime/hash), remove missing."""
    db.init_schema()
    crawler = FileCrawler(path)
    files = list(crawler.crawl())
    total = len(files)
    logger.info("Update: scanning %d files under %s", total, path)

    indexed_paths: set[str] = set()
    for i, fp in enumerate(files, start=1):
        try:
            str_path = str(fp)
            file_hash = crawler.get_hash(fp)
            mtime = fp.stat().st_mtime
            asset_type = FileCrawler.path_to_type(fp)
            existing = db.get_asset_by_path(str_path)
            if existing is None or existing["mtime"] != mtime or existing["hash"] != file_hash:
                db.upsert_asset(str_path, file_hash, mtime, asset_type)
            indexed_paths.add(str_path)
        except OSError as e:
            logger.warning("Skip %s: %s", fp, e)
        if i % 50 == 0 or i == total:
            logger.info("Scanning... [%d/%d]", i, total)

    # Remove assets whose paths no longer exist on disk
    conn = db.connect()
    for row in conn.execute("SELECT id, path FROM assets").fetchall():
        if row["path"] not in indexed_paths:
            db.delete_asset_by_path(row["path"])
            logger.debug("Removed missing path: %s", row["path"])

    logger.info("Update complete. %d assets in index.", len(indexed_paths))


def run_query(db: MediaDatabase, query_text: str, logger: logging.Logger) -> None:
    """
    Search mode: embed query (stub for now) and run KNN against vec_index.
    TODO: Replace placeholder embedding with MLX VLM embedding on M4.
    """
    db.init_schema()
    # Placeholder: zero vector until MLX embedding is wired.
    placeholder_embedding = [0.0] * EMBEDDING_DIM
    results = db.search(placeholder_embedding, k=10)

    logger.info('Query: "%s"', query_text)
    if not results:
        logger.info("No results (index may be empty or no vectors stored).")
        return

    conn = db.connect()
    logger.info("Top %d results (distance):", len(results))
    for asset_id, distance in results:
        row = conn.execute(
            "SELECT path, type FROM assets WHERE id = ?", (asset_id,)
        ).fetchone()
        path = row["path"] if row else "(missing asset)"
        logger.info("  %.4f  %s", distance, path)


def main() -> int:
    """CLI entrypoint: argparse and dispatch to rebuild / update / query."""
    parser = argparse.ArgumentParser(
        prog="mediasearch",
        description="MediaSearch — local-first semantic search for media (JPG, ARW, MP4/MOV).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to SQLite database (default: mediasearch.db in project root)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("rebuild", help="Full scan and index wipe").add_argument(
        "--path", type=Path, required=True, help="Root directory to scan"
    )
    sub.add_parser("update", help="Incremental scan (mtime/hash)").add_argument(
        "--path", type=Path, required=True, help="Root directory to scan"
    )
    query_p = sub.add_parser("query", help="Search by text")
    query_p.add_argument("query", metavar="QUERY", help='Search query (e.g. "sunset at beach")')

    args = parser.parse_args()

    setup_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)

    if args.command == "rebuild":
        with MediaDatabase(args.db) as db:
            run_rebuild(db, args.path, logger)
        return 0

    if args.command == "update":
        with MediaDatabase(args.db) as db:
            run_update(db, args.path, logger)
        return 0

    if args.command == "query":
        with MediaDatabase(args.db) as db:
            run_query(db, args.query, logger)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
