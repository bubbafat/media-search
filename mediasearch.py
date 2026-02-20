#!/usr/bin/env python3
"""
MediaSearch — Local-first semantic search engine for media files.

Phase 1: Core engine — metadata ingestion and deduplication.
Stack: Python, SQLite + sqlite-vec, FFmpeg, ExifTool.
Supports JPG, RAW (ARW), MP4/MOV. Uses Apple MLX for embeddings (M4 Mac Studio).
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterator

# Row for batch upsert: (path, hash, mtime, type, capture_date, lat, lon).
AssetRow = tuple[str, str, float, str, str | None, float | None, float | None]

import sqlite_vec

try:
    from exiftool import ExifToolHelper
except ImportError:
    ExifToolHelper = None  # type: ignore[misc, assignment]

try:
    import ffmpeg
except ImportError:
    ffmpeg = None  # type: ignore[assignment]

# MLX-CLIP for visual search (optional; requires Apple Silicon with Metal).
CLIP_MODEL_NAME = "mlx-community/clip-vit-base-patch32"
_embedder_model: object | None = None
_embedder_processor: object | None = None


def _get_clip_model() -> tuple[object, object]:
    """Load CLIP model and processor via mlx_embeddings (cached per process). Returns (model, processor)."""
    global _embedder_model, _embedder_processor
    if _embedder_model is not None:
        return _embedder_model, _embedder_processor
    try:
        from mlx_embeddings.utils import load
        _embedder_model, _embedder_processor = load(CLIP_MODEL_NAME)
        return _embedder_model, _embedder_processor
    except Exception as e:
        raise RuntimeError(
            f"Failed to load CLIP model {CLIP_MODEL_NAME}. "
            "Requires mlx-embeddings and Apple Silicon with Metal."
        ) from e

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

# ExifTool binary (Homebrew on Apple Silicon).
EXIFTOOL_PATH = "/opt/homebrew/bin/exiftool"

# Video thumbnail size (square frame for embedding models).
THUMB_SIZE = 224
THUMB_TIME_SEC = 1.0

# Sparse hash: for files larger than this, hash first/middle/last 1MB only.
SPARSE_HASH_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB
SPARSE_HASH_CHUNK_BYTES = 1024 * 1024  # 1 MB

# Batch size for bulk asset inserts (reduces disk I/O).
BATCH_UPSERT_SIZE = 100

# Batch size for embedding (GPU throughput on M4).
EMBED_BATCH_SIZE = 32


def _dms_to_decimal(d: float, m: float, s: float, hem: str) -> float:
    """Convert degrees, minutes, seconds and hemisphere to decimal degrees."""
    dec = d + m / 60.0 + s / 3600.0
    if hem in "SW":
        dec = -dec
    return dec


def _parse_dms_single(s: str) -> float | None:
    """Parse a single DMS string like \"47 deg 12' 34.56\" N\" to decimal degrees."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    deg_min_sec = r"(-?\d+(?:\.\d+)?)\s*deg\s*(-?\d+(?:\.\d+)?)\s*'\s*(-?\d+(?:\.\d+)?)\s*\"\s*([NSEW])"
    m = re.match(deg_min_sec, s, re.IGNORECASE)
    if not m:
        return None
    return _dms_to_decimal(float(m.group(1)), float(m.group(2)), float(m.group(3)), m.group(4).upper())


def _parse_gps_position(gps_str: str) -> tuple[float | None, float | None]:
    """
    Parse ExifTool Composite:GPSPosition string to (lat, lon) in decimal degrees.
    E.g. "47 deg 12' 34.56" N, 122 deg 45' 67.89" W" -> (47.2096, -122.7689).
    """
    if not gps_str or not isinstance(gps_str, str):
        return (None, None)
    parts = re.split(r",\s*", gps_str.strip())
    if len(parts) != 2:
        return (None, None)
    lat = _parse_dms_single(parts[0].strip())
    lon = _parse_dms_single(parts[1].strip())
    return (lat, lon)


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
    Loads sqlite-vec from the uv environment.

    Schema:
      - assets: id, path, hash (unique), mtime, type, capture_date, lat, lon
      - vec_index: vec0 virtual table (asset_id integer, embedding float[512])
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

    def init_schema(self) -> None:
        """Create assets table and vec_index virtual table if they do not exist."""
        conn = self.connect()
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
        # Migrate existing assets table: add new columns if missing
        row = conn.execute("PRAGMA table_info(assets)").fetchall()
        col_names = {r[1] for r in row} if row else set()
        for col, typ in [("capture_date", "TEXT"), ("lat", "REAL"), ("lon", "REAL")]:
            if col not in col_names:
                conn.execute(f"ALTER TABLE assets ADD COLUMN {col} {typ}")
        conn.commit()
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

    def get_asset_by_path(self, path: str) -> sqlite3.Row | None:
        """Return the asset row for path, or None."""
        conn = self.connect()
        return conn.execute(
            "SELECT id, path, hash, mtime, type, capture_date, lat, lon FROM assets WHERE path = ?",
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
        Compute SHA-256 hash for deduplication.
        For files > 100MB, uses a sparse hash (first, middle, last 1MB) to speed up video indexing.
        """
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(str(path))
        size = path.stat().st_size
        h = hashlib.sha256()
        with open(path, "rb") as f:
            if size > SPARSE_HASH_THRESHOLD_BYTES:
                # Sparse: first 1MB, middle 1MB, last 1MB
                chunk = f.read(SPARSE_HASH_CHUNK_BYTES)
                if chunk:
                    h.update(chunk)
                mid = (size - SPARSE_HASH_CHUNK_BYTES) // 2
                f.seek(mid)
                chunk = f.read(SPARSE_HASH_CHUNK_BYTES)
                if chunk:
                    h.update(chunk)
                f.seek(max(0, size - SPARSE_HASH_CHUNK_BYTES))
                chunk = f.read(SPARSE_HASH_CHUNK_BYTES)
                if chunk:
                    h.update(chunk)
            else:
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


def get_metadata(path: Path) -> dict[str, str | float | None]:
    """
    Use ExifToolHelper (exiftool at EXIFTOOL_PATH) to extract
    EXIF:DateTimeOriginal and GPS. Prefer Composite:GPSPosition; if null,
    fall back to EXIF:GPSLatitude and EXIF:GPSLongitude.
    """
    out: dict[str, str | float | None] = {
        "capture_date": None,
        "lat": None,
        "lon": None,
    }
    if ExifToolHelper is None:
        return out
    path_str = str(path.resolve())
    tags = [
        "EXIF:DateTimeOriginal",
        "Composite:GPSPosition",
        "EXIF:GPSLatitude",
        "EXIF:GPSLongitude",
    ]
    try:
        with ExifToolHelper(executable=EXIFTOOL_PATH) as et:
            tags_list = et.get_tags([path_str], tags)
    except Exception:
        return out
    if not tags_list or not isinstance(tags_list[0], dict):
        return out
    d = tags_list[0]
    out["capture_date"] = d.get("EXIF:DateTimeOriginal") or d.get("Composite:DateTimeCreated") or None
    if isinstance(out["capture_date"], str):
        out["capture_date"] = out["capture_date"].strip()

    gps = d.get("Composite:GPSPosition")
    if gps:
        lat, lon = _parse_gps_position(str(gps))
        out["lat"], out["lon"] = lat, lon
    else:
        # Fallback: EXIF:GPSLatitude and EXIF:GPSLongitude (e.g. "47 deg 12' 34.56" N")
        lat_val = d.get("EXIF:GPSLatitude")
        lon_val = d.get("EXIF:GPSLongitude")
        if lat_val is not None and lon_val is not None:
            out["lat"] = _parse_dms_single(str(lat_val))
            out["lon"] = _parse_dms_single(str(lon_val))
    return out


class VideoThumbnailer:
    """
    Extracts a single 224x224 frame at 1.0s from videos using ffmpeg-python.
    Saves thumbnails to a hidden .thumbnails directory keyed by file SHA-256 hash.
    """

    def __init__(self, thumb_dir: Path | None = None) -> None:
        self.thumb_dir = Path(thumb_dir) if thumb_dir else Path(__file__).resolve().parent / ".thumbnails"
        self.thumb_dir.mkdir(parents=True, exist_ok=True)

    def thumbnail_path(self, file_hash: str) -> Path:
        """Path where thumbnail for this hash would be stored."""
        return self.thumb_dir / f"{file_hash}.jpg"

    def ensure_thumbnail(self, video_path: Path, file_hash: str) -> Path:
        """
        Extract 224x224 frame at 1.0s from video; save as .thumbnails/<hash>.jpg.
        Returns path to thumbnail (existing or newly created).
        """
        out_path = self.thumbnail_path(file_hash)
        if out_path.exists():
            return out_path
        if ffmpeg is None:
            raise RuntimeError("ffmpeg-python is not installed")
        try:
            (
                ffmpeg.input(
                    str(video_path),
                    hwaccel="videotoolbox",
                    ss=THUMB_TIME_SEC,
                )
                .filter("scale", THUMB_SIZE, THUMB_SIZE)
                .output(str(out_path), vframes=1)
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
        except ffmpeg.Error as e:
            err = (e.stderr or b"").decode(errors="replace")
            raise RuntimeError(f"FFmpeg thumbnail failed: {err}") from e
        return out_path


def _mlx_array_to_list(vec: object) -> list[float]:
    """Convert MLX array or numpy array to list of floats (length EMBEDDING_DIM)."""
    import mlx.core as mx
    if hasattr(vec, "tolist"):
        return vec.tolist()
    arr = mx.asnumpy(vec) if hasattr(mx, "asnumpy") else vec
    return list(arr.flatten().tolist())


class ImageEmbedder:
    """
    MLX-CLIP embedder for image and text. Uses mlx-community/clip-vit-base-patch32
    (via mlx_embeddings) to produce 512-dim vectors for visual search.
    Model is loaded lazily on first use (get_image_embedding or get_text_embedding).
    """

    def __init__(self, model_name: str = CLIP_MODEL_NAME) -> None:
        self.model_name = model_name
        self._model: object | None = None
        self._processor: object | None = None

    @property
    def _model_and_processor(self) -> tuple[object, object]:
        """Load model and processor on first access (lazy); cached for the process."""
        if self._model is None:
            self._model, self._processor = _get_clip_model()
        return self._model, self._processor

    def get_image_embedding(self, image_path: Path | str) -> list[float]:
        """Return a 512-dim embedding vector for the image at image_path."""
        import mlx.core as mx
        from PIL import Image
        path = Path(image_path)
        if not path.is_file():
            raise FileNotFoundError(str(path))
        model, proc = self._model_and_processor
        image = Image.open(path).convert("RGB")
        inputs = proc(images=image, return_tensors="pt", padding=True)
        pv = inputs["pixel_values"].numpy().transpose(0, 2, 3, 1)
        pixel_values = mx.array(pv).astype(mx.float32)
        if hasattr(model, "get_image_features"):
            out = model.get_image_features(pixel_values=pixel_values)
        else:
            out = model(pixel_values=pixel_values)
            out = getattr(out, "image_embeds", getattr(out, "last_hidden_state", out))
        vec = out[0] if hasattr(out, "__getitem__") else out
        return _mlx_array_to_list(vec)

    def get_image_embeddings_batch(
        self, image_paths: list[Path | str]
    ) -> list[list[float]]:
        """Return 512-dim embedding vectors for a batch of images (GPU-efficient)."""
        if not image_paths:
            return []
        import mlx.core as mx
        from PIL import Image
        model, proc = self._model_and_processor
        images = []
        for p in image_paths:
            path = Path(p)
            if not path.is_file():
                raise FileNotFoundError(str(path))
            images.append(Image.open(path).convert("RGB"))
        inputs = proc(images=images, return_tensors="pt", padding=True)
        pv = inputs["pixel_values"].numpy().transpose(0, 2, 3, 1)
        pixel_values = mx.array(pv).astype(mx.float32)
        if hasattr(model, "get_image_features"):
            out = model.get_image_features(pixel_values=pixel_values)
        else:
            out = model(pixel_values=pixel_values)
            out = getattr(out, "image_embeds", getattr(out, "last_hidden_state", out))
        return [_mlx_array_to_list(out[i]) for i in range(len(images))]

    def get_text_embedding(self, text_query: str) -> list[float]:
        """Return a 512-dim embedding vector for the text query."""
        import mlx.core as mx
        model, proc = self._model_and_processor
        inputs = proc(text=[text_query], return_tensors="pt", padding=True, truncation=True)
        input_ids = mx.array(inputs["input_ids"].numpy())
        if hasattr(model, "get_text_features"):
            out = model.get_text_features(input_ids=input_ids)
        else:
            out = model(input_ids=input_ids)
            out = getattr(out, "text_embeds", getattr(out, "last_hidden_state", out))
        vec = out[0] if hasattr(out, "__getitem__") else out
        return _mlx_array_to_list(vec)


def _progress_bar(current: int, total: int, width: int = 30) -> str:
    """Return a simple text progress bar and counter."""
    if total <= 0:
        pct = 0.0
    else:
        pct = current / total
    filled = int(width * pct)
    bar = "=" * filled + (">" if filled < width else "") + " " * (width - filled - 1)
    return f"[{bar}] {current}/{total}"


def run_rebuild(db: MediaDatabase, path: Path, logger: logging.Logger) -> None:
    """Full scan: wipe index, index every file with metadata and video thumbnails, then embed with MLX-CLIP."""
    db.rebuild_schema()
    crawler = FileCrawler(path)
    files = list(crawler.crawl())
    total = len(files)
    thumbnailer = VideoThumbnailer()
    embedder = ImageEmbedder()
    logger.info("Rebuild: indexing %d files under %s", total, path)

    batch: list[AssetRow] = []
    to_embed: list[tuple[str, str, str]] = []  # (path, file_hash, asset_type)
    for i, fp in enumerate(files, start=1):
        try:
            file_hash = crawler.get_hash(fp)
            mtime = fp.stat().st_mtime
            asset_type = FileCrawler.path_to_type(fp)
            meta = get_metadata(fp)
            batch.append((
                str(fp),
                file_hash,
                mtime,
                asset_type,
                meta.get("capture_date") or None,
                meta.get("lat"),
                meta.get("lon"),
            ))
            to_embed.append((str(fp), file_hash, asset_type))
            if asset_type == "VIDEO":
                try:
                    thumbnailer.ensure_thumbnail(fp, file_hash)
                except Exception as e:
                    logger.debug("Thumbnail skip %s: %s", fp, e)
            if len(batch) >= BATCH_UPSERT_SIZE:
                db.batch_upsert_assets(batch)
                batch.clear()
        except OSError as e:
            logger.warning("Skip %s: %s", fp, e)
        logger.info("\r%s", _progress_bar(i, total))
    if batch:
        db.batch_upsert_assets(batch)
    logger.info("")

    # Embed in batches: images/RAW use file path; videos use thumbnail path
    to_embed_with_id: list[tuple[int, Path]] = []
    for str_path, file_hash, asset_type in to_embed:
        try:
            row = db.get_asset_by_path(str_path)
            if not row:
                continue
            if asset_type == "VIDEO":
                image_path = thumbnailer.thumbnail_path(file_hash)
                if not image_path.exists():
                    continue
            else:
                image_path = Path(str_path)
            to_embed_with_id.append((row["id"], image_path))
        except Exception as e:
            logger.debug("Embed skip %s: %s", str_path, e)

    for start in range(0, len(to_embed_with_id), EMBED_BATCH_SIZE):
        batch = to_embed_with_id[start : start + EMBED_BATCH_SIZE]
        try:
            paths = [p for _, p in batch]
            vecs = embedder.get_image_embeddings_batch(paths)
            db.batch_save_embeddings([(aid, vec) for (aid, _), vec in zip(batch, vecs)])
        except Exception as e:
            logger.debug("Embed batch skip: %s", e)
        j = min(start + EMBED_BATCH_SIZE, len(to_embed_with_id))
        logger.info("\rEmbedding... [%d/%d]", j, len(to_embed_with_id))
    logger.info("")
    logger.info("Rebuild complete. %d assets indexed.", total)


def run_update(db: MediaDatabase, path: Path, logger: logging.Logger) -> None:
    """Incremental scan: add/update assets, metadata, thumbnails, then embed new/changed with MLX-CLIP."""
    db.init_schema()
    crawler = FileCrawler(path)
    files = list(crawler.crawl())
    total = len(files)
    thumbnailer = VideoThumbnailer()
    embedder = ImageEmbedder()
    logger.info("Update: scanning %d files under %s", total, path)

    indexed_paths: set[str] = set()
    batch: list[AssetRow] = []
    to_embed: list[tuple[str, str, str]] = []
    for i, fp in enumerate(files, start=1):
        try:
            str_path = str(fp)
            file_hash = crawler.get_hash(fp)
            mtime = fp.stat().st_mtime
            asset_type = FileCrawler.path_to_type(fp)
            existing = db.get_asset_by_path(str_path)
            if existing is None or existing["mtime"] != mtime or existing["hash"] != file_hash:
                meta = get_metadata(fp)
                batch.append((
                    str_path,
                    file_hash,
                    mtime,
                    asset_type,
                    meta.get("capture_date") or None,
                    meta.get("lat"),
                    meta.get("lon"),
                ))
                to_embed.append((str_path, file_hash, asset_type))
                if asset_type == "VIDEO":
                    try:
                        thumbnailer.ensure_thumbnail(fp, file_hash)
                    except Exception as e:
                        logger.debug("Thumbnail skip %s: %s", fp, e)
                if len(batch) >= BATCH_UPSERT_SIZE:
                    db.batch_upsert_assets(batch)
                    batch.clear()
            indexed_paths.add(str_path)
        except OSError as e:
            logger.warning("Skip %s: %s", fp, e)
        logger.info("\r%s", _progress_bar(i, total))
    logger.info("")
    if batch:
        db.batch_upsert_assets(batch)

    # Embed new/updated assets in batches
    to_embed_with_id = []
    for str_path, file_hash, asset_type in to_embed:
        try:
            row = db.get_asset_by_path(str_path)
            if not row:
                continue
            if asset_type == "VIDEO":
                image_path = thumbnailer.thumbnail_path(file_hash)
                if not image_path.exists():
                    continue
            else:
                image_path = Path(str_path)
            to_embed_with_id.append((row["id"], image_path))
        except Exception as e:
            logger.debug("Embed skip %s: %s", str_path, e)

    for start in range(0, len(to_embed_with_id), EMBED_BATCH_SIZE):
        batch = to_embed_with_id[start : start + EMBED_BATCH_SIZE]
        try:
            paths = [p for _, p in batch]
            vecs = embedder.get_image_embeddings_batch(paths)
            db.batch_save_embeddings([(aid, vec) for (aid, _), vec in zip(batch, vecs)])
        except Exception as e:
            logger.debug("Embed batch skip: %s", e)
        j = min(start + EMBED_BATCH_SIZE, len(to_embed_with_id))
        logger.info("\rEmbedding... [%d/%d]", j, len(to_embed_with_id))
    if to_embed_with_id:
        logger.info("")

    # Remove assets whose paths no longer exist on disk
    conn = db.connect()
    for row in conn.execute("SELECT id, path FROM assets").fetchall():
        if row["path"] not in indexed_paths:
            db.delete_asset_by_path(row["path"])
            logger.debug("Removed missing path: %s", row["path"])

    logger.info("Update complete. %d assets in index.", len(indexed_paths))


def run_query(db: MediaDatabase, query_text: str, logger: logging.Logger) -> None:
    """
    Search mode: embed query with MLX-CLIP, run MATCH against vec_index,
    return top 5 file paths sorted by distance.
    """
    db.init_schema()
    embedder = ImageEmbedder()
    query_vec = embedder.get_text_embedding(query_text)
    results = db.search(query_vec, k=5)

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
