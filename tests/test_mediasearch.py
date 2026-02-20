"""Tests for mediasearch.py."""

from __future__ import annotations

import logging
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    import ffmpeg
except ImportError:
    ffmpeg = None  # type: ignore[assignment]

from mediasearch import (
    EMBEDDING_DIM,
    FileCrawler,
    ImageEmbedder,
    MediaDatabase,
    MEDIA_EXTENSIONS,
    SPARSE_HASH_CHUNK_BYTES,
    SPARSE_HASH_THRESHOLD_BYTES,
    VideoThumbnailer,
    get_metadata,
    main,
    run_query,
    run_update,
)
from mediasearch import (
    _parse_dms_single,
    _parse_gps_position,
    _progress_bar,
)


def test_media_extensions() -> None:
    assert MEDIA_EXTENSIONS[".jpg"] == "IMAGE"
    assert MEDIA_EXTENSIONS[".jpeg"] == "IMAGE"
    assert MEDIA_EXTENSIONS[".arw"] == "RAW"
    assert MEDIA_EXTENSIONS[".mp4"] == "VIDEO"
    assert MEDIA_EXTENSIONS[".mov"] == "VIDEO"


def test_path_to_type() -> None:
    assert FileCrawler.path_to_type(Path("a.jpg")) == "IMAGE"
    assert FileCrawler.path_to_type(Path("a.JPEG")) == "IMAGE"
    assert FileCrawler.path_to_type(Path("a.ARW")) == "RAW"
    assert FileCrawler.path_to_type(Path("b.MP4")) == "VIDEO"
    assert FileCrawler.path_to_type(Path("b.mov")) == "VIDEO"
    assert FileCrawler.path_to_type(Path("c.png")) == "unknown"


def test_file_crawler_finds_media(tmp_path: Path) -> None:
    (tmp_path / "photo.jpg").write_bytes(b"x")
    (tmp_path / "video.MOV").write_bytes(b"y")
    (tmp_path / "raw.ARW").write_bytes(b"z")
    (tmp_path / "skip.txt").write_bytes(b"skip")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "nested.jpeg").write_bytes(b"n")

    crawler = FileCrawler(tmp_path)
    found = sorted(str(p) for p in crawler.crawl())
    assert len(found) == 4
    assert any("photo.jpg" in p for p in found)
    assert any("video.MOV" in p for p in found)
    assert any("raw.ARW" in p for p in found)
    assert any("nested.jpeg" in p for p in found)
    assert not any("skip.txt" in p for p in found)


def test_file_crawler_empty_dir(tmp_path: Path) -> None:
    crawler = FileCrawler(tmp_path)
    assert list(crawler.crawl()) == []


def test_get_hash(tmp_path: Path) -> None:
    f = tmp_path / "same.bin"
    f.write_bytes(b"hello")
    crawler = FileCrawler(tmp_path)
    h1 = crawler.get_hash(f)
    h2 = crawler.get_hash(f)
    assert h1 == h2
    assert len(h1) == 64 and all(c in "0123456789abcdef" for c in h1)


def test_get_hash_different_content(tmp_path: Path) -> None:
    (tmp_path / "a.jpg").write_bytes(b"aaa")
    (tmp_path / "b.jpg").write_bytes(b"bbb")
    crawler = FileCrawler(tmp_path)
    assert crawler.get_hash(tmp_path / "a.jpg") != crawler.get_hash(tmp_path / "b.jpg")


def test_get_hash_missing_file() -> None:
    crawler = FileCrawler(Path(tempfile.gettempdir()))
    with pytest.raises(FileNotFoundError):
        crawler.get_hash(Path("/nonexistent/file.jpg"))


def test_crawler_not_a_directory() -> None:
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as t:
        path = Path(t.name)
    try:
        crawler = FileCrawler(path)
        with pytest.raises(NotADirectoryError):
            list(crawler.crawl())
    finally:
        path.unlink(missing_ok=True)


def test_media_database_schema(db_conn: sqlite3.Connection, clear_db: None) -> None:
    r = db_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'assets'").fetchone()
    assert r is not None
    r = db_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_index'").fetchone()
    assert r is not None


def test_media_database_upsert_and_search(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    aid = db.upsert_asset("/fake/photo.jpg", "abc123", 1000.0, "image")
    assert aid > 0
    row = db.get_asset_by_path("/fake/photo.jpg")
    assert row is not None
    assert row["hash"] == "abc123"
    assert row["mtime"] == 1000.0

    # Same path updates
    aid2 = db.upsert_asset("/fake/photo.jpg", "def456", 2000.0, "image")
    assert aid2 == aid
    row2 = db.get_asset_by_path("/fake/photo.jpg")
    assert row2["hash"] == "def456"

    # Embedding and search
    emb = [0.1] * EMBEDDING_DIM
    db.set_embedding(aid, emb)
    results = db.search(emb, k=5)
    assert len(results) >= 1
    assert results[0][0] == aid


# ---- Sparse hash ----
def test_get_hash_sparse_large_file(tmp_path: Path) -> None:
    """For files > threshold, hash uses first/middle/last chunk only; same samples => same hash."""
    chunk = 50
    threshold = 100
    # File size 200: first 50, mid 50 (bytes 75-125), last 50 (bytes 150-200)
    with patch("mediasearch.SPARSE_HASH_THRESHOLD_BYTES", threshold), patch(
        "mediasearch.SPARSE_HASH_CHUNK_BYTES", chunk
    ):
        crawler = FileCrawler(tmp_path)
        # Same first 50, same mid 50, same last 50
        a = tmp_path / "a.mov"
        b = tmp_path / "b.mov"
        # Build 200-byte files: bytes 0-50, 75-125, 150-200 must match
        buf_a = bytearray(200)
        buf_a[0:50] = b"x" * 50
        buf_a[75:125] = b"m" * 50
        buf_a[150:200] = b"e" * 50
        buf_b = bytearray(200)
        buf_b[0:50] = b"x" * 50
        buf_b[75:125] = b"m" * 50
        buf_b[150:200] = b"e" * 50
        a.write_bytes(buf_a)
        b.write_bytes(buf_b)
        assert crawler.get_hash(a) == crawler.get_hash(b)


def test_get_hash_sparse_different_middle_different_hash(tmp_path: Path) -> None:
    """Sparse hash: different middle chunk => different hash."""
    chunk = 50
    threshold = 100
    with patch("mediasearch.SPARSE_HASH_THRESHOLD_BYTES", threshold), patch(
        "mediasearch.SPARSE_HASH_CHUNK_BYTES", chunk
    ):
        crawler = FileCrawler(tmp_path)
        a = tmp_path / "a.mov"
        b = tmp_path / "b.mov"
        buf = bytearray(200)
        buf[0:50] = b"x" * 50
        buf[75:125] = b"m1"
        buf[150:200] = b"e" * 50
        a.write_bytes(buf)
        buf[75:125] = b"m2"
        b.write_bytes(buf)
        assert crawler.get_hash(a) != crawler.get_hash(b)


# ---- GPS parsing ----
def test_parse_gps_position_valid() -> None:
    lat, lon = _parse_gps_position('47 deg 12\' 34.56" N, 122 deg 45\' 67.89" W')
    assert lat is not None and lon is not None
    assert 47.0 < lat < 48.0
    assert -123.0 < lon < -122.0


def test_parse_gps_position_empty_none() -> None:
    assert _parse_gps_position("") == (None, None)
    assert _parse_gps_position(None) == (None, None)  # type: ignore[arg-type]


def test_parse_gps_position_malformed() -> None:
    assert _parse_gps_position("not valid") == (None, None)
    assert _parse_gps_position("47 N") == (None, None)
    assert _parse_gps_position("47 deg 12' 34\" N") == (None, None)  # only one part


def test_parse_dms_single_valid() -> None:
    assert _parse_dms_single('47 deg 12\' 34.56" N') is not None
    assert _parse_dms_single('122 deg 45\' 67.89" W') is not None
    # S/W => negative
    south = _parse_dms_single('33 deg 55\' 0" S')
    assert south is not None and south < 0


def test_parse_dms_single_invalid() -> None:
    assert _parse_dms_single("") is None
    assert _parse_dms_single("nope") is None
    assert _parse_dms_single(None) is None  # type: ignore[arg-type]


# ---- get_metadata ----
def test_get_metadata_returns_structure(tmp_path: Path) -> None:
    (tmp_path / "dummy.jpg").write_bytes(b"x")
    out = get_metadata(tmp_path / "dummy.jpg")
    assert "capture_date" in out and "lat" in out and "lon" in out
    assert out["lat"] is None or isinstance(out["lat"], (int, float))
    assert out["lon"] is None or isinstance(out["lon"], (int, float))


@patch("mediasearch.ExifToolHelper")
def test_get_metadata_fallback_gps_lat_lon(mock_exif_class: object, tmp_path: Path) -> None:
    """When Composite:GPSPosition is missing, use EXIF:GPSLatitude and EXIF:GPSLongitude."""
    (tmp_path / "f.jpg").write_bytes(b"x")
    mock_exif_class.return_value.__enter__.return_value.get_tags.return_value = [
        {
            "EXIF:DateTimeOriginal": "2024:01:15 10:30:00",
            "Composite:GPSPosition": None,
            "EXIF:GPSLatitude": '47 deg 36\' 0" N',
            "EXIF:GPSLongitude": '122 deg 20\' 0" W',
        }
    ]
    out = get_metadata(tmp_path / "f.jpg")
    assert out["capture_date"] == "2024:01:15 10:30:00"
    assert out["lat"] is not None and 47.0 < out["lat"] < 48.0
    assert out["lon"] is not None and -123.0 < out["lon"] < -122.0


def test_get_metadata_when_exiftool_helper_is_none(tmp_path: Path) -> None:
    """When PyExifTool is not installed, return default dict with None values."""
    (tmp_path / "dummy.jpg").write_bytes(b"x")
    with patch("mediasearch.ExifToolHelper", None):
        out = get_metadata(tmp_path / "dummy.jpg")
    assert out["capture_date"] is None
    assert out["lat"] is None
    assert out["lon"] is None


@patch("mediasearch.ExifToolHelper")
def test_get_metadata_get_tags_raises_returns_default(mock_exif_class: object, tmp_path: Path) -> None:
    """When get_tags raises, return default dict (no crash)."""
    (tmp_path / "f.jpg").write_bytes(b"x")
    mock_exif_class.return_value.__enter__.return_value.get_tags.side_effect = OSError("exiftool not found")
    out = get_metadata(tmp_path / "f.jpg")
    assert out["capture_date"] is None
    assert out["lat"] is None
    assert out["lon"] is None


@patch("mediasearch.ExifToolHelper")
def test_get_metadata_capture_date_fallback_datetime_created(mock_exif_class: object, tmp_path: Path) -> None:
    """When EXIF:DateTimeOriginal is missing, use Composite:DateTimeCreated for capture_date."""
    (tmp_path / "f.jpg").write_bytes(b"x")
    mock_exif_class.return_value.__enter__.return_value.get_tags.return_value = [
        {
            "EXIF:DateTimeOriginal": None,
            "Composite:DateTimeCreated": "2024:06:20 14:00:00",
            "Composite:GPSPosition": None,
            "EXIF:GPSLatitude": None,
            "EXIF:GPSLongitude": None,
        }
    ]
    out = get_metadata(tmp_path / "f.jpg")
    assert out["capture_date"] == "2024:06:20 14:00:00"
    assert out["lat"] is None
    assert out["lon"] is None


# ---- MediaDatabase: batch_upsert_assets ----
def test_batch_upsert_assets(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    rows = [
        ("/a.jpg", "h1", 1000.0, "IMAGE", "2024-01-01", 47.0, -122.0),
        ("/b.jpg", "h2", 2000.0, "IMAGE", None, None, None),
    ]
    db.batch_upsert_assets(rows)
    r1 = db.get_asset_by_path("/a.jpg")
    r2 = db.get_asset_by_path("/b.jpg")
    assert r1 is not None and r1["hash"] == "h1" and r1["capture_date"] == "2024-01-01" and r1["lat"] == 47.0
    assert r2 is not None and r2["hash"] == "h2" and r2["capture_date"] is None and r2["lat"] is None


def test_batch_upsert_assets_empty_no_op(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    db.batch_upsert_assets([])
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_batch_upsert_assets_on_conflict_updates(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    db.batch_upsert_assets([("/same.jpg", "hash1", 1000.0, "IMAGE", None, None, None)])
    db.batch_upsert_assets([("/same.jpg", "hash2", 2000.0, "IMAGE", None, None, None)])
    row = db.get_asset_by_path("/same.jpg")
    assert row is not None and row["hash"] == "hash2" and row["mtime"] == 2000.0


# ---- MediaDatabase: delete_asset_by_path ----
def test_delete_asset_by_path_removes_asset_and_embedding(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    aid = db.upsert_asset("/gone.jpg", "h", 1000.0, "IMAGE")
    db.set_embedding(aid, [0.1] * EMBEDDING_DIM)
    db.delete_asset_by_path("/gone.jpg")
    assert db.get_asset_by_path("/gone.jpg") is None


def test_delete_asset_by_path_nonexistent_no_op(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    db.delete_asset_by_path("/nonexistent.jpg")
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


def test_set_embedding_wrong_length_raises(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    aid = db.upsert_asset("/p.jpg", "h", 1000.0, "IMAGE")
    with pytest.raises(ValueError, match=f"embedding length must be {EMBEDDING_DIM}"):
        db.set_embedding(aid, [0.1] * (EMBEDDING_DIM - 1))
    with pytest.raises(ValueError, match=f"embedding length must be {EMBEDDING_DIM}"):
        db.set_embedding(aid, [0.1] * (EMBEDDING_DIM + 1))


def test_search_wrong_query_length_raises(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    with pytest.raises(ValueError, match=f"embedding length must be {EMBEDDING_DIM}"):
        db.search([0.1] * (EMBEDDING_DIM - 1), k=5)
    with pytest.raises(ValueError, match=f"embedding length must be {EMBEDDING_DIM}"):
        db.search([0.1] * (EMBEDDING_DIM + 1), k=5)


def test_rebuild_schema_drops_and_recreates_tables(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    db.upsert_asset("/a.jpg", "h1", 1000.0, "IMAGE")
    db.upsert_asset("/b.jpg", "h2", 2000.0, "IMAGE")
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 2
    db.rebuild_schema()
    assert db_conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0
    r = db_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'assets'").fetchone()
    assert r is not None
    r = db_conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_index'").fetchone()
    assert r is not None


def test_batch_save_embeddings_stores_and_searchable(db_conn: sqlite3.Connection, clear_db: None) -> None:
    """batch_save_embeddings persists multiple embeddings in one transaction; all are searchable."""
    db = MediaDatabase.from_connection(db_conn)
    a1 = db.upsert_asset("/p1.jpg", "h1", 1000.0, "IMAGE")
    a2 = db.upsert_asset("/p2.jpg", "h2", 2000.0, "IMAGE")
    vec1 = [0.1] * EMBEDDING_DIM
    vec2 = [0.2] * EMBEDDING_DIM
    db.batch_save_embeddings([(a1, vec1), (a2, vec2)])
    results1 = db.search(vec1, k=5)
    results2 = db.search(vec2, k=5)
    assert any(r[0] == a1 for r in results1)
    assert any(r[0] == a2 for r in results2)


def test_batch_save_embeddings_empty_no_op(db_conn: sqlite3.Connection, clear_db: None) -> None:
    """batch_save_embeddings with empty list is a no-op."""
    db = MediaDatabase.from_connection(db_conn)
    db.batch_save_embeddings([])
    assert db_conn.execute("SELECT COUNT(*) FROM vec_index").fetchone()[0] == 0


def test_batch_save_embeddings_wrong_length_raises(db_conn: sqlite3.Connection, clear_db: None) -> None:
    """batch_save_embeddings raises ValueError if any embedding has wrong length."""
    db = MediaDatabase.from_connection(db_conn)
    aid = db.upsert_asset("/p.jpg", "h", 1000.0, "IMAGE")
    with pytest.raises(ValueError, match=f"embedding length must be {EMBEDDING_DIM}"):
        db.batch_save_embeddings([(aid, [0.1] * (EMBEDDING_DIM - 1))])


# ---- MediaDatabase: optional columns ----
def test_upsert_asset_optional_capture_date_lat_lon(db_conn: sqlite3.Connection, clear_db: None) -> None:
    db = MediaDatabase.from_connection(db_conn)
    db.upsert_asset(
        "/p.jpg", "h", 1000.0, "IMAGE",
        capture_date="2024:06:15 12:00:00",
        lat=48.5,
        lon=-121.2,
    )
    row = db.get_asset_by_path("/p.jpg")
    assert row is not None
    assert row["capture_date"] == "2024:06:15 12:00:00"
    assert row["lat"] == 48.5
    assert row["lon"] == -121.2


# ---- VideoThumbnailer ----
def test_video_thumbnailer_thumbnail_path(tmp_path: Path) -> None:
    t = VideoThumbnailer(thumb_dir=tmp_path)
    p = t.thumbnail_path("abc123def")
    assert p == tmp_path / "abc123def.jpg"
    assert p.parent == tmp_path


def test_video_thumbnailer_init_creates_dir(tmp_path: Path) -> None:
    thumb_dir = tmp_path / "thumbs"
    assert not thumb_dir.exists()
    VideoThumbnailer(thumb_dir=thumb_dir)
    assert thumb_dir.is_dir()


def test_ensure_thumbnail_returns_existing_path(tmp_path: Path) -> None:
    """When thumbnail file already exists, return it without calling ffmpeg."""
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    existing = thumb_dir / "abc123.jpg"
    existing.write_bytes(b"existing")
    t = VideoThumbnailer(thumb_dir=thumb_dir)
    video = tmp_path / "v.mov"
    video.write_bytes(b"fake")
    with patch("mediasearch.ffmpeg") as mock_ffmpeg:
        result = t.ensure_thumbnail(video, "abc123")
    assert result == existing
    mock_ffmpeg.input.assert_not_called()


def test_ensure_thumbnail_creates_file_when_ffmpeg_succeeds(tmp_path: Path) -> None:
    """When thumbnail missing, ffmpeg runs with VideoToolbox and creates the file."""
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    t = VideoThumbnailer(thumb_dir=thumb_dir)
    video = tmp_path / "v.mov"
    video.write_bytes(b"fake")
    out_path = thumb_dir / "hash99.jpg"

    def touch_thumb(*args: object, **kwargs: object) -> None:
        out_path.touch()

    with patch("mediasearch.ffmpeg.input") as mock_input:
        mock_input.return_value.filter.return_value.output.return_value.overwrite_output.return_value.run.side_effect = touch_thumb
        result = t.ensure_thumbnail(video, "hash99")
    assert result == out_path
    assert out_path.exists()
    mock_input.assert_called_once()
    call_kwargs = mock_input.call_args[1]
    assert call_kwargs.get("hwaccel") == "videotoolbox"


def test_ensure_thumbnail_raises_on_ffmpeg_error(tmp_path: Path) -> None:
    """When ffmpeg raises Error, raise RuntimeError with stderr message."""
    if ffmpeg is None:
        pytest.skip("ffmpeg not installed")
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    t = VideoThumbnailer(thumb_dir=thumb_dir)
    video = tmp_path / "v.mov"
    video.write_bytes(b"fake")
    with patch("mediasearch.ffmpeg.input") as mock_input:
        mock_input.return_value.filter.return_value.output.return_value.overwrite_output.return_value.run.side_effect = ffmpeg.Error(
            cmd=["ffmpeg"], stdout=b"", stderr=b"Invalid data"
        )
        with pytest.raises(RuntimeError, match="FFmpeg thumbnail failed") as exc_info:
            t.ensure_thumbnail(video, "x")
    assert "Invalid data" in str(exc_info.value)


def test_ensure_thumbnail_raises_when_ffmpeg_module_is_none(tmp_path: Path) -> None:
    """When ffmpeg-python is not installed, raise RuntimeError."""
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()
    t = VideoThumbnailer(thumb_dir=thumb_dir)
    video = tmp_path / "v.mov"
    video.write_bytes(b"fake")
    with patch("mediasearch.ffmpeg", None):
        with pytest.raises(RuntimeError, match="ffmpeg-python is not installed"):
            t.ensure_thumbnail(video, "y")


# ---- ImageEmbedder: batch and lazy load ----
def test_get_image_embeddings_batch_empty_returns_empty_without_loading() -> None:
    """get_image_embeddings_batch([]) returns [] and does not load the model."""
    embedder = ImageEmbedder()
    with patch("mediasearch._get_clip_model") as mock_load:
        result = embedder.get_image_embeddings_batch([])
    assert result == []
    mock_load.assert_not_called()


# ---- _progress_bar ----
def test_progress_bar_format() -> None:
    s = _progress_bar(5, 10, width=10)
    assert "5/10" in s
    assert s.startswith("[") and "]" in s


def test_progress_bar_total_zero() -> None:
    s = _progress_bar(0, 0)
    assert "0/0" in s


def test_progress_bar_complete() -> None:
    s = _progress_bar(10, 10, width=10)
    assert "10/10" in s


# ---- Schema migration ----
def test_init_schema_adds_missing_columns() -> None:
    from conftest import _memory_conn_with_sqlite_vec
    conn = _memory_conn_with_sqlite_vec()
    try:
        conn.execute("""
            CREATE TABLE assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                hash TEXT NOT NULL,
                mtime REAL NOT NULL,
                type TEXT NOT NULL
            )
        """)
        conn.commit()
        db = MediaDatabase.from_connection(conn)
        db.init_schema()
        row = conn.execute("PRAGMA table_info(assets)").fetchall()
        col_names = {r[1] for r in row}
        assert "capture_date" in col_names and "lat" in col_names and "lon" in col_names
    finally:
        conn.close()


# ---- connect() failure ----
def test_media_database_connect_raises_when_no_load_extension(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db = MediaDatabase(db_path)
    with patch("mediasearch.sqlite3.connect") as mock_connect:
        mock_conn = mock_connect.return_value
        mock_conn.enable_load_extension.side_effect = AttributeError
        with pytest.raises(RuntimeError, match="load_extension"):
            db.connect()
    assert db._conn is None


# ---- CLI ----
def test_cli_rebuild_requires_path() -> None:
    with patch("sys.argv", ["mediasearch", "rebuild"]):
        with pytest.raises(SystemExit):
            main()


def test_cli_update_requires_path() -> None:
    with patch("sys.argv", ["mediasearch", "update"]):
        with pytest.raises(SystemExit):
            main()


def test_cli_query_accepts_query_string(tmp_path: Path) -> None:
    # Avoid loading MLX (ImageEmbedder) when no Metal device
    with patch("sys.argv", ["mediasearch", "--db", str(tmp_path / "q.db"), "query", "sunset beach"]), patch(
        "mediasearch.run_query"
    ) as mock_run_query:
        exit_code = main()
    assert exit_code == 0
    mock_run_query.assert_called_once()
    assert mock_run_query.call_args[0][1] == "sunset beach"


def test_cli_db_and_verbose_accepted(tmp_path: Path) -> None:
    with patch("sys.argv", ["mediasearch", "--db", str(tmp_path / "x.db"), "query", "test"]), patch(
        "mediasearch.run_query"
    ):
        exit_code = main()
    assert exit_code == 0


# ---- run_update: stale asset removal ----
def test_run_update_removes_assets_no_longer_on_disk(
    db_conn: sqlite3.Connection, clear_db: None, tmp_path: Path
) -> None:
    """When crawl returns fewer paths than in DB, run_update deletes the missing assets."""
    db = MediaDatabase.from_connection(db_conn)
    db.upsert_asset("/stale/removed.jpg", "h1", 1000.0, "IMAGE")
    assert db.get_asset_by_path("/stale/removed.jpg") is not None

    logger = logging.getLogger("test_run_update")
    with patch.object(FileCrawler, "crawl", return_value=iter([])):
        run_update(db, tmp_path, logger)

    assert db.get_asset_by_path("/stale/removed.jpg") is None


# ---- run_query with mock embedder ----
def test_run_query_searches_and_logs_results(db_conn: sqlite3.Connection, clear_db: None) -> None:
    """run_query embeds text, runs search with k=5, and logs top paths (no MLX load)."""
    db = MediaDatabase.from_connection(db_conn)
    aid = db.upsert_asset("/photos/beach.jpg", "h", 1000.0, "IMAGE")
    db.set_embedding(aid, [0.1] * EMBEDDING_DIM)

    logger = logging.getLogger("test_run_query")
    with patch("mediasearch.ImageEmbedder") as mock_embedder_class:
        mock_embedder_class.return_value.get_text_embedding.return_value = [0.1] * EMBEDDING_DIM
        run_query(db, "sunset beach", logger)

    mock_embedder_class.return_value.get_text_embedding.assert_called_once_with("sunset beach")
    results = db.search([0.1] * EMBEDDING_DIM, k=5)
    assert len(results) >= 1
    assert results[0][0] == aid
