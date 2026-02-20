"""Tests for mediasearch.py."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from mediasearch import (
    EMBEDDING_DIM,
    FileCrawler,
    MediaDatabase,
    MEDIA_EXTENSIONS,
    SPARSE_HASH_CHUNK_BYTES,
    SPARSE_HASH_THRESHOLD_BYTES,
    VideoThumbnailer,
    get_metadata,
    main,
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


def _sqlite_has_load_extension() -> bool:
    import sqlite3
    conn = sqlite3.connect(":memory:")
    try:
        conn.enable_load_extension(True)
        return True
    except AttributeError:
        return False
    finally:
        conn.close()


@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_media_database_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    db = MediaDatabase(db_path)
    db.connect()
    db.init_schema()
    db.init_schema()  # idempotent
    conn = db.connect()
    r = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'assets'").fetchone()
    assert r is not None
    r = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'vec_index'").fetchone()
    assert r is not None
    db.close()


@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_media_database_upsert_and_search(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
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


# ---- MediaDatabase: batch_upsert_assets ----
@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_batch_upsert_assets(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
        rows = [
            ("/a.jpg", "h1", 1000.0, "IMAGE", "2024-01-01", 47.0, -122.0),
            ("/b.jpg", "h2", 2000.0, "IMAGE", None, None, None),
        ]
        db.batch_upsert_assets(rows)
        r1 = db.get_asset_by_path("/a.jpg")
        r2 = db.get_asset_by_path("/b.jpg")
        assert r1 is not None and r1["hash"] == "h1" and r1["capture_date"] == "2024-01-01" and r1["lat"] == 47.0
        assert r2 is not None and r2["hash"] == "h2" and r2["capture_date"] is None and r2["lat"] is None


@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_batch_upsert_assets_empty_no_op(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
        db.batch_upsert_assets([])
        assert db.connect().execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_batch_upsert_assets_on_conflict_updates(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
        db.batch_upsert_assets([("/same.jpg", "hash1", 1000.0, "IMAGE", None, None, None)])
        db.batch_upsert_assets([("/same.jpg", "hash2", 2000.0, "IMAGE", None, None, None)])
        row = db.get_asset_by_path("/same.jpg")
        assert row is not None and row["hash"] == "hash2" and row["mtime"] == 2000.0


# ---- MediaDatabase: delete_asset_by_path ----
@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_delete_asset_by_path_removes_asset_and_embedding(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
        aid = db.upsert_asset("/gone.jpg", "h", 1000.0, "IMAGE")
        db.set_embedding(aid, [0.1] * EMBEDDING_DIM)
        db.delete_asset_by_path("/gone.jpg")
        assert db.get_asset_by_path("/gone.jpg") is None


@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_delete_asset_by_path_nonexistent_no_op(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
        db.delete_asset_by_path("/nonexistent.jpg")
        assert db.connect().execute("SELECT COUNT(*) FROM assets").fetchone()[0] == 0


# ---- MediaDatabase: optional columns ----
@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_upsert_asset_optional_capture_date_lat_lon(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    with MediaDatabase(db_path) as db:
        db.init_schema()
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
@pytest.mark.skipif(not _sqlite_has_load_extension(), reason="SQLite load_extension not available")
def test_init_schema_adds_missing_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
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
    conn.close()
    db = MediaDatabase(db_path)
    db.connect()
    db.init_schema()
    row = db.connect().execute("PRAGMA table_info(assets)").fetchall()
    col_names = {r[1] for r in row}
    assert "capture_date" in col_names and "lat" in col_names and "lon" in col_names
    db.close()


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
    with patch("sys.argv", ["mediasearch", "query", "sunset beach"]):
        exit_code = main()
    assert exit_code == 0


def test_cli_db_and_verbose_accepted(tmp_path: Path) -> None:
    with patch("sys.argv", ["mediasearch", "--db", str(tmp_path / "x.db"), "query", "test"]):
        exit_code = main()
    assert exit_code == 0
