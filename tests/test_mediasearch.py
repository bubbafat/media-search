"""Tests for mediasearch.py."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from mediasearch import (
    EMBEDDING_DIM,
    FileCrawler,
    MediaDatabase,
    MEDIA_EXTENSIONS,
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
