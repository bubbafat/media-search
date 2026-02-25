"""Tests for LocalMediaStore (shard path, save/load thumbnail and proxy)."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from PIL import Image

from src.core.storage import LocalMediaStore


@pytest.fixture
def temp_data_dir():
    """Temporary directory for data_dir; patch get_config to return it."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(path)
            store = LocalMediaStore()
            assert store.data_dir == path
            yield store, path


def test_get_shard_path_structure(temp_data_dir):
    """_get_shard_path returns library_slug/category/shard/asset_id.jpg."""
    store, data_dir = temp_data_dir
    path = store._get_shard_path("my-lib", 42, "thumbnails", create_dirs=True)
    assert path == data_dir / "my-lib" / "thumbnails" / "42" / "42.jpg"
    assert path.parent.exists()


def test_get_shard_path_shard_modulo(temp_data_dir):
    """Shard is asset_id % 1000."""
    store, data_dir = temp_data_dir
    p1 = store._get_shard_path("lib", 1005, "proxies", create_dirs=True)
    assert "5" in str(p1.parent)
    p2 = store._get_shard_path("lib", 2000, "proxies", create_dirs=True)
    assert str(p2.parent).endswith("0")


def test_save_and_get_thumbnail_path(temp_data_dir):
    """save_thumbnail creates file; get_thumbnail_path returns it."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_thumbnail("lib1", 1, img)
    path = store.get_thumbnail_path("lib1", 1)
    assert path.exists()
    assert path.suffix == ".jpg"


def test_save_and_get_proxy_path(temp_data_dir):
    """save_proxy creates file; get_proxy_path returns it."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (2000, 2000), color="blue")
    store.save_proxy("lib1", 2, img)
    path = store.get_proxy_path("lib1", 2)
    assert path.exists()
    assert path.suffix == ".jpg"


def test_get_thumbnail_path_raises_when_missing(temp_data_dir):
    """get_thumbnail_path raises FileNotFoundError when file does not exist."""
    store, _ = temp_data_dir
    with pytest.raises(FileNotFoundError):
        store.get_thumbnail_path("lib1", 999)


def test_get_proxy_path_raises_when_missing(temp_data_dir):
    """get_proxy_path raises FileNotFoundError when file does not exist."""
    store, _ = temp_data_dir
    with pytest.raises(FileNotFoundError):
        store.get_proxy_path("lib1", 999)


def test_proxy_and_thumbnail_exist_true_when_both_present(temp_data_dir):
    """proxy_and_thumbnail_exist returns True when both files exist."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_thumbnail("lib1", 1, img)
    store.save_proxy("lib1", 1, img)
    assert store.proxy_and_thumbnail_exist("lib1", 1) is True


def test_proxy_and_thumbnail_exist_false_when_proxy_missing(temp_data_dir):
    """proxy_and_thumbnail_exist returns False when proxy file is missing."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_thumbnail("lib1", 1, img)
    assert store.proxy_and_thumbnail_exist("lib1", 1) is False


def test_proxy_and_thumbnail_exist_false_when_thumbnail_missing(temp_data_dir):
    """proxy_and_thumbnail_exist returns False when thumbnail file is missing."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_proxy("lib1", 1, img)
    assert store.proxy_and_thumbnail_exist("lib1", 1) is False


def test_load_source_image_exif_and_rgb(tmp_path):
    """load_source_image opens image, applies exif_transpose, returns RGB."""
    rgb_path = tmp_path / "test.jpg"
    img = Image.new("RGB", (10, 10), color="green")
    img.save(rgb_path, "JPEG")
    with patch("src.core.storage.get_config") as m:
        m.return_value.data_dir = str(tmp_path)
        store = LocalMediaStore()
    loaded = store.load_source_image(rgb_path)
    assert loaded.mode == "RGB"
