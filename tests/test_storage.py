"""Tests for LocalMediaStore (shard path, save/load thumbnail and proxy)."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
from PIL import Image

from src.core.storage import LocalMediaStore

pytestmark = [pytest.mark.fast]


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
    """Shard is asset_id % 1000 (thumbnails use _get_shard_path; proxies use _get_proxy_path)."""
    store, data_dir = temp_data_dir
    p1 = store._get_proxy_path("lib", 1005, create_dirs=True)
    assert "5" in str(p1.parent)
    p2 = store._get_proxy_path("lib", 2000, create_dirs=True)
    assert str(p2.parent).endswith("0")


def test_save_and_get_thumbnail_path(temp_data_dir):
    """save_thumbnail creates file; get_thumbnail_path returns it."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_thumbnail("lib1", 1, img)
    path = store.get_thumbnail_path("lib1", 1)
    assert path.exists()
    assert path.suffix == ".jpg"


def test_delete_asset_files_removes_thumbnail_proxy_and_clips(temp_data_dir):
    """delete_asset_files removes thumbnail, proxy, and video clips directory."""
    store, data_dir = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_thumbnail("lib1", 42, img)
    store.save_proxy("lib1", 42, img)
    clips_dir = data_dir / "video_clips" / "lib1" / "42"
    clips_dir.mkdir(parents=True)
    (clips_dir / "head_clip.mp4").write_bytes(b"fake")
    assert store.get_thumbnail_path("lib1", 42).exists()
    assert store.get_proxy_path("lib1", 42).exists()
    assert clips_dir.exists()
    store.delete_asset_files("lib1", 42)
    assert not store._get_shard_path("lib1", 42, "thumbnails").exists()
    assert not store._get_proxy_path("lib1", 42).exists()
    assert not clips_dir.exists()


def test_delete_asset_files_handles_missing_files(temp_data_dir):
    """delete_asset_files does not raise when files do not exist."""
    store, _ = temp_data_dir
    store.delete_asset_files("lib1", 999)


def test_atomic_write_no_tmp_remains_after_success(temp_data_dir):
    """save_proxy and save_thumbnail use atomic writes; no .tmp files remain."""
    store, data_dir = temp_data_dir
    img = Image.new("RGB", (100, 100), color="red")
    store.save_proxy("lib1", 1, img)
    store.save_thumbnail("lib1", 1, img)
    proxy_path = store.get_proxy_path("lib1", 1)
    thumb_path = store.get_thumbnail_path("lib1", 1)
    assert proxy_path.exists()
    assert thumb_path.exists()
    assert not proxy_path.with_suffix(proxy_path.suffix + ".tmp").exists()
    assert not thumb_path.with_suffix(thumb_path.suffix + ".tmp").exists()


def test_atomic_write_removes_tmp_on_failure(temp_data_dir):
    """When write_fn raises, _atomic_write removes the .tmp file in finally."""
    from src.core.storage import _atomic_write

    store, data_dir = temp_data_dir
    dest_path = data_dir / "lib1" / "thumbnails" / "0" / "1.jpg"
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    def write_then_fail(tmp_path: Path) -> None:
        tmp_path.write_bytes(b"partial")
        raise OSError("simulated write failure")

    with pytest.raises(OSError, match="simulated write failure"):
        _atomic_write(dest_path, write_then_fail)

    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    assert not tmp_path.exists()
    assert not dest_path.exists()


def test_save_and_get_proxy_path(temp_data_dir):
    """save_proxy creates WebP file; get_proxy_path returns it; proxy max dimension is 768."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (2000, 2000), color="blue")
    store.save_proxy("lib1", 2, img)
    path = store.get_proxy_path("lib1", 2)
    assert path.exists()
    assert path.suffix == ".webp"
    with Image.open(path) as proxy_im:
        assert max(proxy_im.size) == 768


def test_save_proxy_and_thumbnail_cascades_dimensions(temp_data_dir):
    """save_proxy_and_thumbnail creates both files with expected max dimensions."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (4000, 2000), color="purple")
    store.save_proxy_and_thumbnail("lib1", 3, img)

    proxy_path = store.get_proxy_path("lib1", 3)
    thumb_path = store.get_thumbnail_path("lib1", 3)
    assert proxy_path.exists()
    assert thumb_path.exists()

    with Image.open(proxy_path) as proxy_im:
        assert max(proxy_im.size) == 768
        proxy_size = proxy_im.size

    with Image.open(thumb_path) as thumb_im:
        assert max(thumb_im.size) == 320
        # Thumbnail should not be larger than proxy in any dimension.
        assert thumb_im.size[0] <= proxy_size[0]
        assert thumb_im.size[1] <= proxy_size[1]


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


def test_small_image_not_upscaled_for_proxy_and_thumbnail(temp_data_dir):
    """Icon-sized images are not upscaled for proxy or thumbnail."""
    store, _ = temp_data_dir
    img = Image.new("RGB", (32, 32), color="white")
    store.save_proxy_and_thumbnail("lib1", 5, img)

    proxy_path = store.get_proxy_path("lib1", 5)
    thumb_path = store.get_thumbnail_path("lib1", 5)
    assert proxy_path.exists()
    assert thumb_path.exists()

    with Image.open(proxy_path) as proxy_im:
        assert proxy_im.size == (32, 32)
    with Image.open(thumb_path) as thumb_im:
        assert thumb_im.size == (32, 32)


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


def test_load_source_image_falls_back_to_pyvips_when_pillow_fails(tmp_path):
    """When Pillow cannot open the file, load_source_image uses pyvips and returns RGB."""
    path = tmp_path / "photo.raf"
    path.write_bytes(b"not-a-real-raf")
    fake_rgb = Image.new("RGB", (5, 5), color="red")
    with patch("src.core.storage.get_config") as cfg:
        cfg.return_value.data_dir = str(tmp_path)
        store = LocalMediaStore()
        with patch("src.core.storage.Image") as pil_image:
            pil_image.open.side_effect = OSError("cannot identify image file")
            with patch("src.core.storage._load_image_via_pyvips", return_value=fake_rgb):
                loaded = store.load_source_image(path, use_previews=False)
    assert loaded is fake_rgb
    assert loaded.mode == "RGB"


def test_load_source_image_uses_pyvips_thumbnail_when_pillow_fails_and_previews_enabled(
    tmp_path,
):
    """When use_previews=True and path is RAW, we try rawpy then pyvips thumbnail; Pillow is never used."""
    path = tmp_path / "photo.arw"
    path.write_bytes(b"not-a-real-raw")

    # Fake vips image that returns a small RGB array.
    # Must support autorot() (called by _normalize_vips_image) and interpretation/format for normalization.
    class _FakeVipsImage:
        interpretation = "srgb"
        format = "uchar"

        def autorot(self):
            return self

        def numpy(self):
            return (np.ones((8, 6, 3)) * 255).astype("uint8")

    with patch("src.core.storage.get_config") as cfg:
        cfg.return_value.data_dir = str(tmp_path)
        store = LocalMediaStore()

    # RAW path: rawpy returns None (no embedded thumb), then we use pyvips thumbnail.
    with patch("src.core.storage._load_raw_preview_rawpy", return_value=None):
        with patch("src.core.storage._load_image_via_pyvips") as full_decode:
            with patch("pyvips.Image.thumbnail", return_value=_FakeVipsImage()):
                loaded = store.load_source_image(path, use_previews=True)

    full_decode.assert_not_called()
    assert isinstance(loaded, Image.Image)
    assert loaded.mode == "RGB"


def test_load_source_image_raw_extension_never_calls_pillow(tmp_path):
    """For RAW extensions with use_previews=True, Pillow.open is never called; rawpy or pyvips used."""
    path = tmp_path / "photo.cr2"
    path.write_bytes(b"not-a-real-cr2")
    small_rgb = Image.new("RGB", (100, 100), color="blue")

    with patch("src.core.storage.get_config") as cfg:
        cfg.return_value.data_dir = str(tmp_path)
        store = LocalMediaStore()
    with patch("src.core.storage._load_raw_preview_rawpy", return_value=small_rgb):
        with patch("src.core.storage.Image") as pil_image:
            loaded = store.load_source_image(path, use_previews=True)
    assert loaded is small_rgb
    pil_image.open.assert_not_called()


def test_load_source_image_raw_use_previews_false_uses_full_pyvips(tmp_path):
    """For RAW extension with use_previews=False, we use full pyvips decode only; no rawpy preview."""
    path = tmp_path / "photo.nef"
    path.write_bytes(b"not-a-real-nef")
    fake_rgb = Image.new("RGB", (10, 10), color="red")

    with patch("src.core.storage.get_config") as cfg:
        cfg.return_value.data_dir = str(tmp_path)
        store = LocalMediaStore()
    with patch("src.core.storage._load_raw_preview_rawpy") as rawpy_load:
        with patch("src.core.storage._load_image_via_pyvips", return_value=fake_rgb):
            loaded = store.load_source_image(path, use_previews=False)
    rawpy_load.assert_not_called()
    assert loaded is fake_rgb


def test_generate_proxy_and_thumbnail_raw_uses_rawpy_when_pyvips_fails(tmp_path):
    """For RAW with use_previews, when pyvips thumbnail fails, rawpy preview is used and load_source_image not called."""
    path = tmp_path / "photo.dng"
    path.write_bytes(b"not-a-real-dng")
    small_rgb = Image.new("RGB", (200, 200), color="green")

    with patch("src.core.storage.get_config") as cfg:
        cfg.return_value.data_dir = str(tmp_path)
        store = LocalMediaStore()
    with patch("src.core.storage._vips_thumbnail_from_file", side_effect=RuntimeError("vips fail")):
        with patch("src.core.storage._load_raw_preview_rawpy", return_value=small_rgb):
            with patch.object(store, "load_source_image") as load_src:
                store.generate_proxy_and_thumbnail_from_source(
                    "lib1", 1, path, use_previews=True
                )
    load_src.assert_not_called()
    assert store.get_proxy_path("lib1", 1).exists()
    assert store.get_thumbnail_path("lib1", 1).exists()
