"""Local media store: sharded thumbnails and proxies under data_dir."""

from pathlib import Path

from PIL import Image
from PIL import ImageOps

from src.core.config import get_config


def _load_image_via_pyvips(path: Path) -> Image.Image:
    """Open image with pyvips (e.g. RAW, DNG), return PIL Image in RGB."""
    import pyvips

    vips_img = pyvips.Image.new_from_file(str(path))
    # Ensure sRGB 3-band for consistency (libvips may return 16-bit or different colourspace)
    if getattr(vips_img, "interpretation", None) != "srgb":
        vips_img = vips_img.colourspace("srgb")
    if getattr(vips_img, "format", None) != "uchar":
        vips_img = vips_img.cast("uchar")
    arr = vips_img.numpy()
    if arr.ndim == 2:
        pil_img = Image.fromarray(arr).convert("RGB")
    else:
        pil_img = Image.fromarray(arr, "RGB")
    return pil_img


class LocalMediaStore:
    """
    Stores thumbnails and proxies under data_dir, sharded by library_slug and asset_id.
    Directory layout: data_dir / library_slug / category / (asset_id % 1000) / {asset_id}.jpg
    (proxies use .webp instead of .jpg).
    """

    PROXY_EXTENSION = ".webp"

    def __init__(self) -> None:
        self.data_dir = Path(get_config().data_dir)

    def load_source_image(self, source_path: Path | str, *, use_previews: bool = True) -> Image.Image:
        """Open image, fix EXIF orientation, return RGB.

        Uses Pillow first for common formats. For RAW/DNG/unsupported formats, may use a
        fast-path preview when use_previews is True, otherwise falls back to a full
        pyvips decode.
        """
        path = Path(source_path)
        try:
            img = Image.open(path)
            img.load()
            img = ImageOps.exif_transpose(img)
            return img.convert("RGB")
        except (OSError, ValueError) as e:
            # UnidentifiedImageError subclasses OSError; for RAW/DNG/other formats try pyvips.
            # Respect the use_previews flag to optionally take a lower-resolution, fast-path
            # decode when available to reduce memory usage.
            try:
                if use_previews:
                    try:
                        import pyvips  # type: ignore[import]

                        # Use libvips thumbnail API which prefers embedded previews or
                        # shrink-on-load where possible. Target a long edge of ~1280px to
                        # keep enough detail while keeping memory bounded.
                        thumb_vips = pyvips.Image.thumbnail(str(path), 1280)
                        arr = thumb_vips.numpy()
                        if arr.ndim == 2:
                            pil_img = Image.fromarray(arr).convert("RGB")
                        else:
                            pil_img = Image.fromarray(arr, "RGB")
                        return pil_img
                    except Exception:
                        # Fall back to full decode path below.
                        pass

                img = _load_image_via_pyvips(path)
                return img
            except Exception:
                raise e

    def _get_shard_path(
        self,
        library_slug: str,
        asset_id: int,
        category: str,
        *,
        create_dirs: bool = False,
    ) -> Path:
        shard = asset_id % 1000
        directory = self.data_dir / library_slug / category / str(shard)
        if create_dirs:
            directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{asset_id}.jpg"

    def save_thumbnail(self, library_slug: str, asset_id: int, image: Image.Image) -> None:
        """Create 320x320 thumbnail and save as JPEG quality 85."""
        path = self._get_shard_path(library_slug, asset_id, "thumbnails", create_dirs=True)
        thumb = image.copy()
        thumb.thumbnail((320, 320))
        thumb.save(path, "JPEG", quality=85)

    def save_proxy(self, library_slug: str, asset_id: int, image: Image.Image) -> None:
        """Create 768x768 proxy and save as WebP (quality 85)."""
        path = self._get_proxy_path(library_slug, asset_id, create_dirs=True)
        proxy = image.copy()
        proxy.thumbnail((768, 768))
        proxy.save(path, "WEBP", quality=85)

    def _get_proxy_path(
        self,
        library_slug: str,
        asset_id: int,
        *,
        create_dirs: bool = False,
    ) -> Path:
        """Return path for proxy file (WebP). Used by save_proxy, get_proxy_path, proxy_and_thumbnail_exist."""
        shard = asset_id % 1000
        directory = self.data_dir / library_slug / "proxies" / str(shard)
        if create_dirs:
            directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{asset_id}{self.PROXY_EXTENSION}"

    def get_thumbnail_write_path(self, library_slug: str, asset_id: int) -> Path:
        """Return path for writing a thumbnail; creates parent dirs. Used for video thumbnails."""
        return self._get_shard_path(library_slug, asset_id, "thumbnails", create_dirs=True)

    def get_thumbnail_path(self, library_slug: str, asset_id: int) -> Path:
        """Return path to thumbnail; raise FileNotFoundError if it does not exist."""
        path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def thumbnail_exists(self, library_slug: str, asset_id: int) -> bool:
        """Return True if the thumbnail file exists. Used by proxy --repair for videos."""
        path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        return path.exists()

    def get_proxy_path(self, library_slug: str, asset_id: int) -> Path:
        """Return path to proxy (WebP); raise FileNotFoundError if it does not exist."""
        path = self._get_proxy_path(library_slug, asset_id)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def proxy_and_thumbnail_exist(self, library_slug: str, asset_id: int) -> bool:
        """Return True if both proxy (WebP) and thumbnail files exist. Used by proxy --repair."""
        proxy_path = self._get_proxy_path(library_slug, asset_id)
        thumb_path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        return proxy_path.exists() and thumb_path.exists()
