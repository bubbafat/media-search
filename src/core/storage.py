"""Local media store: sharded thumbnails and proxies under data_dir."""

from pathlib import Path

from PIL import Image
from PIL import ImageOps

from src.core.config import get_config


class LocalMediaStore:
    """
    Stores thumbnails and proxies under data_dir, sharded by library_slug and asset_id.
    Directory layout: data_dir / library_slug / category / (asset_id % 1000) / {asset_id}.jpg
    """

    def __init__(self) -> None:
        self.data_dir = Path(get_config().data_dir)

    def load_source_image(self, source_path: Path | str) -> Image.Image:
        """Open image, fix EXIF orientation, return RGB."""
        path = Path(source_path)
        img = Image.open(path)
        img = ImageOps.exif_transpose(img)
        return img.convert("RGB")

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
        """Create 1024x1024 proxy and save as JPEG quality 85."""
        path = self._get_shard_path(library_slug, asset_id, "proxies", create_dirs=True)
        proxy = image.copy()
        proxy.thumbnail((1024, 1024))
        proxy.save(path, "JPEG", quality=85)

    def get_thumbnail_path(self, library_slug: str, asset_id: int) -> Path:
        """Return path to thumbnail; raise FileNotFoundError if it does not exist."""
        path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def get_proxy_path(self, library_slug: str, asset_id: int) -> Path:
        """Return path to proxy; raise FileNotFoundError if it does not exist."""
        path = self._get_shard_path(library_slug, asset_id, "proxies")
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def proxy_and_thumbnail_exist(self, library_slug: str, asset_id: int) -> bool:
        """Return True if both proxy and thumbnail files exist. Used by proxy --repair."""
        proxy_path = self._get_shard_path(library_slug, asset_id, "proxies")
        thumb_path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        return proxy_path.exists() and thumb_path.exists()
