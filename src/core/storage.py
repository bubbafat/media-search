"""Local media store: sharded thumbnails and proxies under data_dir."""

import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from PIL import Image
from PIL import ImageOps

from src.core.config import get_config
from src.core.file_extensions import RAW_EXTENSIONS
from src.core.io_utils import file_non_empty

if TYPE_CHECKING:
    import pyvips  # type: ignore[import]

_log = logging.getLogger(__name__)

# One-time warning when falling back from rawpy (optional dependency or no embedded thumb)
_rawpy_fallback_warned = False


def rawpy_available() -> bool:
    """Return True if rawpy can be imported (e.g. for CLI to warn when missing)."""
    try:
        import rawpy  # noqa: F401
        return True
    except ImportError:
        return False


def _load_raw_preview_rawpy(path: Path) -> Image.Image | None:
    """Load embedded thumbnail/preview from a RAW file via rawpy (LibRaw). Returns None on failure.

    Uses only extract_thumb(); does not full demosaic. Converts to RGB PIL.Image for pipeline.
    """
    try:
        import rawpy  # type: ignore[import]
    except ImportError:
        return None
    try:
        with rawpy.imread(str(path)) as raw:
            thumb = raw.extract_thumb()
        if thumb is None:
            return None
        data = thumb.data
        if data is None:
            return None
        if isinstance(data, bytes):
            pil_img = Image.open(io.BytesIO(data)).convert("RGB")
            return pil_img
        # ndarray (h, w, c)
        import numpy as np
        arr = data if hasattr(data, "shape") else None
        if arr is None:
            return None
        if arr.ndim == 2:
            pil_img = Image.fromarray(arr).convert("RGB")
        else:
            if arr.dtype != np.uint8:
                arr = np.clip(arr, 0, 255).astype(np.uint8)
            if arr.shape[-1] == 3:
                pil_img = Image.fromarray(arr, "RGB")
            else:
                pil_img = Image.fromarray(arr).convert("RGB")
        return pil_img
    except Exception:
        return None


def _normalize_vips_image(vips_img: "pyvips.Image") -> "pyvips.Image":
    """
    Ensure a pyvips image is in 8-bit sRGB for consistent output.
    Applies EXIF orientation so portrait smartphone photos render correctly.
    """
    vips_img = vips_img.autorot()
    interp = getattr(vips_img, "interpretation", None)
    if interp != "srgb" and hasattr(vips_img, "colourspace"):
        vips_img = vips_img.colourspace("srgb")
    fmt = getattr(vips_img, "format", None)
    if fmt != "uchar" and hasattr(vips_img, "cast"):
        vips_img = vips_img.cast("uchar")
    return vips_img


def _vips_to_pil(vips_img: "pyvips.Image") -> Image.Image:
    """
    Convert a pyvips image to a PIL.Image for compatibility boundaries.
    """
    vips_img = _normalize_vips_image(vips_img)
    arr = vips_img.numpy()
    if arr.ndim == 2:
        return Image.fromarray(arr).convert("RGB")
    return Image.fromarray(arr, "RGB")


def _pil_to_vips(img: Image.Image) -> "pyvips.Image":
    """
    Convert a PIL.Image to a pyvips image for internal vips pipelines.
    """
    import pyvips  # type: ignore[import]

    if img.mode != "RGB":
        img = img.convert("RGB")
    arr = bytearray(img.tobytes())
    width, height = img.size
    vips_img = pyvips.Image.new_from_memory(
        bytes(arr),
        width,
        height,
        3,
        "uchar",
    )
    return _normalize_vips_image(vips_img)


def _load_image_via_pyvips(path: Path) -> Image.Image:
    """Open image with pyvips (e.g. RAW, DNG), return PIL Image in RGB."""
    import pyvips  # type: ignore[import]

    vips_img = pyvips.Image.new_from_file(str(path), access="sequential")
    vips_img = _normalize_vips_image(vips_img)
    arr = vips_img.numpy()
    if arr.ndim == 2:
        pil_img = Image.fromarray(arr).convert("RGB")
    else:
        pil_img = Image.fromarray(arr, "RGB")
    return pil_img


def _vips_thumbnail_from_file(path: Path, max_size: tuple[int, int]) -> "pyvips.Image":
    """
    Create a pyvips thumbnail from a file using shrink-on-load.

    The result fits within max_size (no upscaling) and uses sequential access
    to bound memory for large inputs.
    """
    import pyvips  # type: ignore[import]

    width, height = max_size
    thumb = pyvips.Image.thumbnail(
        str(path),
        width,
        height=height,
        size="down",  # never upscale; copy when smaller
        access="sequential",
    )
    return _normalize_vips_image(thumb)


def _vips_thumbnail_from_vips(
    vips_img: "pyvips.Image", max_size: tuple[int, int]
) -> "pyvips.Image":
    """
    Create a smaller thumbnail from an existing pyvips image, fitting within max_size.
    """
    width, height = max_size
    thumb = vips_img.thumbnail_image(width, height=height, size="down")
    return _normalize_vips_image(thumb)


def _atomic_write(dest_path: Path, write_fn: Callable[[Path], None]) -> None:
    """Write to tmp path then atomically rename. Clean up tmp on failure."""
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    try:
        write_fn(tmp_path)
        tmp_path.replace(dest_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def _vips_write_jpeg(vips_img: "pyvips.Image", path: Path, quality: int = 85) -> None:
    """Write a pyvips image as JPEG to path with the given quality (atomic)."""
    def _do_write(p: Path) -> None:
        vips_img.write_to_file(str(p), Q=quality)

    _atomic_write(path, _do_write)


def _vips_write_webp(vips_img: "pyvips.Image", path: Path, quality: int = 85) -> None:
    """Write a pyvips image as WebP to path with the given quality (atomic)."""
    def _do_write(p: Path) -> None:
        vips_img.write_to_file(str(p), Q=quality)

    _atomic_write(path, _do_write)


class LocalMediaStore:
    """
    Stores thumbnails and proxies under data_dir, sharded by library_slug and asset_id.
    Directory layout: data_dir / library_slug / category / (asset_id % 1000) / {asset_id}.jpg
    (proxies use .webp instead of .jpg).
    """

    PROXY_EXTENSION = ".webp"
    THUMBNAIL_SIZE: tuple[int, int] = (320, 320)
    PROXY_SIZE: tuple[int, int] = (768, 768)

    def __init__(self) -> None:
        self.data_dir = Path(get_config().data_dir)

    def load_source_image(self, source_path: Path | str, *, use_previews: bool = True) -> Image.Image:
        """Open image via Pillow, fix EXIF orientation, return RGB `PIL.Image`.

        This API is kept for compatibility and ad-hoc tooling that truly needs an
        in-memory `PIL.Image`. For high-throughput proxy/thumbnail generation in the
        worker pipeline, prefer the pyvips-based file APIs such as
        `generate_proxy_and_thumbnail_from_source`, which avoid materializing a full
        frame in Python space.

        Uses Pillow first for common formats. For RAW/DNG/unsupported formats, may use a
        fast-path libvips preview when use_previews is True, otherwise falls back to a full
        pyvips decode. For RAW_EXTENSIONS we never use Pillow; we try rawpy preview then pyvips.
        """
        path = Path(source_path)
        suffix_lower = path.suffix.lower()

        # RAW-only path: never use Pillow for these extensions (avoids full-frame decode/memory blowup).
        if suffix_lower in RAW_EXTENSIONS:
            global _rawpy_fallback_warned
            if use_previews:
                img = _load_raw_preview_rawpy(path)
                if img is not None:
                    return img
                if not rawpy_available() and not _rawpy_fallback_warned:
                    _rawpy_fallback_warned = True
                    _log.warning(
                        "rawpy unavailable or no embedded preview for RAW; falling back to libvips. "
                        "Memory use may be higher. Install rawpy (and LibRaw) for optimal RAW handling."
                    )
                try:
                    import pyvips  # type: ignore[import]
                    thumb_vips = pyvips.Image.thumbnail(
                        str(path),
                        1280,
                        height=1280,
                        size="down",
                        access="sequential",
                    )
                    thumb_vips = _normalize_vips_image(thumb_vips)
                    arr = thumb_vips.numpy()
                    if arr.ndim == 2:
                        return Image.fromarray(arr).convert("RGB")
                    return Image.fromarray(arr, "RGB")
                except Exception:
                    pass
            return _load_image_via_pyvips(path)

        # Non-RAW: Pillow first, then pyvips on failure.
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
                        # keep enough detail while keeping memory bounded, never upscaling.
                        thumb_vips = pyvips.Image.thumbnail(
                            str(path),
                            1280,
                            height=1280,
                            size="down",
                            access="sequential",
                        )
                        thumb_vips = _normalize_vips_image(thumb_vips)
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

    @staticmethod
    def _fit_within_box_no_upscale(image: Image.Image, max_size: tuple[int, int]) -> Image.Image:
        """Return a copy of image resized to fit within max_size, never upscaling.

        If the image already fits within the target box (both dimensions <= max_size),
        this returns a same-size copy. Otherwise it downsamples with aspect ratio
        preserved so that max(width, height) == max(max_size).
        """
        max_w, max_h = max_size
        if image.width <= max_w and image.height <= max_h:
            return image.copy()

        # Pillow's thumbnail() modifies in-place; operate on a copy.
        resized = image.copy()
        resized.thumbnail(max_size)
        return resized

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
        """Create thumbnail (max 320x320) and save as JPEG quality 85.

        The thumbnail is never upscaled: if the input image is already smaller than
        the target box, it is saved at its original resolution.
        """
        path = self._get_shard_path(library_slug, asset_id, "thumbnails", create_dirs=True)
        thumb = self._fit_within_box_no_upscale(image, self.THUMBNAIL_SIZE)

        def _do_write(p: Path) -> None:
            thumb.save(p, "JPEG", quality=85)

        _atomic_write(path, _do_write)

    def save_proxy(self, library_slug: str, asset_id: int, image: Image.Image) -> Image.Image:
        """Create proxy (max 768x768) and save as WebP (quality 85).

        The proxy is never upscaled: if the input image is already smaller than
        the target box, it is saved at its original resolution.

        Returns the in-memory proxy Image so callers that also need a thumbnail
        can reuse it instead of re-downsampling the original.
        """
        path = self._get_proxy_path(library_slug, asset_id, create_dirs=True)
        proxy = self._fit_within_box_no_upscale(image, self.PROXY_SIZE)

        def _do_write(p: Path) -> None:
            proxy.save(p, "WEBP", quality=85)

        _atomic_write(path, _do_write)
        return proxy

    def save_proxy_and_thumbnail(self, library_slug: str, asset_id: int, image: Image.Image) -> None:
        """Save cascaded proxy and thumbnail derived from that proxy.

        Source (full-res or pyvips preview) -> proxy (<=768x768) -> thumbnail (<=320x320).

        Small images are never upscaled:
        - If the source is smaller than the proxy target, the proxy uses source resolution.
        - If the proxy is smaller than the thumbnail target, the thumbnail uses proxy resolution.
        """
        proxy = self.save_proxy(library_slug, asset_id, image)
        self.save_thumbnail(library_slug, asset_id, proxy)

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
        """Return True if the thumbnail file exists and is non-empty. Used by proxy --repair for videos."""
        path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        return file_non_empty(path)

    def get_proxy_path(self, library_slug: str, asset_id: int) -> Path:
        """Return path to proxy (WebP); raise FileNotFoundError if it does not exist."""
        path = self._get_proxy_path(library_slug, asset_id)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def proxy_and_thumbnail_exist(self, library_slug: str, asset_id: int) -> bool:
        """Return True if both proxy (WebP) and thumbnail files exist and are non-empty. Used by proxy --repair."""
        proxy_path = self._get_proxy_path(library_slug, asset_id)
        thumb_path = self._get_shard_path(library_slug, asset_id, "thumbnails")
        return file_non_empty(proxy_path) and file_non_empty(thumb_path)

    def generate_proxy_and_thumbnail_from_source(
        self,
        library_slug: str,
        asset_id: int,
        source_path: Path | str,
        *,
        use_previews: bool = True,
    ) -> None:
        """
        Generate proxy (WebP) and thumbnail (JPEG) for an image asset from the source path.

        This is a pyvips-first pipeline:
        - For the common case, it uses libvips shrink-on-load thumbnailing with sequential
          access to bound memory and never upscale.
        - On failure (or when previews are disabled), it falls back to the existing
          Pillow/pyvips load_source_image + save_proxy_and_thumbnail path.
        """
        path = Path(source_path)
        suffix_lower = path.suffix.lower()

        if use_previews:
            try:
                # Fast path: single read of the source into a pyvips thumbnail that fits
                # within the proxy box, then derive the smaller thumbnail from that.
                proxy_vips = _vips_thumbnail_from_file(path, self.PROXY_SIZE)

                proxy_path = self._get_proxy_path(library_slug, asset_id, create_dirs=True)
                thumb_path = self._get_shard_path(
                    library_slug, asset_id, "thumbnails", create_dirs=True
                )

                _vips_write_webp(proxy_vips, proxy_path)

                thumb_vips = _vips_thumbnail_from_vips(proxy_vips, self.THUMBNAIL_SIZE)
                _vips_write_jpeg(thumb_vips, thumb_path)
                return
            except Exception:
                # Fall back to the slower, but battle-tested, Pillow-based pipeline below.
                pass

        # RAW with use_previews: try rawpy embedded preview before load_source_image.
        if use_previews and suffix_lower in RAW_EXTENSIONS:
            rawpy_img = _load_raw_preview_rawpy(path)
            if rawpy_img is not None:
                self.save_proxy_and_thumbnail(library_slug, asset_id, rawpy_img)
                return
            global _rawpy_fallback_warned
            if not rawpy_available() and not _rawpy_fallback_warned:
                _rawpy_fallback_warned = True
                _log.warning(
                    "rawpy unavailable or no embedded preview for RAW; falling back to libvips. "
                    "Memory use may be higher. Install rawpy (and LibRaw) for optimal RAW handling."
                )

        # Slow path: use the existing load_source_image semantics (Pillow first for non-RAW, then full
        # pyvips decode where needed, honouring use_previews for RAW/DNG) and reuse the
        # Pillow-based resize/encode helpers.
        image = self.load_source_image(path, use_previews=use_previews)
        self.save_proxy_and_thumbnail(library_slug, asset_id, image)
