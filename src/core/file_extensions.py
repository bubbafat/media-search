"""Single source of truth for supported file extensions (scanner, proxy, repair)."""

# Video extensions (unchanged)
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov"}

# Image extensions: common raster + major camera RAW (last ~10 years) + TIFF/DNG
_COMMON_IMAGE = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
_CANON_RAW = {".cr2", ".cr3", ".crw"}
_NIKON_RAW = {".nef", ".nrw"}
_SONY_RAW = {".arw", ".sr2", ".srf"}
_FUJI_RAW = {".raf"}
_OLYMPUS_RAW = {".orf"}
_PANASONIC_RAW = {".rw2", ".raw"}
_LEICA_RAW = {".rwl"}
_UNIVERSAL_IMAGE = {".dng", ".tif", ".tiff"}

IMAGE_EXTENSIONS = (
    _COMMON_IMAGE
    | _CANON_RAW
    | _NIKON_RAW
    | _SONY_RAW
    | _FUJI_RAW
    | _OLYMPUS_RAW
    | _PANASONIC_RAW
    | _LEICA_RAW
    | _UNIVERSAL_IMAGE
)

SUPPORTED_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS

# List form with leading dots for claim_asset_by_status and regex building
IMAGE_EXTENSIONS_LIST = sorted(IMAGE_EXTENSIONS)

# Suffixes without leading dot (for regex e.g. repair pattern)
IMAGE_EXTENSION_SUFFIXES = [ext.lstrip(".") for ext in IMAGE_EXTENSIONS_LIST]
