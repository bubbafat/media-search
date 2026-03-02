from __future__ import annotations

"""Thin wrapper around pyexiftool for reading EXIF metadata.

Responsibility:
- Manage a (lazily started) long-lived exiftool process per worker process.
- Expose a simple `read_metadata(path: Path) -> dict[str, Any]` API.
- Apply basic filtering to drop large vendor-specific maker note fields.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Set

import exiftool  # type: ignore[import-not-found]

from src.core.config import get_config

_log = logging.getLogger(__name__)

# Keys that are often extremely large or vendor-specific and safe to drop.
VENDOR_FIELD_DENYLIST: Set[str] = set()
MAKERNOTE_PREFIXES: tuple[str, ...] = ("MakerNote", "MakerNotes")

_EXIFTOOL_INSTANCE: exiftool.ExifTool | None = None


class ExifToolError(RuntimeError):
    """Raised when exiftool fails for a given file."""


def _get_exiftool() -> exiftool.ExifTool:
    """
    Return a process-wide ExifTool instance, starting it on first use.

    This keeps a long-lived exiftool process per worker process so repeated
    metadata reads avoid process startup overhead.
    """
    global _EXIFTOOL_INSTANCE
    if _EXIFTOOL_INSTANCE is not None:
        return _EXIFTOOL_INSTANCE

    cfg = get_config()
    executable = getattr(cfg, "exiftool_path", "exiftool") or "exiftool"
    tool = exiftool.ExifTool(executable=executable)
    try:
        tool.start()
    except Exception as e:  # noqa: BLE001
        raise ExifToolError(f"Failed to start exiftool at '{executable}': {e}") from e
    _EXIFTOOL_INSTANCE = tool
    return tool


def _filter_keys(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Drop maker-note and vendor-specific keys to keep raw_exif size bounded.

    - Any key starting with MakerNote or MakerNotes is removed.
    - Any key explicitly listed in VENDOR_FIELD_DENYLIST is removed.
    """
    if not data:
        return {}
    deny: Set[str] = {k.lower() for k in VENDOR_FIELD_DENYLIST}
    result: Dict[str, Any] = {}
    for k, v in data.items():
        if any(k.startswith(prefix) for prefix in MAKERNOTE_PREFIXES):
            continue
        if k.lower() in deny:
            continue
        result[k] = v
    return result


def set_vendor_field_denylist(fields: Iterable[str]) -> None:
    """Override the vendor field denylist (primarily for tests or tuning)."""
    global VENDOR_FIELD_DENYLIST
    VENDOR_FIELD_DENYLIST = set(fields)


def read_metadata(path: Path) -> dict[str, Any]:
    """
    Read EXIF/metadata for a single file using exiftool -json -n.

    Returns the first parsed dict for the file with maker notes stripped.
    Raises ExifToolError on any failure, including when exiftool writes to stderr.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Source file does not exist for EXIF read: {path}")

    tool = _get_exiftool()
    try:
        # Newer pyexiftool exposes execute_json; fall back to get_metadata_batch otherwise.
        if hasattr(tool, "execute_json"):
            raw_list = tool.execute_json("-json", "-n", str(path))  # type: ignore[attr-defined]
        else:
            raw_list = tool.get_metadata_batch([str(path)])
    except Exception as e:  # noqa: BLE001
        _log.error("exiftool failed for %s: %s", path, e, exc_info=True)
        raise ExifToolError(f"exiftool failed for {path}: {e}") from e

    if not raw_list:
        return {}
    if not isinstance(raw_list, list):
        raise ExifToolError(f"Unexpected exiftool output type for {path}: {type(raw_list)!r}")

    # exiftool normally returns one dict per input file; we only care about the first.
    first = raw_list[0]
    if not isinstance(first, dict):
        raise ExifToolError(
            f"Unexpected exiftool JSON payload for {path}: expected dict, got {type(first)!r}"
        )

    # Drop exiftool's own bookkeeping key when present.
    first.pop("SourceFile", None)
    return _filter_keys(first)


def shutdown_exiftool() -> None:
    """Terminate the global exiftool process, if started. Primarily for tests."""
    global _EXIFTOOL_INSTANCE
    if _EXIFTOOL_INSTANCE is None:
        return
    try:
        _EXIFTOOL_INSTANCE.terminate()
    except Exception:  # noqa: BLE001
        _log.warning("Failed to terminate exiftool cleanly", exc_info=True)
    finally:
        _EXIFTOOL_INSTANCE = None

