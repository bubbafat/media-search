from __future__ import annotations

"""Normalization of raw EXIF into media_metadata.

Pure Python helpers only: no database access. Configuration is read via
`get_config()` for thresholds but there is no I/O beyond config.
"""

import math
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

import reverse_geocoder as rg  # type: ignore[import-not-found]

from src.core.config import get_config
from src.models.entities import Asset, AssetType


def _parse_exif_datetime(value: Any) -> datetime | None:
    """Parse EXIF-style datetime string 'YYYY:MM:DD HH:MM:SS'."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None


def _compute_capture_ts(raw_exif: dict[str, Any], asset: Asset) -> tuple[float | None, str | None]:
    """
    Compute capture_ts (epoch seconds) and capture_ts_source.

    Preference order:
    - DateTimeOriginal (with optional OffsetTimeOriginal)
    - MediaCreateDate / CreateDate / TrackCreateDate
    - asset.mtime
    """
    offset_raw = raw_exif.get("OffsetTimeOriginal")
    offset = None
    if isinstance(offset_raw, str) and offset_raw:
        sign = 1 if offset_raw.strip().startswith("+") else -1
        try:
            hh, mm = offset_raw.strip().lstrip("+-").split(":")
            delta_minutes = int(hh) * 60 + int(mm)
            offset = timezone(sign * timedelta(minutes=delta_minutes))
        except Exception:
            offset = None

    def _from_keys(keys: list[str]) -> tuple[float | None, str | None]:
        for k in keys:
            dt = _parse_exif_datetime(raw_exif.get(k))
            if dt is None:
                continue
            if offset is not None:
                dt = dt.replace(tzinfo=offset)
                source = "exif"
            else:
                dt = dt.replace(tzinfo=timezone.utc)
                source = "exif_naive"
            return dt.timestamp(), source
        return None, None

    ts, source = _from_keys(
        ["DateTimeOriginal", "MediaCreateDate", "CreateDate", "TrackCreateDate"]
    )
    if ts is not None:
        return ts, source

    # Fallback to asset.mtime (already Unix timestamp float)
    try:
        ts = float(asset.mtime)
    except Exception:
        return None, None
    return ts, "asset_mtime"


def _normalize_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    s = value.strip()
    return s or None


def _detect_color_space(raw_exif: dict[str, Any]) -> str:
    # 1. ICC profile string containing "p3"
    for k, v in raw_exif.items():
        if "profile" in k.lower() and isinstance(v, str):
            if "p3" in v.lower():
                return "DisplayP3"

    # 2. EXIF ColorSpace numeric
    cs_val = raw_exif.get("ColorSpace")
    num: int | None = None
    if isinstance(cs_val, (int, float)):
        num = int(cs_val)
    elif isinstance(cs_val, str):
        try:
            num = int(cs_val)
        except ValueError:
            num = None
    if num == 1:
        return "sRGB"
    if num == 2:
        return "AdobeRGB"

    # 3. Video color primaries / transfer function
    candidates = []
    for key in ("VideoColorPrimaries", "ColorPrimaries", "TransferFunction", "VideoTransferFunction"):
        val = raw_exif.get(key)
        if isinstance(val, str):
            candidates.append(val.upper())
    for c in candidates:
        if "BT.709" in c or "BT709" in c:
            return "Rec709"
        if "BT.2020" in c or "BT2020" in c:
            return "Rec2020"

    return "Unknown"


def _parse_frame_rate(raw_exif: dict[str, Any]) -> float | None:
    for key in ("VideoFrameRate", "AvgFrameRate", "FrameRate"):
        val = raw_exif.get(key)
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            s = val.strip()
            if not s:
                continue
            if "/" in s:
                num_s, den_s = s.split("/", 1)
                try:
                    num = float(num_s)
                    den = float(den_s)
                    if den > 0:
                        return num / den
                except ValueError:
                    continue
            else:
                try:
                    return float(s)
                except ValueError:
                    continue
    return None


def _parse_duration(raw_exif: dict[str, Any], is_video: bool) -> float | None:
    if not is_video:
        return None
    val = raw_exif.get("Duration")
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if ":" in s:
            parts = s.split(":")
            try:
                parts_f = [float(p) for p in parts]
            except ValueError:
                return None
            if len(parts_f) == 3:
                h, m, sec = parts_f
                return h * 3600 + m * 60 + sec
            if len(parts_f) == 2:
                m, sec = parts_f
                return m * 60 + sec
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _resolution(raw_exif: dict[str, Any]) -> tuple[int | None, int | None]:
    width_keys = ["ExifImageWidth", "ImageWidth", "SourceImageWidth"]
    height_keys = ["ExifImageHeight", "ImageHeight", "SourceImageHeight"]
    w = h = None
    for k in width_keys:
        val = raw_exif.get(k)
        try:
            if isinstance(val, (int, float)):
                w = int(val)
                break
            if isinstance(val, str) and val.strip():
                w = int(float(val.strip()))
                break
        except ValueError:
            continue
    for k in height_keys:
        val = raw_exif.get(k)
        try:
            if isinstance(val, (int, float)):
                h = int(val)
                break
            if isinstance(val, str) and val.strip():
                h = int(float(val.strip()))
                break
        except ValueError:
            continue
    return w, h


def _generation_hint(
    raw_exif: dict[str, Any],
    asset: Asset,
    frame_rate: float | None,
    width: int | None,
    height: int | None,
) -> str:
    if asset.type == AssetType.image:
        return "original"

    # Log-curve detection
    log_tokens = ("s-log", "slog", "v-log", "vlog", "c-log", "clog", "logc", "f-log", "flog", " log")
    for key, val in raw_exif.items():
        if not isinstance(val, str):
            continue
        lk = key.lower()
        if not any(t in lk for t in ("gamma", "profile", "curve", "picturestyle", "transfer")):
            continue
        lv = val.lower()
        if any(tok in lv for tok in log_tokens):
            return "original"

    # Bits-per-pixel-per-frame heuristic
    bitrate_val = raw_exif.get("VideoBitrate")
    bitrate: float | None = None
    if isinstance(bitrate_val, (int, float)):
        bitrate = float(bitrate_val)
    elif isinstance(bitrate_val, str):
        try:
            bitrate = float(bitrate_val.strip())
        except ValueError:
            bitrate = None

    cfg = get_config()
    orig_thr = getattr(cfg, "generation_hint_original_bpppf_threshold", 0.08)
    proxy_thr = getattr(cfg, "generation_hint_proxy_bpppf_threshold", 0.03)

    if bitrate and frame_rate and width and height and width > 0 and height > 0 and frame_rate > 0:
        denom = float(width) * float(height) * frame_rate
        if denom > 0:
            bpppf = bitrate / denom
            if bpppf >= orig_thr:
                return "original"
            if bpppf <= proxy_thr:
                # Additional codec name hints for proxies
                for key in ("VideoCodecName", "CompressorName", "CodecID"):
                    v = raw_exif.get(key)
                    if isinstance(v, str):
                        lv = v.lower()
                        if "proxy" in lv or " lt" in lv or lv.endswith("lt"):
                            return "proxy"
                return "proxy"

    return "unknown"


def _geo_fields(raw_exif: dict[str, Any]) -> tuple[float | None, float | None, str | None, str | None, str | None]:
    lat_raw = raw_exif.get("GPSLatitude")
    lon_raw = raw_exif.get("GPSLongitude")
    try:
        lat = float(lat_raw) if lat_raw is not None else None
        lon = float(lon_raw) if lon_raw is not None else None
    except (TypeError, ValueError):
        lat = lon = None
    if lat is None or lon is None or math.isnan(lat) or math.isnan(lon):
        return None, None, None, None, None

    country = region = city = None
    try:
        results = rg.search((lat, lon))  # type: ignore[call-arg]
        if results:
            rec = results[0]
            if isinstance(rec, dict):
                country = _normalize_str(rec.get("cc"))
                region = _normalize_str(rec.get("admin1"))
                city = _normalize_str(rec.get("name"))
    except Exception:
        country = region = city = None
    return lat, lon, country, region, city


def normalize_media_metadata(raw_exif: dict[str, Any], *, asset: Asset) -> dict[str, Any]:
    """
    Normalize raw exiftool output into media_metadata.

    All fields use .get() with None defaults; missing or unparsable values
    result in nulls in the returned dict.
    """
    capture_ts, capture_source = _compute_capture_ts(raw_exif, asset)
    camera_make = _normalize_str(raw_exif.get("Make"))
    camera_model = (
        _normalize_str(raw_exif.get("Model"))
        or _normalize_str(raw_exif.get("CameraModelName"))
        or _normalize_str(raw_exif.get("DeviceModelName"))
    )
    color_space = _detect_color_space(raw_exif)

    frame_rate = _parse_frame_rate(raw_exif) if asset.type == AssetType.video else None
    duration_sec = _parse_duration(raw_exif, asset.type == AssetType.video)
    width, height = _resolution(raw_exif)
    generation_hint = _generation_hint(raw_exif, asset, frame_rate, width, height)
    gps_lat, gps_lon, country, region, city = _geo_fields(raw_exif)

    return {
        "metadata_version": 1,
        "capture_ts": capture_ts,
        "capture_ts_source": capture_source,
        "camera_make": camera_make,
        "camera_model": camera_model,
        "color_space": color_space,
        "generation_hint": generation_hint,
        "gps_lat": gps_lat,
        "gps_lon": gps_lon,
        "country": country,
        "region": region,
        "city": city,
        "resolution_w": width,
        "resolution_h": height,
        "duration_sec": duration_sec,
        "frame_rate": frame_rate,
    }

