"""Unit tests for metadata normalization helpers (no DB, no I/O)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict

import pytest

from src.metadata.normalization import normalize_media_metadata
from src.models.entities import Asset, AssetType

pytestmark = [pytest.mark.fast]


def _make_asset(*, asset_type: AssetType, mtime: float = 0.0) -> Asset:
    return Asset(
        library_id="lib",
        rel_path="file.ext",
        type=asset_type,
        mtime=mtime,
        size=0,
    )


def test_capture_ts_prefers_exif_datetimeoriginal_with_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    """DateTimeOriginal with OffsetTimeOriginal is used for capture_ts with 'exif' source."""
    asset = _make_asset(asset_type=AssetType.image, mtime=0.0)
    raw_exif: Dict[str, Any] = {
        "DateTimeOriginal": "2025:01:02 03:04:05",
        "OffsetTimeOriginal": "+02:00",
    }

    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["capture_ts_source"] == "exif"
    # Local time 03:04:05 at +02:00 should correspond to 01:04:05 UTC.
    dt = datetime(2025, 1, 2, 1, 4, 5, tzinfo=timezone.utc)
    assert abs(meta["capture_ts"] - dt.timestamp()) < 5.0  # type: ignore[operator]


def test_capture_ts_falls_back_to_asset_mtime_when_no_exif() -> None:
    asset = _make_asset(asset_type=AssetType.image, mtime=1234.5)
    raw_exif: Dict[str, Any] = {}
    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["capture_ts"] == pytest.approx(1234.5)
    assert meta["capture_ts_source"] == "asset_mtime"


def test_camera_make_and_model_stripping_and_fallbacks() -> None:
    asset = _make_asset(asset_type=AssetType.image)
    raw_exif: Dict[str, Any] = {
        "Make": "  Canon ",
        "Model": "   ",
        "CameraModelName": " R5 ",
    }
    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["camera_make"] == "Canon"
    assert meta["camera_model"] == "R5"


def test_color_space_detection_branches() -> None:
    asset = _make_asset(asset_type=AssetType.image)

    # Display P3 via ICC profile text.
    meta = normalize_media_metadata({"ProfileDescription": "Display P3 profile"}, asset=asset)
    assert meta["color_space"] == "DisplayP3"

    # Numeric ColorSpace mapping.
    meta = normalize_media_metadata({"ColorSpace": 1}, asset=asset)
    assert meta["color_space"] == "sRGB"
    meta = normalize_media_metadata({"ColorSpace": 2}, asset=asset)
    assert meta["color_space"] == "AdobeRGB"

    # Video Rec.709 / Rec.2020.
    video_asset = _make_asset(asset_type=AssetType.video)
    meta = normalize_media_metadata({"VideoColorPrimaries": "BT.709"}, asset=video_asset)
    assert meta["color_space"] == "Rec709"
    meta = normalize_media_metadata({"ColorPrimaries": "BT.2020"}, asset=video_asset)
    assert meta["color_space"] == "Rec2020"


def test_generation_hint_log_curve_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Log-style gamma in EXIF implies original for video."""
    asset = _make_asset(asset_type=AssetType.video)
    raw_exif: Dict[str, Any] = {
        "Gamma": "S-Log3",
        "VideoBitrate": 1.0,
        "VideoFrameRate": 1.0,
        "ExifImageWidth": 1920,
        "ExifImageHeight": 1080,
    }
    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["generation_hint"] == "original"


def test_generation_hint_bpppf_thresholds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bits-per-pixel-per-frame thresholding drives original/proxy/unknown."""
    from src.metadata import normalization as norm

    # Patch config thresholds via get_config used in normalization.
    monkeypatch.setattr(
        norm,
        "get_config",
        lambda: SimpleNamespace(
            generation_hint_original_bpppf_threshold=0.08,
            generation_hint_proxy_bpppf_threshold=0.03,
        ),
    )

    asset = _make_asset(asset_type=AssetType.video)
    base_exif: Dict[str, Any] = {
        "VideoFrameRate": 25,
        "ExifImageWidth": 1920,
        "ExifImageHeight": 1080,
    }

    # Above original threshold.
    raw_exif = dict(base_exif)
    raw_exif["VideoBitrate"] = 1920 * 1080 * 25 * 0.1
    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["generation_hint"] == "original"

    # Below proxy threshold.
    raw_exif = dict(base_exif)
    raw_exif["VideoBitrate"] = 1920 * 1080 * 25 * 0.01
    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["generation_hint"] == "proxy"


def test_gps_and_reverse_geocode(monkeypatch: pytest.MonkeyPatch) -> None:
    """GPS lat/lon are passed through and geocoded to country/region/city."""
    asset = _make_asset(asset_type=AssetType.image)
    raw_exif: Dict[str, Any] = {"GPSLatitude": 40.0, "GPSLongitude": -74.0}

    from src.metadata import normalization as norm

    def _fake_search(coords):
        assert coords == (40.0, -74.0)
        return [
            {
                "cc": "US",
                "admin1": "New York",
                "name": "New York City",
            }
        ]

    monkeypatch.setattr(norm.rg, "search", _fake_search)

    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["gps_lat"] == pytest.approx(40.0)
    assert meta["gps_lon"] == pytest.approx(-74.0)
    assert meta["country"] == "US"
    assert meta["region"] == "New York"
    assert meta["city"] == "New York City"


def test_resolution_prefers_exifimagewidth() -> None:
    asset = _make_asset(asset_type=AssetType.image)
    raw_exif: Dict[str, Any] = {
        "ExifImageWidth": 4000,
        "ImageWidth": 3900,
        "ExifImageHeight": 3000,
        "ImageHeight": 2900,
    }
    meta = normalize_media_metadata(raw_exif, asset=asset)
    assert meta["resolution_w"] == 4000
    assert meta["resolution_h"] == 3000


def test_duration_and_frame_rate_parsing() -> None:
    video_asset = _make_asset(asset_type=AssetType.video)
    raw_exif: Dict[str, Any] = {
        "Duration": "0:01:30",
        "VideoFrameRate": "30000/1001",
    }
    meta = normalize_media_metadata(raw_exif, asset=video_asset)
    assert meta["duration_sec"] == pytest.approx(90.0)
    assert meta["frame_rate"] == pytest.approx(29.97, rel=1e-3)

    image_asset = _make_asset(asset_type=AssetType.image)
    meta_img = normalize_media_metadata(raw_exif, asset=image_asset)
    assert meta_img["duration_sec"] is None
    assert meta_img["frame_rate"] is None

