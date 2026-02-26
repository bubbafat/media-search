"""Tests for video preview.webp generation."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from PIL import Image

from src.repository.video_scene_repo import VideoSceneListItem
from src.video.preview import PREVIEW_LONG_SIDE, build_preview_webp

pytestmark = [pytest.mark.fast]


def _make_jpeg(path: Path, size: tuple[int, int] = (100, 100)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", size, color="red")
    img.save(path, "JPEG", quality=85)


def test_build_preview_webp_produces_valid_file(tmp_path):
    """build_preview_webp with scene JPEGs produces preview.webp in the scene folder."""
    scenes_dir = tmp_path / "video_scenes" / "lib" / "42"
    jpeg1 = scenes_dir / "0.000_3.000.jpg"
    jpeg2 = scenes_dir / "3.000_6.000.jpg"
    _make_jpeg(jpeg1)
    _make_jpeg(jpeg2)

    mock_repo = MagicMock()
    mock_repo.list_scenes.return_value = [
        VideoSceneListItem(
            id=1,
            start_ts=0.0,
            end_ts=3.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/lib/42/0.000_3.000.jpg",
            keep_reason="phash",
        ),
        VideoSceneListItem(
            id=2,
            start_ts=3.0,
            end_ts=6.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/lib/42/3.000_6.000.jpg",
            keep_reason="temporal",
        ),
    ]

    result = build_preview_webp(42, "lib", mock_repo, tmp_path)

    assert result is not None
    assert result == scenes_dir / "preview.webp"
    assert result.exists()
    with Image.open(result) as im:
        im.load()
        assert im.format == "WEBP"
        # Animated WebP has n_frames > 1 when multiple frames
        assert getattr(im, "n_frames", 1) >= 1


def test_build_preview_webp_returns_none_when_no_scenes(tmp_path):
    """build_preview_webp returns None and writes nothing when list_scenes is empty."""
    mock_repo = MagicMock()
    mock_repo.list_scenes.return_value = []

    result = build_preview_webp(99, "lib", mock_repo, tmp_path)

    assert result is None
    assert not (tmp_path / "video_scenes" / "lib" / "99" / "preview.webp").exists()


def test_build_preview_webp_returns_none_when_no_loadable_frames(tmp_path):
    """build_preview_webp returns None when all rep_frame_paths are missing."""
    mock_repo = MagicMock()
    mock_repo.list_scenes.return_value = [
        VideoSceneListItem(
            id=1,
            start_ts=0.0,
            end_ts=3.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/lib/1/missing.jpg",
            keep_reason="phash",
        ),
    ]

    result = build_preview_webp(1, "lib", mock_repo, tmp_path)

    assert result is None
    assert not (tmp_path / "video_scenes" / "lib" / "1" / "preview.webp").exists()


def test_build_preview_webp_single_frame(tmp_path):
    """build_preview_webp with one scene produces valid single-frame WebP."""
    jpeg1 = tmp_path / "video_scenes" / "lib" / "1" / "only.jpg"
    _make_jpeg(jpeg1)
    mock_repo = MagicMock()
    mock_repo.list_scenes.return_value = [
        VideoSceneListItem(
            id=1,
            start_ts=0.0,
            end_ts=5.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/lib/1/only.jpg",
            keep_reason="forced",
        ),
    ]

    result = build_preview_webp(1, "lib", mock_repo, tmp_path)

    assert result is not None
    assert result.exists()
    with Image.open(result) as im:
        im.load()
        assert im.format == "WEBP"


def test_build_preview_webp_preserves_aspect_ratio_long_side_320(tmp_path):
    """Preview uses 320px as long side and preserves aspect ratio (no square padding)."""
    # Landscape: 400×200 -> 320×160
    landscape_jpeg = tmp_path / "video_scenes" / "lib" / "1" / "landscape.jpg"
    _make_jpeg(landscape_jpeg, size=(400, 200))
    mock_repo_landscape = MagicMock()
    mock_repo_landscape.list_scenes.return_value = [
        VideoSceneListItem(
            id=1,
            start_ts=0.0,
            end_ts=3.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/lib/1/landscape.jpg",
            keep_reason="phash",
        ),
    ]
    result_landscape = build_preview_webp(1, "lib", mock_repo_landscape, tmp_path)
    assert result_landscape is not None
    with Image.open(result_landscape) as im:
        im.load()
        w, h = im.size
        assert max(w, h) == PREVIEW_LONG_SIDE
        assert (w, h) == (320, 160)

    # Portrait: 200×400 -> 160×320
    portrait_jpeg = tmp_path / "video_scenes" / "lib" / "2" / "portrait.jpg"
    _make_jpeg(portrait_jpeg, size=(200, 400))
    mock_repo_portrait = MagicMock()
    mock_repo_portrait.list_scenes.return_value = [
        VideoSceneListItem(
            id=1,
            start_ts=0.0,
            end_ts=3.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/lib/2/portrait.jpg",
            keep_reason="phash",
        ),
    ]
    result_portrait = build_preview_webp(2, "lib", mock_repo_portrait, tmp_path)
    assert result_portrait is not None
    with Image.open(result_portrait) as im:
        im.load()
        w, h = im.size
        assert max(w, h) == PREVIEW_LONG_SIDE
        assert (w, h) == (160, 320)
