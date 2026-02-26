"""Tests for video preview.webp generation."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from PIL import Image

from src.repository.video_scene_repo import VideoSceneListItem
from src.video.preview import build_preview_webp

pytestmark = [pytest.mark.fast]


def _make_jpeg(path: Path, size: tuple[int, int] = (100, 100)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", size, color="red")
    img.save(path, "JPEG", quality=85)


def test_build_preview_webp_produces_valid_file(tmp_path):
    """build_preview_webp with scene JPEGs produces preview.webp in the scene folder."""
    jpeg1 = tmp_path / "0.000_3.000.jpg"
    jpeg2 = tmp_path / "3.000_6.000.jpg"
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
            rep_frame_path=str(jpeg1),
            keep_reason="phash",
        ),
        VideoSceneListItem(
            id=2,
            start_ts=3.0,
            end_ts=6.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path=str(jpeg2),
            keep_reason="temporal",
        ),
    ]

    scenes_dir = tmp_path / "video_scenes" / "lib" / "42"
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
            rep_frame_path=str(tmp_path / "missing.jpg"),
            keep_reason="phash",
        ),
    ]

    result = build_preview_webp(1, "lib", mock_repo, tmp_path)

    assert result is None
    assert not (tmp_path / "video_scenes" / "lib" / "1" / "preview.webp").exists()


def test_build_preview_webp_single_frame(tmp_path):
    """build_preview_webp with one scene produces valid single-frame WebP."""
    jpeg1 = tmp_path / "only.jpg"
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
            rep_frame_path=str(jpeg1),
            keep_reason="forced",
        ),
    ]

    result = build_preview_webp(1, "lib", mock_repo, tmp_path)

    assert result is not None
    assert result.exists()
    with Image.open(result) as im:
        im.load()
        assert im.format == "WEBP"
