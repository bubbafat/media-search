"""Integration tests for video scene indexing: persistence and resume (testcontainers Postgres)."""

from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.ai.schema import ModelCard, VisualAnalysis
from src.ai.vision_base import BaseVisionAnalyzer
from src.models.entities import AssetType, Library, SceneKeepReason, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.video_scene_repo import (
    VideoActiveState,
    VideoSceneRepository,
    VideoSceneRow,
)
from src.video.high_res_extractor import SOI, EOI
from src.video.indexing import run_video_scene_indexing
from src.video.scene_segmenter import SceneResult


class _SequentialDescriptionAnalyzer(BaseVisionAnalyzer):
    """Returns descriptions from a list in order (for dedup testing)."""

    def __init__(self, descriptions: list[str]):
        self._descriptions = list(descriptions)
        self._index = 0

    def get_model_card(self) -> ModelCard:
        return ModelCard(name="test-sequential", version="1.0")

    def analyze_image(self, image_path: Path) -> VisualAnalysis:
        desc = self._descriptions[self._index] if self._index < len(self._descriptions) else ""
        self._index += 1
        return VisualAnalysis(description=desc, tags=[], ocr_text=None)


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return (AssetRepository, VideoSceneRepository)."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return AssetRepository(session_factory), VideoSceneRepository(session_factory)


def _ensure_library_and_asset(session_factory, slug: str) -> int:
    """Insert a library and one video asset. Return asset id."""
    session = session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Video Lib",
                absolute_path="/tmp/vid-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo = AssetRepository(session_factory)
    asset_repo.upsert_asset(slug, "video.mp4", AssetType.video, 1000.0, 5000)
    session = session_factory()
    try:
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :slug AND rel_path = 'video.mp4'"),
            {"slug": slug},
        ).fetchone()
        assert row is not None
        return row[0]
    finally:
        session.close()


def test_indexing_persists_scenes_and_clears_state_on_eof(engine, _session_factory, tmp_path):
    """Full run: mock segmenter yields two scenes then EOF; assert 2 rows in video_scenes and no active_state."""
    asset_repo, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-full")
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size

    scene1 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=1.0,
        scene_start_pts=0.0,
        scene_end_pts=5.0,
        keep_reason=SceneKeepReason.phash,
        sharpness_score=10.0,
    )
    state1 = ("abc123", 5.0, 5.0, -1.0)
    scene2 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=8.0,
        scene_start_pts=5.0,
        scene_end_pts=12.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=20.0,
    )
    mock_yields = [(scene1, state1), (scene2, None)]

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    with patch("src.video.indexing.VideoScanner") as MockScanner:
        MockScanner.return_value.out_width = w
        MockScanner.return_value.out_height = h
        with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
            MockSegmenter.return_value.iter_scenes.return_value = mock_yields
            run_video_scene_indexing(asset_id, video_path, "vid-int-full", video_repo)

    assert video_repo.get_max_end_ts(asset_id) == 12.0
    assert video_repo.get_active_state(asset_id) is None
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).scalar()
        assert count == 2
    finally:
        session.close()


def test_indexing_resume_continues_and_clears_state(engine, _session_factory, tmp_path):
    """Resume: pre-seed one scene and active_state; mock segmenter yields one more scene (EOF). Assert 2 scenes and no active_state."""
    asset_repo, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-resume")
    # Pre-insert one scene and active state (simulate crash after first scene)
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description=None,
            metadata=None,
            sharpness_score=10.0,
            rep_frame_path=str(tmp_path / "0_5.jpg"),
            keep_reason="phash",
        ),
        VideoActiveState(
            anchor_phash="resumed_hash",
            scene_start_ts=5.0,
            current_best_pts=7.0,
            current_best_sharpness=5.0,
        ),
    )
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size

    scene_final = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=8.0,
        scene_start_pts=5.0,
        scene_end_pts=12.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=20.0,
    )
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    with patch("src.video.indexing.VideoScanner") as MockScanner:
        MockScanner.return_value.out_width = w
        MockScanner.return_value.out_height = h
        with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
            MockSegmenter.return_value.iter_scenes.return_value = [(scene_final, None)]
            run_video_scene_indexing(asset_id, video_path, "vid-int-resume", video_repo)

    assert video_repo.get_max_end_ts(asset_id) == 12.0
    assert video_repo.get_active_state(asset_id) is None
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).scalar()
        assert count == 2
    finally:
        session.close()


def test_indexing_vision_dedup_flags_semantic_duplicate(engine, _session_factory, tmp_path):
    """With vision_analyzer, two scenes with similar descriptions get semantic_duplicate on the second."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-int-dedup")
    w, h = 480, 270
    frame_size = w * h * 3
    fake_frame = b"\x80" * frame_size
    # Descriptions chosen so token_set_ratio > 85
    desc1 = "A man standing in a room"
    desc2 = "A man standing in a room with a window"
    analyzer = _SequentialDescriptionAnalyzer([desc1, desc2])

    scene1 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=1.0,
        scene_start_pts=0.0,
        scene_end_pts=5.0,
        keep_reason=SceneKeepReason.phash,
        sharpness_score=10.0,
    )
    state1 = ("abc123", 5.0, 5.0, -1.0)
    scene2 = SceneResult(
        best_frame_bytes=fake_frame,
        best_pts=8.0,
        scene_start_pts=5.0,
        scene_end_pts=12.0,
        keep_reason=SceneKeepReason.forced,
        sharpness_score=20.0,
    )
    mock_yields = [(scene1, state1), (scene2, None)]

    minimal_jpeg = SOI + b"x" + EOI
    showinfo_line = "showinfo pts_time:1.0 n:30"

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"fake")
    with patch("src.video.indexing.VideoScanner") as MockScanner:
        MockScanner.return_value.out_width = w
        MockScanner.return_value.out_height = h
        with patch("src.video.indexing.SceneSegmenter") as MockSegmenter:
            MockSegmenter.return_value.iter_scenes.return_value = mock_yields
            with patch("src.video.indexing.extract_high_res_frame") as mock_extract:
                mock_extract.return_value = (minimal_jpeg, showinfo_line)
                run_video_scene_indexing(
                    asset_id, video_path, "vid-int-dedup", video_repo, vision_analyzer=analyzer
                )

    session = _session_factory()
    try:
        rows = session.execute(
            text(
                "SELECT description, metadata FROM video_scenes WHERE asset_id = :aid ORDER BY start_ts"
            ),
            {"aid": asset_id},
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == desc1
        assert rows[1][0] == desc2
        meta2 = rows[1][1]
        assert meta2 is not None
        assert meta2.get("semantic_duplicate") is True
    finally:
        session.close()
