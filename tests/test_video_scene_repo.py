"""Tests for VideoSceneRepository (testcontainers Postgres): get_max_end_ts, get_active_state, save_scene_and_update_state."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.video_scene_repo import (
    VideoActiveState,
    VideoSceneListItem,
    VideoSceneRepository,
    VideoSceneRow,
)

pytestmark = [pytest.mark.slow]


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
    """Insert a library and one video asset. Return asset id. Caller must pass a unique slug per test."""
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


def test_list_scenes_returns_ordered_scenes(engine, _session_factory):
    """list_scenes returns all scenes for the asset ordered by start_ts with all fields."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-list")
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=5.0,
            end_ts=12.0,
            description="Second",
            metadata={"key": "value"},
            sharpness_score=20.0,
            rep_frame_path="/data/2.jpg",
            keep_reason="temporal",
        ),
        None,
    )
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description="First",
            metadata=None,
            sharpness_score=10.0,
            rep_frame_path="/data/1.jpg",
            keep_reason="phash",
        ),
        None,
    )
    # save_scene inserts in order; we inserted second then first, so id 1 = second scene, id 2 = first scene
    # list_scenes orders by start_ts so first scene (0-5) then second (5-12)
    scenes = video_repo.list_scenes(asset_id)
    assert len(scenes) == 2
    assert scenes[0].start_ts == 0.0
    assert scenes[0].end_ts == 5.0
    assert scenes[0].description == "First"
    assert scenes[0].metadata is None
    assert scenes[0].keep_reason == "phash"
    assert scenes[0].rep_frame_path == "/data/1.jpg"
    assert scenes[1].start_ts == 5.0
    assert scenes[1].end_ts == 12.0
    assert scenes[1].description == "Second"
    assert scenes[1].metadata == {"key": "value"}
    assert scenes[1].keep_reason == "temporal"


def test_list_scenes_empty_returns_empty_list(engine, _session_factory):
    """list_scenes when no scenes exist returns empty list."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-list-empty")
    assert video_repo.list_scenes(asset_id) == []


def test_clear_index_for_asset_removes_scenes_and_active_state(engine, _session_factory):
    """clear_index_for_asset deletes all video_scenes and video_active_state for the asset."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-clear")
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description="First",
            metadata=None,
            sharpness_score=10.0,
            rep_frame_path="/data/1.jpg",
            keep_reason="phash",
        ),
        VideoActiveState("abc", 0.0, 2.0, 10.0),
    )
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=5.0,
            end_ts=12.0,
            description="Second",
            metadata={},
            sharpness_score=20.0,
            rep_frame_path="/data/2.jpg",
            keep_reason="temporal",
        ),
        None,
    )
    assert len(video_repo.list_scenes(asset_id)) == 2
    video_repo.clear_index_for_asset(asset_id)
    assert video_repo.list_scenes(asset_id) == []
    assert video_repo.get_active_state(asset_id) is None
    assert video_repo.get_max_end_ts(asset_id) is None


def test_get_max_end_ts_empty_returns_none(engine, _session_factory):
    """get_max_end_ts when no scenes exist returns None."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-empty")
    assert video_repo.get_max_end_ts(asset_id) is None


def test_get_max_end_ts_returns_max(engine, _session_factory):
    """get_max_end_ts returns max(end_ts) after inserting scenes."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-max")
    scene = VideoSceneRow(
        start_ts=0.0,
        end_ts=10.5,
        description=None,
        metadata={"showinfo": {}},
        sharpness_score=100.0,
        rep_frame_path="/data/scenes/1.jpg",
        keep_reason="phash",
    )
    video_repo.save_scene_and_update_state(asset_id, scene, None)
    assert video_repo.get_max_end_ts(asset_id) == 10.5

    scene2 = VideoSceneRow(
        start_ts=10.5,
        end_ts=25.0,
        description=None,
        metadata=None,
        sharpness_score=80.0,
        rep_frame_path="/data/scenes/2.jpg",
        keep_reason="temporal",
    )
    video_repo.save_scene_and_update_state(asset_id, scene2, None)
    assert video_repo.get_max_end_ts(asset_id) == 25.0


def test_get_last_scene_description_empty_returns_none(engine, _session_factory):
    """get_last_scene_description when no scenes exist returns None."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-desc-empty")
    assert video_repo.get_last_scene_description(asset_id) is None


def test_get_last_scene_description_returns_most_recent(engine, _session_factory):
    """get_last_scene_description returns description of scene with max end_ts."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-desc")
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description="First scene",
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="/data/1.jpg",
            keep_reason="phash",
        ),
        None,
    )
    video_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=5.0,
            end_ts=10.0,
            description="Second scene",
            metadata=None,
            sharpness_score=2.0,
            rep_frame_path="/data/2.jpg",
            keep_reason="temporal",
        ),
        None,
    )
    assert video_repo.get_last_scene_description(asset_id) == "Second scene"


def test_metadata_jsonb_persists_nested(engine, _session_factory):
    """metadata JSONB column correctly persists and returns nested dict (e.g. moondream, showinfo)."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-jsonb")
    nested = {
        "moondream": {
            "description": "A person in a room",
            "tags": ["indoor", "person"],
            "ocr_text": None,
        },
        "showinfo": "pts_time:1.5 n:45 ...",
    }
    scene = VideoSceneRow(
        start_ts=0.0,
        end_ts=3.0,
        description="A person in a room",
        metadata=nested,
        sharpness_score=50.0,
        rep_frame_path="/data/s.jpg",
        keep_reason="phash",
    )
    video_repo.save_scene_and_update_state(asset_id, scene, None)
    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT description, metadata FROM video_scenes WHERE asset_id = :aid ORDER BY end_ts DESC LIMIT 1"
            ),
            {"aid": asset_id},
        ).fetchone()
        assert row is not None
        assert row[0] == "A person in a room"
        meta = row[1]
        assert meta is not None
        assert meta.get("moondream", {}).get("description") == "A person in a room"
        assert meta.get("moondream", {}).get("tags") == ["indoor", "person"]
        assert meta.get("showinfo") == "pts_time:1.5 n:45 ..."
    finally:
        session.close()


def test_get_active_state_empty_returns_none(engine, _session_factory):
    """get_active_state when no row exists returns None."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-state-empty")
    assert video_repo.get_active_state(asset_id) is None


def test_save_scene_and_update_state_upserts_active_state(engine, _session_factory):
    """save_scene_and_update_state with active_state UPSERTs and get_active_state returns it."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-upsert")
    scene = VideoSceneRow(
        start_ts=0.0,
        end_ts=5.0,
        description=None,
        metadata=None,
        sharpness_score=50.0,
        rep_frame_path="/data/1.jpg",
        keep_reason="phash",
    )
    state = VideoActiveState(
        anchor_phash="abc123",
        scene_start_ts=5.0,
        current_best_pts=7.0,
        current_best_sharpness=60.0,
    )
    scene_id = video_repo.save_scene_and_update_state(asset_id, scene, state)
    assert scene_id >= 1
    loaded = video_repo.get_active_state(asset_id)
    assert loaded is not None
    assert loaded.anchor_phash == "abc123"
    assert loaded.scene_start_ts == 5.0
    assert loaded.current_best_pts == 7.0
    assert loaded.current_best_sharpness == 60.0


def test_save_scene_and_update_state_delete_on_finalization(engine, _session_factory):
    """save_scene_and_update_state with active_state=None deletes the active_state row."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-finalize")
    state = VideoActiveState(
        anchor_phash="x",
        scene_start_ts=0.0,
        current_best_pts=1.0,
        current_best_sharpness=10.0,
    )
    scene = VideoSceneRow(
        start_ts=0.0,
        end_ts=10.0,
        description=None,
        metadata=None,
        sharpness_score=10.0,
        rep_frame_path="/data/final.jpg",
        keep_reason="forced",
    )
    video_repo.save_scene_and_update_state(asset_id, scene, state)
    assert video_repo.get_active_state(asset_id) is not None
    video_repo.save_scene_and_update_state(asset_id, scene, None)
    assert video_repo.get_active_state(asset_id) is None


def test_save_scene_upsert_idempotent(engine, _session_factory):
    """Calling save_scene_and_update_state twice with same asset_id and state leaves one row (UPSERT)."""
    _, video_repo = _create_tables_and_seed(engine, _session_factory)
    asset_id = _ensure_library_and_asset(_session_factory, "vid-lib-idempotent")
    scene = VideoSceneRow(
        start_ts=0.0,
        end_ts=3.0,
        description=None,
        metadata=None,
        sharpness_score=1.0,
        rep_frame_path="/data/a.jpg",
        keep_reason="phash",
    )
    state = VideoActiveState(
        anchor_phash="same",
        scene_start_ts=3.0,
        current_best_pts=4.0,
        current_best_sharpness=2.0,
    )
    video_repo.save_scene_and_update_state(asset_id, scene, state)
    video_repo.save_scene_and_update_state(asset_id, scene, state)
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM video_active_state WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).scalar()
        assert count == 1
    finally:
        session.close()
