"""Tests for SearchSyncWorker.

Requires:
- testcontainers Postgres (same pattern as other slow tests)
- dev Quickwit instance at http://127.0.0.1:7281

All tests marked slow. Tests are skipped if Quickwit is unavailable.
Indexes are created with 'test_scenes_' prefix and deleted in teardown.
"""
import json
import time
import uuid

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import (
    Asset,
    AssetType,
    LibraryModelPolicy,
    SystemMetadata,
)
from src.repository.asset_repo import AssetRepository
from src.repository.library_model_policy_repo import LibraryModelPolicyRepository
from src.repository.library_repo import LibraryRepository
from src.repository.quickwit_search_repo import QuickwitSearchRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository, VideoSceneRow
from src.repository.worker_repo import WorkerRepository
from src.workers.search_sync_worker import SearchSyncWorker

pytestmark = [pytest.mark.slow]

_QUICKWIT_DEV_URL = "http://127.0.0.1:7281"
_SCHEMA_PATH = "quickwit/media_scenes_schema.json"
_PROD_INDEX_PREFIX = "media_scenes"


def _test_index_name() -> str:
    name = f"test_scenes_{uuid.uuid4().hex[:8]}"
    assert not name.startswith(_PROD_INDEX_PREFIX)
    return name


def _wait_for_commit(seconds: int = 12) -> None:
    """Wait for Quickwit to commit indexed documents."""
    time.sleep(seconds)


@pytest.fixture(autouse=True)
def require_quickwit():
    """Skip all tests in this module if dev Quickwit is unavailable."""
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, "")
    if not repo.is_healthy():
        pytest.skip(
            f"Dev Quickwit instance not available at {_QUICKWIT_DEV_URL}. "
            "Start it before running these tests."
        )


def _bootstrap(engine, session_factory):
    """Create tables, seed system metadata, return commonly used repos."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        if session.get(SystemMetadata, "schema_version") is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return {
        "asset_repo": AssetRepository(session_factory),
        "scene_repo": VideoSceneRepository(session_factory),
        "policy_repo": LibraryModelPolicyRepository(session_factory),
        "system_metadata_repo": SystemMetadataRepository(session_factory),
        "worker_repo": WorkerRepository(session_factory),
        "lib_repo": LibraryRepository(session_factory),
    }


def _make_worker(repos, index_name: str | None = None) -> SearchSyncWorker:
    return SearchSyncWorker(
        worker_id=f"test-worker-{uuid.uuid4().hex[:8]}",
        repository=repos["worker_repo"],
        asset_repo=repos["asset_repo"],
        scene_repo=repos["scene_repo"],
        policy_repo=repos["policy_repo"],
        quickwit_base_url=_QUICKWIT_DEV_URL,
        system_metadata_repo=repos["system_metadata_repo"],
    )


def _seed_library(lib_repo, slug="test-lib") -> str:
    if lib_repo.get_by_slug(slug) is None:
        name = slug.replace("-", " ").title()
        lib_repo.add(name, f"/tmp/{slug}")
    return slug


def _seed_image_asset(asset_repo, library_slug: str, session_factory) -> Asset:
    rel_path = f"photos/img_{uuid.uuid4().hex[:6]}.jpg"
    asset_repo.upsert_asset(
        library_slug, rel_path, AssetType.image, 1000.0, 100
    )
    asset = asset_repo.get_asset(library_slug, rel_path)
    assert asset is not None
    session = session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET status = 'completed',
                    visual_analysis = CAST(:va AS jsonb),
                    preview_path = :preview_path
                WHERE id = :id
            """),
            {
                "va": json.dumps({
                    "description": "a red apple on a white table",
                    "tags": ["apple", "table"],
                }),
                "preview_path": f"/previews/{uuid.uuid4().hex[:6]}.jpg",
                "id": asset.id,
            },
        )
        session.commit()
    finally:
        session.close()
    out = asset_repo.get_asset(library_slug, rel_path)
    assert out is not None
    return out


def _seed_video_asset(asset_repo, library_slug: str, session_factory) -> Asset:
    rel_path = f"videos/vid_{uuid.uuid4().hex[:6]}.mp4"
    asset_repo.upsert_asset(
        library_slug, rel_path, AssetType.video, 1000.0, 100
    )
    asset = asset_repo.get_asset(library_slug, rel_path)
    assert asset is not None
    session = session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET status = 'completed',
                    video_preview_path = :path
                WHERE id = :id
            """),
            {
                "path": f"/previews/{uuid.uuid4().hex[:6]}.mp4",
                "id": asset.id,
            },
        )
        session.commit()
    finally:
        session.close()
    out = asset_repo.get_asset(library_slug, rel_path)
    assert out is not None
    return out


def _seed_scene(scene_repo, asset_id: int, start: float = 0.0, end: float = 4.0):
    scene_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=start,
            end_ts=end,
            description="a scene description",
            metadata={"moondream": {"description": "test scene", "tags": ["test"]}},
            sharpness_score=0.0,
            rep_frame_path=f"/frames/{uuid.uuid4().hex[:6]}.jpg",
            keep_reason="phash",
        ),
        None,
    )


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

def test_processes_completed_assets_and_writes_correct_document_count(engine, _session_factory):
    """Worker writes 1 doc per image, N docs per video with N scenes."""
    repos = _bootstrap(engine, _session_factory)
    lib_slug = _seed_library(repos["lib_repo"])
    index_name = _test_index_name()
    qw = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)

    # Pre-create the index and policy so we control the index name
    qw.create_index(index_name, _SCHEMA_PATH)
    repos["policy_repo"].upsert(LibraryModelPolicy(
        library_slug=lib_slug,
        active_index_name=index_name,
        locked=False,
        promotion_progress=0.0,
    ))

    image = _seed_image_asset(repos["asset_repo"], lib_slug, _session_factory)
    video = _seed_video_asset(repos["asset_repo"], lib_slug, _session_factory)
    _seed_scene(repos["scene_repo"], video.id, 0.0, 4.0)
    _seed_scene(repos["scene_repo"], video.id, 4.0, 8.0)

    try:
        worker = _make_worker(repos)
        result = worker.process_task()
        assert result is True
        _wait_for_commit(12)

        # 1 image doc + 2 scene docs = 3 total
        resp_image = qw.search("apple")
        resp_video = qw.search("test scene")
        assert len(resp_image) == 1
        assert resp_image[0].asset.id == image.id
        assert len(resp_video) == 2
    finally:
        qw.delete_index(index_name)


def test_progress_key_updated_after_batch(engine, _session_factory):
    """After process_task, search_sync_last_asset_id equals max asset id in batch."""
    repos = _bootstrap(engine, _session_factory)
    lib_slug = _seed_library(repos["lib_repo"])
    index_name = _test_index_name()
    qw = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)
    qw.create_index(index_name, _SCHEMA_PATH)
    repos["policy_repo"].upsert(LibraryModelPolicy(
        library_slug=lib_slug,
        active_index_name=index_name,
        locked=False,
        promotion_progress=0.0,
    ))

    asset1 = _seed_image_asset(repos["asset_repo"], lib_slug, _session_factory)
    asset2 = _seed_image_asset(repos["asset_repo"], lib_slug, _session_factory)
    expected_max_id = max(asset1.id, asset2.id)

    try:
        worker = _make_worker(repos)
        worker.process_task()
        stored = repos["system_metadata_repo"].get_value("search_sync_last_asset_id")
        assert stored == str(expected_max_id)
    finally:
        qw.delete_index(index_name)


def test_returns_false_when_no_work_remains(engine, _session_factory):
    """process_task returns False when cursor is already at or beyond all assets."""
    repos = _bootstrap(engine, _session_factory)
    lib_slug = _seed_library(repos["lib_repo"])
    asset = _seed_image_asset(repos["asset_repo"], lib_slug, _session_factory)

    # Set cursor beyond the asset
    repos["system_metadata_repo"].set_value(
        "search_sync_last_asset_id", str(asset.id + 1000)
    )
    worker = _make_worker(repos)
    result = worker.process_task()
    assert result is False


def test_video_with_no_scenes_skipped_without_error(engine, _session_factory):
    """Completed video with no scenes is skipped gracefully; batch still succeeds."""
    repos = _bootstrap(engine, _session_factory)
    lib_slug = _seed_library(repos["lib_repo"])
    index_name = _test_index_name()
    qw = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)
    qw.create_index(index_name, _SCHEMA_PATH)
    repos["policy_repo"].upsert(LibraryModelPolicy(
        library_slug=lib_slug,
        active_index_name=index_name,
        locked=False,
        promotion_progress=0.0,
    ))

    video = _seed_video_asset(repos["asset_repo"], lib_slug, _session_factory)
    # Intentionally seed no scenes

    # Ensure this video is in the batch (reset cursor so worker picks it up)
    repos["system_metadata_repo"].set_value("search_sync_last_asset_id", "0")
    try:
        worker = _make_worker(repos)
        # Must not raise
        result = worker.process_task()
        # Batch had one asset so returns True (cursor advances)
        assert result is True
        # Progress key advances to the video asset id
        stored = repos["system_metadata_repo"].get_value("search_sync_last_asset_id")
        assert stored == str(video.id)
    finally:
        qw.delete_index(index_name)


def test_policy_created_on_first_run(engine, _session_factory):
    """Worker creates policy and Quickwit index when none exists for library."""
    repos = _bootstrap(engine, _session_factory)
    lib_slug = _seed_library(repos["lib_repo"], slug="new-lib")
    # Confirm no policy exists
    assert repos["policy_repo"].get(lib_slug) is None

    asset = _seed_image_asset(repos["asset_repo"], lib_slug, _session_factory)
    # Ensure this asset is in the batch (reset cursor so worker picks it up)
    repos["system_metadata_repo"].set_value("search_sync_last_asset_id", "0")
    worker = _make_worker(repos)

    created_index = None
    try:
        worker.process_task()
        policy = repos["policy_repo"].get(lib_slug)
        assert policy is not None
        assert policy.active_index_name.startswith("media_scenes_new-lib_")
        assert policy.locked is False  # must NOT be locked on first run
        created_index = policy.active_index_name
        # Confirm the index actually exists in Quickwit
        import httpx
        resp = httpx.get(f"{_QUICKWIT_DEV_URL}/api/v1/indexes/{created_index}")
        assert resp.status_code == 200
    finally:
        if created_index:
            qw = QuickwitSearchRepository(_QUICKWIT_DEV_URL, created_index)
            qw.delete_index(created_index)
