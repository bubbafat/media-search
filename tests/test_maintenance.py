"""MaintenanceService and repository maintenance methods (testcontainers Postgres)."""

import os
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

from src.cli import app
from src.core.maintenance import MaintenanceService
from src.models.entities import (
    Asset,
    AssetStatus,
    AssetType,
    Library,
    SystemMetadata,
    WorkerState,
)
from src.models.entities import WorkerStatus as WorkerStatusEntity
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.project_repo import ProjectRepository
from src.repository.video_scene_repo import VideoSceneRow
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository

pytestmark = [pytest.mark.slow]


def _create_tables_and_repos(engine, session_factory):
    """Run alembic migrations to match production schema (VARCHAR asset.status). Return (asset_repo, worker_repo)."""
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("script_location", "migrations")
    # Attach engine for offline mode - alembic needs DB URL from env
    command.upgrade(alembic_cfg, "head")
    return AssetRepository(session_factory), WorkerRepository(session_factory)


def _create_tables_and_all_repos(engine, session_factory):
    """Create all tables, seed schema_version. Return (asset_repo, worker_repo, library_repo, video_scene_repo)."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, session_factory)
    library_repo = LibraryRepository(session_factory)
    video_scene_repo = VideoSceneRepository(session_factory)
    return asset_repo, worker_repo, library_repo, video_scene_repo


def test_count_stale_workers_matches_prune_criteria(engine, _session_factory):
    """count_stale_workers returns same count that prune_stale_workers would delete."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    worker_repo.register_worker("recent-worker", WorkerState.idle, "host1")
    worker_repo.register_worker("stale-worker", WorkerState.idle, "host1")

    session = _session_factory()
    try:
        from datetime import datetime, timedelta, timezone

        cutoff = datetime.now(timezone.utc) - timedelta(hours=25)
        session.execute(
            text(
                "UPDATE worker_status SET last_seen_at = :cutoff WHERE worker_id = 'stale-worker'"
            ),
            {"cutoff": cutoff},
        )
        session.commit()
    finally:
        session.close()

    count_before = worker_repo.count_stale_workers(max_age_hours=24)
    assert count_before == 1

    deleted = worker_repo.prune_stale_workers(max_age_hours=24)
    assert deleted == 1

    count_after = worker_repo.count_stale_workers(max_age_hours=24)
    assert count_after == 0


def test_prune_stale_workers_deletes_old_rows(engine, _session_factory):
    """prune_stale_workers deletes worker_status rows older than max_age_hours."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    worker_repo.register_worker("recent-worker", WorkerState.idle, "host1")
    worker_repo.register_worker("stale-worker", WorkerState.idle, "host1")

    session = _session_factory()
    try:
        from datetime import datetime, timedelta, timezone

        cutoff = datetime.now(timezone.utc) - timedelta(hours=25)
        row = session.get(WorkerStatusEntity, "stale-worker")
        assert row is not None
        session.execute(
            text(
                "UPDATE worker_status SET last_seen_at = :cutoff WHERE worker_id = 'stale-worker'"
            ),
            {"cutoff": cutoff},
        )
        session.commit()
    finally:
        session.close()

    deleted = worker_repo.prune_stale_workers(max_age_hours=24)
    assert deleted == 1

    session = _session_factory()
    try:
        stale = session.get(WorkerStatusEntity, "stale-worker")
        recent = session.get(WorkerStatusEntity, "recent-worker")
        assert stale is None
        assert recent is not None
    finally:
        session.close()


def test_count_stale_leases_matches_reclaim_criteria(engine, _session_factory):
    """count_stale_leases returns same count that reclaim_stale_leases would update."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="count-lib",
                name="Count Lib",
                absolute_path="/tmp/count",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("count-lib", "a.jpg", AssetType.image, 1000.0, 5000)
    asset_repo.upsert_asset("count-lib", "b.jpg", AssetType.image, 1001.0, 5001)

    claimed_a = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="count-lib"
    )
    assert claimed_a is not None
    asset_id_a = claimed_a.id
    claimed_b = asset_repo.claim_asset_by_status(
        "worker-2", AssetStatus.pending, [".jpg"], library_slug="count-lib"
    )
    assert claimed_b is not None
    asset_id_b = claimed_b.id

    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET lease_expires_at = (NOW() AT TIME ZONE 'UTC') - INTERVAL '1 hour'
                WHERE id IN (:id_a, :id_b)
            """),
            {"id_a": asset_id_a, "id_b": asset_id_b},
        )
        session.commit()
    finally:
        session.close()

    count_before = asset_repo.count_stale_leases(global_scope=True)
    assert count_before == 2

    updated = asset_repo.reclaim_stale_leases(global_scope=True)
    assert updated == 2

    count_after = asset_repo.count_stale_leases(global_scope=True)
    assert count_after == 0


def test_reclaim_stale_leases_resets_processing_assets(engine, _session_factory):
    """reclaim_stale_leases resets assets stuck in processing with expired leases."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="reclaim-lib",
                name="Reclaim Lib",
                absolute_path="/tmp/reclaim",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("reclaim-lib", "a.jpg", AssetType.image, 1000.0, 5000)
    asset_repo.upsert_asset("reclaim-lib", "b.jpg", AssetType.image, 1001.0, 5001)

    claimed_a = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="reclaim-lib"
    )
    assert claimed_a is not None
    asset_id_a = claimed_a.id
    claimed_b = asset_repo.claim_asset_by_status(
        "worker-2", AssetStatus.pending, [".jpg"], library_slug="reclaim-lib"
    )
    assert claimed_b is not None
    asset_id_b = claimed_b.id

    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET lease_expires_at = (NOW() AT TIME ZONE 'UTC') - INTERVAL '1 hour'
                WHERE id IN (:id_a, :id_b)
            """),
            {"id_a": asset_id_a, "id_b": asset_id_b},
        )
        session.commit()
    finally:
        session.close()

    updated = asset_repo.reclaim_stale_leases(global_scope=True)
    assert updated == 2

    session = _session_factory()
    try:
        row_a = session.get(Asset, asset_id_a)
        row_b = session.get(Asset, asset_id_b)
        assert row_a is not None
        assert row_b is not None
        assert row_a.status == AssetStatus.pending
        assert row_b.status == AssetStatus.pending
        assert row_a.worker_id is None
        assert row_b.worker_id is None
        assert row_a.lease_expires_at is None
        assert row_b.lease_expires_at is None
    finally:
        session.close()


def test_reclaim_stale_leases_poisons_when_retry_count_exceeds_5(engine, _session_factory):
    """reclaim_stale_leases sets status to poisoned when retry_count > 5."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="poison-lib",
                name="Poison Lib",
                absolute_path="/tmp/poison",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("poison-lib", "x.jpg", AssetType.image, 1000.0, 5000)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="poison-lib"
    )
    assert claimed is not None
    asset_id = claimed.id

    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET retry_count = 6, lease_expires_at = (NOW() AT TIME ZONE 'UTC') - INTERVAL '1 hour'
                WHERE id = :id
            """),
            {"id": asset_id},
        )
        session.commit()
    finally:
        session.close()

    updated = asset_repo.reclaim_stale_leases(global_scope=True)
    assert updated == 1

    session = _session_factory()
    try:
        row = session.get(Asset, asset_id)
        assert row is not None
        assert row.status == AssetStatus.poisoned
        assert row.retry_count == 7
    finally:
        session.close()


def test_reset_poisoned_assets_global(engine, _session_factory):
    """reset_poisoned_assets with library_slug=None resets all poisoned assets to pending."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="reset-poison-lib",
                name="Reset Poison Lib",
                absolute_path="/tmp/reset-poison",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("reset-poison-lib", "a.jpg", AssetType.image, 1000.0, 5000)
    asset_repo.upsert_asset("reset-poison-lib", "b.jpg", AssetType.image, 1001.0, 5001)

    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET status = 'poisoned', retry_count = 3, error_message = 'Something broke'
                WHERE library_id = 'reset-poison-lib'
            """),
        )
        session.commit()
    finally:
        session.close()

    updated = asset_repo.reset_poisoned_assets(library_slug=None)
    assert updated >= 2

    session = _session_factory()
    try:
        rows = session.execute(
            text(
                "SELECT status, retry_count, error_message FROM asset WHERE library_id = 'reset-poison-lib'"
            ),
        ).fetchall()
        assert len(rows) == 2
        for row in rows:
            assert row[0] == "pending"
            assert row[1] == 0
            assert row[2] is None
    finally:
        session.close()


def test_reset_poisoned_assets_filtered_by_library(engine, _session_factory):
    """reset_poisoned_assets with library_slug only resets assets in that library."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="reset-lib-a",
                name="Reset Lib A",
                absolute_path="/tmp/reset-a",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug="reset-lib-b",
                name="Reset Lib B",
                absolute_path="/tmp/reset-b",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("reset-lib-a", "a.jpg", AssetType.image, 1000.0, 5000)
    asset_repo.upsert_asset("reset-lib-b", "b.jpg", AssetType.image, 1001.0, 5001)

    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET status = 'poisoned', retry_count = 1, error_message = 'err'
                WHERE library_id IN ('reset-lib-a', 'reset-lib-b')
            """),
        )
        session.commit()
    finally:
        session.close()

    updated = asset_repo.reset_poisoned_assets(library_slug="reset-lib-a")
    assert updated == 1

    session = _session_factory()
    try:
        row_a = session.execute(
            text("SELECT status, retry_count, error_message FROM asset WHERE library_id = 'reset-lib-a'"),
        ).fetchone()
        row_b = session.execute(
            text("SELECT status, retry_count, error_message FROM asset WHERE library_id = 'reset-lib-b'"),
        ).fetchone()
        assert row_a[0] == "pending"
        assert row_a[1] == 0
        assert row_a[2] is None
        assert row_b[0] == "poisoned"
        assert row_b[1] == 1
        assert row_b[2] == "err"
    finally:
        session.close()


def test_maintenance_retry_poisoned_cli(engine, _session_factory, tmp_path):
    """maintenance retry-poisoned rescues poisoned assets and prints count."""
    asset_repo, worker_repo, library_repo, video_scene_repo = _create_tables_and_all_repos(
        engine, _session_factory
    )

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="retry-cli-lib",
                name="Retry CLI Lib",
                absolute_path="/tmp/retry-cli",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("retry-cli-lib", "a.jpg", AssetType.image, 1000.0, 5000)
    asset_repo.upsert_asset("retry-cli-lib", "b.jpg", AssetType.image, 1001.0, 5001)

    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET status = 'poisoned', retry_count = 2, error_message = 'failed'
                WHERE library_id = 'retry-cli-lib'
            """),
        )
        session.commit()
    finally:
        session.close()

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["maintenance", "retry-poisoned", "--library", "retry-cli-lib"],
        env={**os.environ, "MEDIA_SEARCH_DATA_DIR": str(tmp_path)},
    )
    assert result.exit_code == 0
    assert "Rescued 2 asset(s) back into the pipeline." in result.stdout

    result2 = runner.invoke(
        app,
        ["maintenance", "retry-poisoned", "--library", "retry-cli-lib"],
        env={**os.environ, "MEDIA_SEARCH_DATA_DIR": str(tmp_path)},
    )
    assert result2.exit_code == 0
    assert "No poisoned assets to rescue." in result2.stdout


def test_cleanup_temp_dir_removes_old_files(tmp_path):
    """cleanup_temp_dir deletes files older than max_age_seconds."""
    tmp_dir = tmp_path / "tmp"
    tmp_dir.mkdir()
    old_file = tmp_dir / "old.txt"
    new_file = tmp_dir / "new.txt"
    old_file.write_text("old")
    new_file.write_text("new")
    past = time.time() - (5 * 3600)
    old_file.touch()
    new_file.touch()
    import os

    os.utime(old_file, (past, past))

    svc = MaintenanceService(
        asset_repo=MagicMock(),
        worker_repo=MagicMock(),
        data_dir=tmp_path,
        library_repo=MagicMock(),
        video_scene_repo=MagicMock(),
    )
    deleted = svc.cleanup_temp_dir(max_age_seconds=4 * 3600)
    assert deleted == 1
    assert not old_file.exists()
    assert new_file.exists()


def test_preview_temp_cleanup_returns_count_and_size(tmp_path):
    """preview_temp_cleanup returns file count and total bytes without deleting."""
    tmp_dir = tmp_path / "tmp"
    tmp_dir.mkdir()
    old_file = tmp_dir / "old.txt"
    new_file = tmp_dir / "new.txt"
    old_file.write_text("12345")  # 5 bytes
    new_file.write_text("new")
    past = time.time() - (5 * 3600)
    import os

    os.utime(old_file, (past, past))
    os.utime(new_file, (time.time(), time.time()))

    svc = MaintenanceService(
        asset_repo=MagicMock(),
        worker_repo=MagicMock(),
        data_dir=tmp_path,
        library_repo=MagicMock(),
        video_scene_repo=MagicMock(),
    )
    count, total_bytes = svc.preview_temp_cleanup(max_age_seconds=4 * 3600)
    assert count == 1
    assert total_bytes == 5
    assert old_file.exists()
    assert new_file.exists()


def test_cleanup_temp_dir_skips_when_local_transcode_active(tmp_path):
    """cleanup_temp_dir skips tmp when has_active_local_transcodes returns True."""
    tmp_dir = tmp_path / "tmp"
    tmp_dir.mkdir()
    old_file = tmp_dir / "old.txt"
    old_file.write_text("old")
    past = time.time() - (5 * 3600)
    os.utime(old_file, (past, past))

    worker_repo = MagicMock(spec=WorkerRepository)
    worker_repo.has_active_local_transcodes.return_value = True
    svc = MaintenanceService(
        asset_repo=MagicMock(),
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=MagicMock(),
        video_scene_repo=MagicMock(),
        hostname="test-host",
    )
    deleted = svc.cleanup_temp_dir(max_age_seconds=4 * 3600)
    assert deleted == 0
    assert old_file.exists()
    worker_repo.has_active_local_transcodes.assert_called_once_with("test-host")


def test_cleanup_temp_dir_skips_nonexistent_dir(tmp_path):
    """cleanup_temp_dir returns 0 when tmp dir does not exist."""
    svc = MaintenanceService(
        asset_repo=MagicMock(),
        worker_repo=MagicMock(),
        data_dir=tmp_path,
        library_repo=MagicMock(),
        video_scene_repo=MagicMock(),
    )
    deleted = svc.cleanup_temp_dir()
    assert deleted == 0


def test_run_all_executes_all_tasks():
    """run_all invokes prune_stale_workers, reclaim_stale_leases, cleanup_temp_dir."""
    asset_repo = MagicMock(spec=AssetRepository)
    worker_repo = MagicMock(spec=WorkerRepository)
    asset_repo.reclaim_stale_leases.return_value = 0
    worker_repo.prune_stale_workers.return_value = 0

    svc = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=Path("/nonexistent"),
        library_repo=MagicMock(),
        video_scene_repo=MagicMock(),
    )
    svc.run_all()

    worker_repo.prune_stale_workers.assert_called_once_with(max_age_hours=24)
    asset_repo.reclaim_stale_leases.assert_called_once()
    # cleanup_temp_dir runs; dir doesn't exist so returns 0
    assert not (Path("/nonexistent") / "tmp").exists()


def test_preview_data_dir_cleanup_returns_count_and_size(
    engine, _session_factory, tmp_path
):
    """preview_data_dir_cleanup returns file count and total bytes without deleting."""
    asset_repo, worker_repo, library_repo, video_scene_repo = _create_tables_and_all_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="preview-lib",
                name="Preview Lib",
                absolute_path="/tmp/preview",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    orphan_dir = tmp_path / "preview-lib" / "thumbnails" / "999"
    orphan_dir.mkdir(parents=True)
    orphan_file = orphan_dir / "999.jpg"
    orphan_file.write_text("x" * 100)  # 100 bytes
    import os

    os.utime(orphan_file, (time.time() - 20 * 60, time.time() - 20 * 60))

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    file_count, total_bytes = service.preview_data_dir_cleanup(
        min_file_age_seconds=15 * 60
    )
    assert file_count == 1
    assert total_bytes == 100
    assert orphan_file.exists()


def test_cleanup_data_dir_removes_orphaned_files(engine, _session_factory, tmp_path):
    """cleanup_data_dir deletes orphan files (no DB entry) older than 15 min."""
    asset_repo, worker_repo, library_repo, video_scene_repo = _create_tables_and_all_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="cleanup-lib",
                name="Cleanup Lib",
                absolute_path="/tmp/cleanup",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Orphan file: no asset with this id
    orphan_dir = tmp_path / "cleanup-lib" / "thumbnails" / "999"
    orphan_dir.mkdir(parents=True)
    orphan_file = orphan_dir / "999.jpg"
    orphan_file.write_text("orphan")
    import os

    os.utime(orphan_file, (time.time() - 20 * 60, time.time() - 20 * 60))

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    deleted = service.cleanup_data_dir(min_file_age_seconds=15 * 60)
    assert deleted == 1
    assert not orphan_file.exists()


def test_cleanup_data_dir_skips_trashed_libraries(engine, _session_factory, tmp_path):
    """cleanup_data_dir does not touch files under trashed library paths."""
    asset_repo, worker_repo, library_repo, video_scene_repo = _create_tables_and_all_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        from datetime import datetime, timezone

        session.add(
            Library(
                slug="trashed-lib",
                name="Trashed Lib",
                absolute_path="/tmp/trashed",
                is_active=True,
                sampling_limit=100,
                deleted_at=datetime.now(timezone.utc),
            )
        )
        session.commit()
    finally:
        session.close()

    orphan_dir = tmp_path / "trashed-lib" / "thumbnails" / "0"
    orphan_dir.mkdir(parents=True)
    orphan_file = orphan_dir / "1.jpg"
    orphan_file.write_text("orphan")
    import os

    os.utime(orphan_file, (time.time() - 20 * 60, time.time() - 20 * 60))

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    deleted = service.cleanup_data_dir(min_file_age_seconds=15 * 60)
    assert deleted == 0
    assert orphan_file.exists()


def test_cleanup_data_dir_skips_recent_files(engine, _session_factory, tmp_path):
    """cleanup_data_dir does not delete orphan files newer than 15 min."""
    asset_repo, worker_repo, library_repo, video_scene_repo = _create_tables_and_all_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="recent-lib",
                name="Recent Lib",
                absolute_path="/tmp/recent",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    orphan_dir = tmp_path / "recent-lib" / "thumbnails" / "0"
    orphan_dir.mkdir(parents=True)
    orphan_file = orphan_dir / "1.jpg"
    orphan_file.write_text("recent orphan")
    # mtime is now (recent)

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    deleted = service.cleanup_data_dir(min_file_age_seconds=15 * 60)
    assert deleted == 0
    assert orphan_file.exists()


def test_cleanup_data_dir_keeps_expected_files(engine, _session_factory, tmp_path):
    """cleanup_data_dir does not delete files that have DB entries."""
    asset_repo, worker_repo, library_repo, video_scene_repo = _create_tables_and_all_repos(
        engine, _session_factory
    )
    lib_slug = f"expected-lib-{uuid.uuid4().hex[:8]}"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=lib_slug,
                name="Expected Lib",
                absolute_path="/tmp/expected",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(lib_slug, "a.jpg", AssetType.image, 1000.0, 5000)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug=lib_slug
    )
    assert claimed is not None
    asset_id = claimed.id
    asset_repo.update_asset_status(asset_id, AssetStatus.proxied)

    # Verify asset appears in get_asset_ids_expecting_proxy (pre-condition for expected set)
    proxy_ids = asset_repo.get_asset_ids_expecting_proxy(library_slug=lib_slug)
    assert (asset_id, lib_slug, "image") in proxy_ids, (
        f"Asset {asset_id} not in get_asset_ids_expecting_proxy; proxy_ids={proxy_ids}"
    )

    shard = asset_id % 1000
    thumb_dir = tmp_path / lib_slug / "thumbnails" / str(shard)
    thumb_dir.mkdir(parents=True)
    thumb_file = thumb_dir / f"{asset_id}.jpg"
    thumb_file.write_text("expected thumb")
    os.utime(thumb_file, (time.time() - 20 * 60, time.time() - 20 * 60))

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    deleted = service.cleanup_data_dir(min_file_age_seconds=15 * 60)
    assert deleted == 0, f"Expected 0 deleted, got {deleted}; thumb file should be in expected set"
    assert thumb_file.exists()


def test_get_all_asset_paths_returns_ids_in_non_deleted_libraries(
    engine, _session_factory
):
    """get_all_asset_paths returns (id, library_slug, rel_path, is_in_project) for assets in non-deleted libs."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="paths-lib",
                name="Paths Lib",
                absolute_path="/tmp/paths",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("paths-lib", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("paths-lib", "b.mp4", AssetType.video, 2000.0, 200)
    paths = asset_repo.get_all_asset_paths(limit=100, offset=0)
    assert len(paths) >= 2
    ids = {p[0] for p in paths}
    lib_slugs = {p[1] for p in paths}
    assert "paths-lib" in lib_slugs
    rel_paths = [p[2] for p in paths if p[1] == "paths-lib"]
    assert "a.jpg" in rel_paths
    assert "b.mp4" in rel_paths
    # Assets not in projects have is_in_project=False
    paths_lib_rows = [p for p in paths if p[1] == "paths-lib"]
    for row in paths_lib_rows:
        assert row[3] is False


def test_get_all_asset_paths_includes_is_in_project(engine, _session_factory):
    """get_all_asset_paths returns is_in_project=True for assets in a project."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)
    project_repo = ProjectRepository(_session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="project-lib",
                name="Project Lib",
                absolute_path="/tmp/project",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("project-lib", "in_project.jpg", AssetType.image, 1000.0, 100)
    asset = asset_repo.get_asset("project-lib", "in_project.jpg")
    assert asset is not None
    project = project_repo.create_project("Test Bin")
    project_repo.add_asset_to_project(project.id or 0, asset.id)

    paths = asset_repo.get_all_asset_paths(limit=100, offset=0)
    in_project_row = next(
        (p for p in paths if p[0] == asset.id),
        None,
    )
    assert in_project_row is not None
    assert in_project_row[3] is True


def test_delete_asset_cascade_removes_asset_and_dependents(
    engine, _session_factory
):
    """delete_asset_cascade removes video_active_state, video_scenes, videoframe, project_assets, asset."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)
    scene_repo = VideoSceneRepository(_session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="cascade-lib",
                name="Cascade Lib",
                absolute_path="/tmp/cascade",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("cascade-lib", "video.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset("cascade-lib", "video.mp4")
    assert asset is not None
    asset_id = asset.id
    scene_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description=None,
            metadata=None,
            sharpness_score=1.0,
            rep_frame_path="video_scenes/cascade-lib/1/0_5.jpg",
            keep_reason="phash",
        ),
        None,
    )
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).fetchone()
        assert row[0] == 1
    finally:
        session.close()

    asset_repo.delete_asset_cascade(asset_id)

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE id = :aid"),
            {"aid": asset_id},
        ).fetchone()
        assert row[0] == 0
        row2 = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :aid"),
            {"aid": asset_id},
        ).fetchone()
        assert row2[0] == 0
    finally:
        session.close()


def test_delete_asset_cascade_raises_for_project_asset(engine, _session_factory):
    """delete_asset_cascade raises RuntimeError when asset is in a project."""
    asset_repo, worker_repo = _create_tables_and_repos(engine, _session_factory)
    project_repo = ProjectRepository(_session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="protected-lib",
                name="Protected Lib",
                absolute_path="/tmp/protected",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("protected-lib", "protected.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset("protected-lib", "protected.mp4")
    assert asset is not None
    asset_id = asset.id
    project = project_repo.create_project("Protected Bin")
    project_repo.add_asset_to_project(project.id or 0, asset_id)

    with pytest.raises(RuntimeError) as exc_info:
        asset_repo.delete_asset_cascade(asset_id)
    assert "linked to a project" in str(exc_info.value)
    assert str(asset_id) in str(exc_info.value)

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE id = :aid"),
            {"aid": asset_id},
        ).fetchone()
        assert row[0] == 1
    finally:
        session.close()


def test_reap_missing_source_files_dry_run_counts_only(
    engine, _session_factory, tmp_path
):
    """reap_missing_source_files(dry_run=True) counts missing sources without deleting."""
    asset_repo, worker_repo, library_repo, video_scene_repo = (
        _create_tables_and_all_repos(engine, _session_factory)
    )
    lib_slug = f"reap-dry-lib-{uuid.uuid4().hex[:8]}"
    lib_path = tmp_path / lib_slug
    lib_path.mkdir()
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=lib_slug,
                name="Reap Dry Lib",
                absolute_path=str(lib_path.resolve()),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(lib_slug, "missing.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset(lib_slug, "missing.mp4")
    assert asset is not None

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    would_delete, deleted = service.reap_missing_source_files(dry_run=True)
    assert would_delete >= 1
    assert deleted == 0
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE library_id = :slug"),
            {"slug": lib_slug},
        ).fetchone()
        assert row[0] == 1
    finally:
        session.close()


def test_reap_missing_source_files_deletes_assets_with_missing_source(
    engine, _session_factory, tmp_path
):
    """reap_missing_source_files(dry_run=False) deletes assets and files when source missing."""
    asset_repo, worker_repo, library_repo, video_scene_repo = (
        _create_tables_and_all_repos(engine, _session_factory)
    )
    lib_slug = f"reap-exec-lib-{uuid.uuid4().hex[:8]}"
    lib_path = tmp_path / lib_slug
    lib_path.mkdir()
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=lib_slug,
                name="Reap Exec Lib",
                absolute_path=str(lib_path.resolve()),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(lib_slug, "gone.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset(lib_slug, "gone.mp4")
    assert asset is not None
    asset_id = asset.id

    from src.core.storage import LocalMediaStore

    mock_cfg = MagicMock()
    mock_cfg.data_dir = str(tmp_path)
    with patch("src.core.storage.get_config", return_value=mock_cfg):
        store = LocalMediaStore()
        thumb_path = (
            tmp_path / lib_slug / "thumbnails" / str(asset_id % 1000) / f"{asset_id}.jpg"
        )
        thumb_path.parent.mkdir(parents=True, exist_ok=True)
        thumb_path.write_text("thumb")

        service = MaintenanceService(
            asset_repo=asset_repo,
            worker_repo=worker_repo,
            data_dir=tmp_path,
            library_repo=library_repo,
            video_scene_repo=video_scene_repo,
            storage=store,
        )
        would_delete, deleted = service.reap_missing_source_files(dry_run=False)
    assert deleted >= 1
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE id = :aid"),
            {"aid": asset_id},
        ).fetchone()
        assert row[0] == 0
    finally:
        session.close()


def test_reap_missing_source_files_skips_project_assets(
    engine, _session_factory, tmp_path
):
    """reap_missing_source_files skips assets in projects: no DB delete, no proxy delete, logs warning."""
    asset_repo, worker_repo, library_repo, video_scene_repo = (
        _create_tables_and_all_repos(engine, _session_factory)
    )
    project_repo = ProjectRepository(_session_factory)
    lib_slug = f"reap-protected-lib-{uuid.uuid4().hex[:8]}"
    lib_path = tmp_path / lib_slug
    lib_path.mkdir()
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=lib_slug,
                name="Reap Protected Lib",
                absolute_path=str(lib_path.resolve()),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(lib_slug, "protected_missing.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset(lib_slug, "protected_missing.mp4")
    assert asset is not None
    asset_id = asset.id
    project = project_repo.create_project("Protected Bin")
    project_repo.add_asset_to_project(project.id or 0, asset_id)

    from src.core.storage import LocalMediaStore

    mock_cfg = MagicMock()
    mock_cfg.data_dir = str(tmp_path)
    with patch("src.core.storage.get_config", return_value=mock_cfg):
        store = LocalMediaStore()
        proxy_path = (
            tmp_path / lib_slug / "proxies" / str(asset_id % 1000) / f"{asset_id}.webp"
        )
        proxy_path.parent.mkdir(parents=True, exist_ok=True)
        proxy_path.write_text("proxy")

        service = MaintenanceService(
            asset_repo=asset_repo,
            worker_repo=worker_repo,
            data_dir=tmp_path,
            library_repo=library_repo,
            video_scene_repo=video_scene_repo,
            storage=store,
        )
        would_delete, deleted = service.reap_missing_source_files(dry_run=False)
    assert deleted == 0

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE id = :aid"),
            {"aid": asset_id},
        ).fetchone()
        assert row[0] == 1, "Asset should remain in DB"
    finally:
        session.close()
    assert proxy_path.exists(), "Proxy should remain on disk"


def test_maintenance_run_dry_run_shows_preview(engine, _session_factory, tmp_path):
    """maintenance run --dry-run prints stale workers, stale leases, temp file info, would reap without changes."""
    _create_tables_and_all_repos(engine, _session_factory)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["maintenance", "run", "--dry-run"],
        env={**os.environ, "MEDIA_SEARCH_DATA_DIR": str(tmp_path)},
    )
    assert result.exit_code == 0
    assert "Dry run" in result.stdout
    assert "Stale workers" in result.stdout
    assert "Stale leases" in result.stdout
    assert "Temp files" in result.stdout
    assert "Would reap" in result.stdout
    assert "Run without --dry-run to apply changes" in result.stdout


def test_maintenance_cleanup_data_dir_dry_run_shows_preview(
    engine, _session_factory, tmp_path
):
    """maintenance cleanup-data-dir --dry-run prints orphaned file count without deleting."""
    _create_tables_and_all_repos(engine, _session_factory)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["maintenance", "cleanup-data-dir", "--dry-run"],
        env={**os.environ, "MEDIA_SEARCH_DATA_DIR": str(tmp_path)},
    )
    assert result.exit_code == 0
    assert "Dry run" in result.stdout
    assert "files" in result.stdout
    assert "reclaimable" in result.stdout
    assert "Run without --dry-run to apply changes" in result.stdout


def test_cleanup_data_dir_skips_nonexistent_dirs(tmp_path):
    """cleanup_data_dir returns 0 when data_dir has no library subdirs."""
    asset_repo = MagicMock(spec=AssetRepository)
    asset_repo.get_asset_ids_expecting_proxy.return_value = []
    asset_repo.get_all_video_preview_paths_excluding_trash.return_value = []
    worker_repo = MagicMock(spec=WorkerRepository)
    library_repo = MagicMock(spec=LibraryRepository)
    library_repo.list_libraries.return_value = []
    video_scene_repo = MagicMock(spec=VideoSceneRepository)
    video_scene_repo.get_all_rep_frame_paths_excluding_trash.return_value = []

    service = MaintenanceService(
        asset_repo=asset_repo,
        worker_repo=worker_repo,
        data_dir=tmp_path,
        library_repo=library_repo,
        video_scene_repo=video_scene_repo,
    )
    deleted = service.cleanup_data_dir(min_file_age_seconds=15 * 60)
    assert deleted == 0
