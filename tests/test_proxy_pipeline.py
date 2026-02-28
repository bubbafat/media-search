"""Tests for proxy pipeline: claim_asset_by_status and update_asset_status (testcontainers Postgres)."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.core.file_extensions import (
    IMAGE_EXTENSIONS_LIST,
    PROXYABLE_EXTENSIONS_LIST,
    VIDEO_EXTENSIONS_LIST,
)
from src.models.entities import AIModel, Asset, AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository

pytestmark = [pytest.mark.slow]


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return AssetRepository."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return AssetRepository(session_factory)


def _set_all_asset_statuses_to(engine, session_factory, status: AssetStatus) -> None:
    """Set every asset's status so claim only sees the ones we create in the test."""
    session = session_factory()
    try:
        session.execute(text("UPDATE asset SET status = :s"), {"s": status.value})
        session.commit()
    finally:
        session.close()


def test_claim_asset_by_status_returns_asset_with_library(engine, _session_factory):
    """claim_asset_by_status claims one pending asset and returns it with library loaded."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="proxy-lib",
                name="Proxy Lib",
                absolute_path="/tmp/proxy-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("proxy-lib", "photo.jpg", AssetType.image, 1000.0, 5000)

    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg", ".jpeg", ".png"], library_slug="proxy-lib"
    )
    assert claimed is not None
    assert claimed.id is not None
    assert claimed.status == AssetStatus.processing
    assert claimed.worker_id == "worker-1"
    assert claimed.retry_count == 0
    assert claimed.library is not None
    assert claimed.library.slug == "proxy-lib"
    assert claimed.library.absolute_path == "/tmp/proxy-lib"
    assert claimed.rel_path == "photo.jpg"


def test_renew_asset_lease_updates_lease_expires_at(engine, _session_factory):
    """renew_asset_lease bumps lease_expires_at for a claimed asset."""
    from datetime import datetime, timezone

    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="lease-lib",
                name="Lease Lib",
                absolute_path="/tmp/lease",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("lease-lib", "video.mp4", AssetType.video, 2000.0, 10_000)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".mp4", ".mov"], library_slug="lease-lib"
    )
    assert claimed is not None
    asset_id = claimed.id

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT lease_expires_at FROM asset WHERE id = :id"), {"id": asset_id}
        ).fetchone()
        lease_before = row[0]
    finally:
        session.close()

    asset_repo.renew_asset_lease(asset_id, lease_seconds=60)

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT lease_expires_at FROM asset WHERE id = :id"), {"id": asset_id}
        ).fetchone()
        lease_after = row[0]
    finally:
        session.close()

    assert lease_after is not None
    assert lease_before is not None
    # New lease should be ~60s from now (allow 0–120s window for clock/timing)
    now_utc = datetime.now(timezone.utc)
    if lease_after.tzinfo is None:
        lease_after = lease_after.replace(tzinfo=timezone.utc)
    assert (lease_after - now_utc).total_seconds() > 0
    assert (lease_after - now_utc).total_seconds() < 120


def test_get_video_asset_ids_by_library_returns_only_videos_excludes_deleted(engine, _session_factory):
    """get_video_asset_ids_by_library returns video asset IDs for the library and excludes deleted libraries."""
    from datetime import datetime, timezone

    asset_repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="vid-reindex-lib",
                name="Vid Reindex Lib",
                absolute_path="/tmp/vid-reindex",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug="deleted-vid-lib",
                name="Deleted Vid Lib",
                absolute_path="/tmp/deleted-vid",
                is_active=True,
                sampling_limit=100,
                deleted_at=datetime.now(timezone.utc),
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("vid-reindex-lib", "a.mp4", AssetType.video, 1000.0, 1000)
    asset_repo.upsert_asset("vid-reindex-lib", "b.mov", AssetType.video, 1000.0, 2000)
    asset_repo.upsert_asset("vid-reindex-lib", "c.jpg", AssetType.image, 1000.0, 500)
    asset_repo.upsert_asset("deleted-vid-lib", "d.mp4", AssetType.video, 1000.0, 3000)

    ids = asset_repo.get_video_asset_ids_by_library("vid-reindex-lib")
    assert len(ids) == 2
    session = _session_factory()
    try:
        rows = session.execute(
            text("SELECT id, rel_path FROM asset WHERE library_id = 'vid-reindex-lib' AND type = 'video'"),
        ).fetchall()
    finally:
        session.close()
    expected = {int(r[0]) for r in rows}
    assert set(ids) == expected
    assert {r[1] for r in rows} == {"a.mp4", "b.mov"}

    deleted_ids = asset_repo.get_video_asset_ids_by_library("deleted-vid-lib")
    assert deleted_ids == []


def test_claim_asset_by_status_returns_none_when_no_eligible(engine, _session_factory):
    """claim_asset_by_status returns None when no asset has the given status."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="empty-lib",
                name="Empty",
                absolute_path="/tmp/empty",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="empty-lib"
    )
    assert claimed is None


def test_claim_asset_by_status_filters_by_extension(engine, _session_factory):
    """claim_asset_by_status only claims assets whose rel_path ends with supported ext."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="ext-lib",
                name="Ext Lib",
                absolute_path="/tmp/ext",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("ext-lib", "file.mp4", AssetType.video, 1000.0, 100)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg", ".png"], library_slug="ext-lib"
    )
    assert claimed is None


def test_update_asset_status_clears_worker_and_lease(engine, _session_factory):
    """update_asset_status sets status and clears worker_id and lease_expires_at."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="update-lib",
                name="Update Lib",
                absolute_path="/tmp/update",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("update-lib", "x.jpg", AssetType.image, 0.0, 0)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="update-lib"
    )
    assert claimed is not None
    assert claimed.worker_id == "worker-1"

    asset_repo.update_asset_status(claimed.id, AssetStatus.proxied)

    session = _session_factory()
    try:
        row = session.get(Asset, claimed.id)
        assert row is not None
        assert row.status == AssetStatus.proxied
        assert row.worker_id is None
        assert row.lease_expires_at is None
    finally:
        session.close()


def test_mark_completed_sets_status_and_analysis_model_clears_worker(engine, _session_factory):
    """mark_completed sets status=completed, analysis_model_id, clears worker_id and lease."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="mc-lib",
                name="MC Lib",
                absolute_path="/tmp/mc",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(AIModel(name="analyzer", version="1.0"))
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("mc-lib", "done.jpg", AssetType.image, 0.0, 0)
    claimed = asset_repo.claim_asset_by_status(
        "ai-worker-1", AssetStatus.pending, [".jpg"], library_slug="mc-lib"
    )
    assert claimed is not None
    assert claimed.worker_id == "ai-worker-1"

    session = _session_factory()
    try:
        model_row = session.execute(
            text("SELECT id FROM aimodel WHERE name = 'analyzer' AND version = '1.0'")
        ).fetchone()
        model_id = model_row[0]
    finally:
        session.close()

    asset_repo.mark_completed(claimed.id, model_id)

    session = _session_factory()
    try:
        row = session.get(Asset, claimed.id)
        assert row is not None
        assert row.status == AssetStatus.completed
        assert row.analysis_model_id == model_id
        assert row.worker_id is None
        assert row.lease_expires_at is None
    finally:
        session.close()


def test_update_asset_status_sets_error_message(engine, _session_factory):
    """update_asset_status can set error_message (e.g. for poisoned)."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="err-lib",
                name="Err Lib",
                absolute_path="/tmp/err",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("err-lib", "bad.jpg", AssetType.image, 0.0, 0)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="err-lib"
    )
    assert claimed is not None

    asset_repo.update_asset_status(
        claimed.id, AssetStatus.poisoned, error_message="File corrupted"
    )

    session = _session_factory()
    try:
        row = session.get(Asset, claimed.id)
        assert row is not None
        assert row.status == AssetStatus.poisoned
        assert row.error_message == "File corrupted"
    finally:
        session.close()


def test_claim_asset_reclaims_expired_lease(engine, _session_factory):
    """claim_asset_by_status can reclaim assets with processing status and expired lease."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="expired-lease-lib",
                name="Expired Lease Lib",
                absolute_path="/tmp/expired-lease",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("expired-lease-lib", "vid.mp4", AssetType.video, 1000.0, 5000)
    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset
                SET status = 'processing', worker_id = 'crashed-worker',
                    lease_expires_at = (NOW() AT TIME ZONE 'UTC') - interval '1 hour'
                WHERE library_id = 'expired-lease-lib' AND rel_path = 'vid.mp4'
            """)
        )
        session.commit()
    finally:
        session.close()

    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".mp4", ".mov"], library_slug="expired-lease-lib"
    )
    assert claimed is not None
    assert claimed.status == AssetStatus.processing
    assert claimed.worker_id == "worker-1"
    assert claimed.rel_path == "vid.mp4"


def test_update_asset_status_increments_retry_on_failed(engine, _session_factory):
    """update_asset_status(asset_id, AssetStatus.failed) increments retry_count."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="retry-inc-lib",
                name="Retry Inc Lib",
                absolute_path="/tmp/retry-inc",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("retry-inc-lib", "x.jpg", AssetType.image, 0.0, 0)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="retry-inc-lib"
    )
    assert claimed is not None
    asset_id = claimed.id

    asset_repo.update_asset_status(asset_id, AssetStatus.failed, "transient error")
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status, retry_count FROM asset WHERE id = :id"), {"id": asset_id}
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"
        assert int(row[1]) == 1
    finally:
        session.close()


def test_update_asset_status_resets_retry_on_proxied(engine, _session_factory):
    """update_asset_status(asset_id, AssetStatus.proxied) resets retry_count to 0."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="retry-reset-lib",
                name="Retry Reset Lib",
                absolute_path="/tmp/retry-reset",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("retry-reset-lib", "y.jpg", AssetType.image, 0.0, 0)
    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug="retry-reset-lib"
    )
    assert claimed is not None
    asset_id = claimed.id

    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET retry_count = 3 WHERE id = :id"), {"id": asset_id}
        )
        session.commit()
    finally:
        session.close()

    asset_repo.update_asset_status(asset_id, AssetStatus.proxied)
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status, retry_count FROM asset WHERE id = :id"), {"id": asset_id}
        ).fetchone()
        assert row is not None
        assert row[0] == "proxied"
        assert int(row[1]) == 0
    finally:
        session.close()


def test_claim_asset_by_status_with_library_slug_returns_asset_from_that_library(
    engine, _session_factory
):
    """claim_asset_by_status(library_slug=X) only claims assets from library X."""
    slug_a, slug_b = "lib-a-returns-asset", "lib-b-returns-asset"
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug_a,
                name="Lib A",
                absolute_path="/tmp/lib-a",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug=slug_b,
                name="Lib B",
                absolute_path="/tmp/lib-b",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug_a, "a.jpg", AssetType.image, 1000.0, 5000)
    asset_repo.upsert_asset(slug_b, "b.jpg", AssetType.image, 1000.0, 5000)

    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug=slug_a
    )
    assert claimed is not None
    assert claimed.library_id == slug_a
    assert claimed.rel_path == "a.jpg"

    # No more pending in lib-a
    again = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug=slug_a
    )
    assert again is None

    # Can still claim from lib-b
    from_b = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug=slug_b
    )
    assert from_b is not None
    assert from_b.library_id == slug_b
    assert from_b.rel_path == "b.jpg"


def test_claim_asset_by_status_with_library_slug_returns_none_when_no_pending_in_that_library(
    engine, _session_factory
):
    """claim_asset_by_status(library_slug=X) returns None when library X has no pending assets."""
    slug_a, slug_b = "lib-a-returns-none", "lib-b-returns-none"
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug_a,
                name="Lib A",
                absolute_path="/tmp/lib-a",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug=slug_b,
                name="Lib B",
                absolute_path="/tmp/lib-b",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Only lib-a has a pending asset
    asset_repo.upsert_asset(slug_a, "a.jpg", AssetType.image, 1000.0, 5000)

    claimed = asset_repo.claim_asset_by_status(
        "worker-1", AssetStatus.pending, [".jpg"], library_slug=slug_b
    )
    assert claimed is None


def test_claim_asset_by_status_filters_by_effective_target_model(engine, _session_factory):
    """claim_asset_by_status with target_model_id and system_default_model_id only claims assets whose library effective target matches."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.pending)
    session = _session_factory()
    try:
        session.add(AIModel(name="model_a", version="1"))
        session.add(AIModel(name="model_b", version="1"))
        session.commit()
        rows = session.execute(
            text("SELECT id FROM aimodel WHERE name IN ('model_a', 'model_b') ORDER BY name")
        ).fetchall()
        id_a, id_b = rows[0][0], rows[1][0]
        session.add(
            Library(
                slug="eff-lib-default",
                name="Default",
                absolute_path="/tmp/eff-default",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=None,
            )
        )
        session.add(
            Library(
                slug="eff-lib-a",
                name="A",
                absolute_path="/tmp/eff-a",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=id_a,
            )
        )
        session.add(
            Library(
                slug="eff-lib-b",
                name="B",
                absolute_path="/tmp/eff-b",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=id_b,
            )
        )
        session.commit()
    finally:
        session.close()

    system_metadata_repo = SystemMetadataRepository(_session_factory)
    system_metadata_repo.set_value(SystemMetadataRepository.DEFAULT_AI_MODEL_ID_KEY, str(id_a))

    asset_repo.upsert_asset("eff-lib-default", "d.jpg", AssetType.image, 0.0, 100)
    asset_repo.upsert_asset("eff-lib-a", "a.jpg", AssetType.image, 0.0, 100)
    asset_repo.upsert_asset("eff-lib-b", "b.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE library_id IN ('eff-lib-default', 'eff-lib-a', 'eff-lib-b')")
        )
        session.commit()
    finally:
        session.close()

    claimed1 = asset_repo.claim_asset_by_status(
        "worker-a",
        AssetStatus.proxied,
        [".jpg"],
        target_model_id=id_a,
        system_default_model_id=id_a,
        library_slug=None,
        global_scope=True,
    )
    assert claimed1 is not None
    assert claimed1.library_id in ("eff-lib-default", "eff-lib-a")

    claimed2 = asset_repo.claim_asset_by_status(
        "worker-a",
        AssetStatus.proxied,
        [".jpg"],
        target_model_id=id_a,
        system_default_model_id=id_a,
        library_slug=None,
        global_scope=True,
    )
    assert claimed2 is not None
    assert claimed2.library_id in ("eff-lib-default", "eff-lib-a")
    assert claimed2.id != claimed1.id

    claimed3 = asset_repo.claim_asset_by_status(
        "worker-a",
        AssetStatus.proxied,
        [".jpg"],
        target_model_id=id_a,
        system_default_model_id=id_a,
        library_slug=None,
        global_scope=True,
    )
    assert claimed3 is None

    claimed_b = asset_repo.claim_asset_by_status(
        "worker-b",
        AssetStatus.proxied,
        [".jpg"],
        target_model_id=id_b,
        system_default_model_id=id_a,
        library_slug=None,
        global_scope=True,
    )
    assert claimed_b is not None
    assert claimed_b.library_id == "eff-lib-b"


def test_count_pending_rejects_implicit_global_scope(engine, _session_factory):
    """count_pending with library_slug=None raises ValueError unless global_scope=True."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    with pytest.raises(ValueError, match="Pass library_slug or global_scope=True"):
        asset_repo.count_pending(None)
    with pytest.raises(ValueError, match="Pass library_slug or global_scope=True"):
        asset_repo.count_pending(library_slug=None)


def test_count_pending(engine, _session_factory):
    """count_pending returns count of pending assets in non-deleted libraries."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="cnt-lib",
                name="Count Lib",
                absolute_path="/tmp/cnt",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    assert asset_repo.count_pending(global_scope=True) == 0
    asset_repo.upsert_asset("cnt-lib", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("cnt-lib", "b.jpg", AssetType.image, 1000.0, 200)
    assert asset_repo.count_pending(global_scope=True) == 2
    assert asset_repo.count_pending("cnt-lib") == 2


def test_count_pending_filtered_by_library(engine, _session_factory):
    """count_pending(library_slug) returns only pending assets for that library."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="cnt-a",
                name="Cnt A",
                absolute_path="/tmp/cnt-a",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug="cnt-b",
                name="Cnt B",
                absolute_path="/tmp/cnt-b",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("cnt-a", "a1.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("cnt-a", "a2.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("cnt-b", "b1.jpg", AssetType.image, 1000.0, 100)

    assert asset_repo.count_pending(global_scope=True) == 3
    assert asset_repo.count_pending("cnt-a") == 2
    assert asset_repo.count_pending("cnt-b") == 1
    assert asset_repo.count_pending("other") == 0


def test_count_pending_proxyable_includes_images_and_videos(engine, _session_factory):
    """count_pending_proxyable returns pending image + video assets (ProxyWorker handles both)."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="proxyable-lib",
                name="Proxyable Lib",
                absolute_path="/tmp/proxyable",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("proxyable-lib", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("proxyable-lib", "b.png", AssetType.image, 1000.0, 200)
    asset_repo.upsert_asset("proxyable-lib", "c.mp4", AssetType.video, 1000.0, 300)
    assert asset_repo.count_pending("proxyable-lib") == 3
    assert asset_repo.count_pending_proxyable("proxyable-lib") == 3


def test_get_asset_ids_expecting_proxy_returns_only_relevant_statuses(
    engine, _session_factory
):
    """get_asset_ids_expecting_proxy returns only proxied/completed/extracting/analyzing."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.pending)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-lib",
                name="Repair Lib",
                absolute_path="/tmp/repair",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-lib", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("repair-lib", "b.jpg", AssetType.image, 1000.0, 200)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE rel_path = 'a.jpg'")
        )
        session.execute(
            text("UPDATE asset SET status = 'pending' WHERE rel_path = 'b.jpg'")
        )
        session.commit()
    finally:
        session.close()

    ids = asset_repo.get_asset_ids_expecting_proxy(library_slug="repair-lib")
    assert len(ids) == 1
    assert ids[0][1] == "repair-lib"
    assert ids[0][2] == "image"
    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT id, library_id FROM asset WHERE rel_path = 'a.jpg' AND library_id = 'repair-lib'"
            )
        ).fetchone()
        assert row is not None
        assert (row[0], row[1]) == (ids[0][0], ids[0][1])
    finally:
        session.close()


def test_get_asset_ids_expecting_proxy_respects_library_slug(engine, _session_factory):
    """get_asset_ids_expecting_proxy(library_slug) returns only that library's assets."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-a",
                name="Repair A",
                absolute_path="/tmp/ra",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug="repair-b",
                name="Repair B",
                absolute_path="/tmp/rb",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-a", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("repair-b", "b.jpg", AssetType.image, 1000.0, 100)
    session = _session_factory()
    try:
        session.execute(text("UPDATE asset SET status = 'proxied'"))
        session.commit()
    finally:
        session.close()

    ids_a = asset_repo.get_asset_ids_expecting_proxy(library_slug="repair-a")
    ids_b = asset_repo.get_asset_ids_expecting_proxy(library_slug="repair-b")
    assert len(ids_a) == 1 and ids_a[0][1] == "repair-a"
    assert len(ids_b) == 1 and ids_b[0][1] == "repair-b"


def test_get_asset_ids_expecting_proxy_includes_images_and_videos(
    engine, _session_factory
):
    """get_asset_ids_expecting_proxy returns both image and video assets (ProxyWorker handles both)."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="ext-repair",
                name="Ext Repair",
                absolute_path="/tmp/ext-repair",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("ext-repair", "photo.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("ext-repair", "clip.mp4", AssetType.video, 1000.0, 200)
    session = _session_factory()
    try:
        session.execute(text("UPDATE asset SET status = 'proxied'"))
        session.commit()
    finally:
        session.close()

    ids = asset_repo.get_asset_ids_expecting_proxy(library_slug="ext-repair")
    assert len(ids) == 2
    asset_ids = {r[0]: r[2] for r in ids}
    session = _session_factory()
    try:
        for rel_path, expected_type in [("photo.jpg", "image"), ("clip.mp4", "video")]:
            row = session.execute(
                text(
                    "SELECT id, type::text FROM asset WHERE rel_path = :p AND library_id = 'ext-repair'"
                ),
                {"p": rel_path},
            ).fetchone()
            assert row is not None
            assert row[0] in asset_ids
            assert asset_ids[row[0]] == expected_type
    finally:
        session.close()


@pytest.mark.fast
def test_proxyable_extensions_no_duplicates_disjoint():
    """PROXYABLE_EXTENSIONS_LIST has no duplicates; IMAGE and VIDEO extensions are disjoint."""
    assert len(PROXYABLE_EXTENSIONS_LIST) == len(set(PROXYABLE_EXTENSIONS_LIST))
    image_set = set(IMAGE_EXTENSIONS_LIST)
    video_set = set(VIDEO_EXTENSIONS_LIST)
    assert image_set.isdisjoint(video_set), "IMAGE and VIDEO extensions must be disjoint"


@pytest.mark.fast
def test_cli_proxy_respects_ignore_previews_flag(monkeypatch):
    """proxy CLI wires ignore_previews into ImageProxyWorker(use_previews) based on config."""
    from src import cli

    # Patch session factory and repositories to avoid real DB connections.
    fake_session_factory = MagicMock(name="session_factory")
    monkeypatch.setattr(cli, "_get_session_factory", lambda: fake_session_factory)

    fake_lib_repo = MagicMock(name="LibraryRepository")
    fake_asset_repo = MagicMock(name="AssetRepository")
    fake_worker_repo = MagicMock(name="WorkerRepository")
    fake_sys_repo = MagicMock(name="SystemMetadataRepository")
    monkeypatch.setattr(cli, "LibraryRepository", fake_lib_repo)
    monkeypatch.setattr(cli, "AssetRepository", fake_asset_repo)
    monkeypatch.setattr(cli, "WorkerRepository", fake_worker_repo)
    monkeypatch.setattr(cli, "SystemMetadataRepository", fake_sys_repo)

    fake_asset_repo.return_value.count_pending_proxyable.return_value = 0

    # Config: previews enabled by default.
    cfg = MagicMock()
    cfg.use_raw_previews = True
    monkeypatch.setattr(cli, "get_config", lambda: cfg)

    # Capture how ImageProxyWorker is constructed.
    worker_mock = MagicMock(name="ImageProxyWorker")
    monkeypatch.setattr(cli, "ImageProxyWorker", worker_mock)

    # Case 1: ignore_previews=False -> use_previews=True when config allows previews.
    cli.proxy(
        heartbeat=15.0,
        worker_name="worker-a",
        library_slug=None,
        all_libraries=True,
        verbose=False,
        repair=False,
        once=True,
        ignore_previews=False,
    )
    _, kwargs1 = worker_mock.call_args
    assert kwargs1["use_previews"] is True
    assert kwargs1["library_slug"] is None

    worker_mock.reset_mock()

    # Case 2: ignore_previews=True -> use_previews=False even when config allows previews.
    cli.proxy(
        heartbeat=15.0,
        worker_name="worker-b",
        library_slug=None,
        all_libraries=True,
        verbose=False,
        repair=False,
        once=True,
        ignore_previews=True,
    )
    _, kwargs2 = worker_mock.call_args
    assert kwargs2["use_previews"] is False


def test_proxy_worker_video_720p_pipeline(engine, _session_factory, tmp_path):
    """When a video is processed by VideoProxyWorker, 720p pipeline runs: thumbnail, head-clip, scene indexing; status becomes proxied."""
    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)

    lib_root = tmp_path / "vid-proxy-lib"
    lib_root.mkdir()
    (lib_root / "clip.mp4").write_bytes(b"fake-video")

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="vid-proxy-lib",
                name="Vid Proxy Lib",
                absolute_path=str(lib_root),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("vid-proxy-lib", "clip.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset("vid-proxy-lib", "clip.mp4")
    assert asset is not None
    assert asset.id is not None
    asset_id = asset.id

    from src.video.clip_extractor import FFmpegAttempt

    def mock_extract_video_frame(source, dest, timestamp=0.0):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9")
        return FFmpegAttempt(cmd=["ffmpeg", "-frames:v", "1"], returncode=0, stderr="")

    def mock_extract_head_clip_copy(source, dest, duration=10.0):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"fake-head-clip")
        return FFmpegAttempt(cmd=["ffmpeg", "-c", "copy"], returncode=0, stderr="")

    from src.repository.system_metadata_repo import SystemMetadataRepository
    from src.repository.video_scene_repo import VideoSceneRepository
    from src.repository.worker_repo import WorkerRepository
    from src.workers.video_proxy_worker import VideoProxyWorker

    worker_repo = WorkerRepository(_session_factory)
    system_metadata_repo = SystemMetadataRepository(_session_factory)
    scene_repo = VideoSceneRepository(_session_factory)

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        config_mock = MagicMock()
        config_mock.data_dir = str(data_dir)
        with patch("src.core.config.get_config", return_value=config_mock):
            with patch("src.workers.video_proxy_worker.get_config", return_value=config_mock):
                with patch("src.core.storage.get_config", return_value=config_mock):
                    ok_transcode = FFmpegAttempt(cmd=["ffmpeg", "-c:v", "libx264"], returncode=0, stderr="")
                    with patch(
                        "src.workers.video_proxy_worker.transcode_to_720p_h264_detailed",
                        return_value=[ok_transcode],
                    ):
                        with patch(
                            "src.workers.video_proxy_worker.extract_video_frame_detailed",
                            side_effect=mock_extract_video_frame,
                        ):
                            with patch(
                                "src.workers.video_proxy_worker.extract_head_clip_copy_detailed",
                                side_effect=mock_extract_head_clip_copy,
                            ):
                                with patch("src.workers.video_proxy_worker.run_video_scene_indexing"):
                                    worker = VideoProxyWorker(
                                        worker_id="vid-proxy-worker",
                                        repository=worker_repo,
                                        heartbeat_interval_seconds=15.0,
                                        asset_repo=asset_repo,
                                        system_metadata_repo=system_metadata_repo,
                                        scene_repo=scene_repo,
                                        library_slug="vid-proxy-lib",
                                    )
                                    result = worker.process_task()

        assert result is True
        session = _session_factory()
        try:
            row = session.execute(
                text("SELECT status, video_preview_path FROM asset WHERE id = :id"), {"id": asset_id}
            ).fetchone()
            assert row is not None
            assert row[0] == "proxied"
            assert row[1] == "video_clips/vid-proxy-lib/" + str(asset_id) + "/head_clip.mp4"
        finally:
            session.close()

        shard = asset_id % 1000
        thumb_path = data_dir / "vid-proxy-lib" / "thumbnails" / str(shard) / f"{asset_id}.jpg"
        head_clip_path = data_dir / "video_clips" / "vid-proxy-lib" / str(asset_id) / "head_clip.mp4"
        proxy_path = data_dir / "vid-proxy-lib" / "proxies" / str(shard) / f"{asset_id}.webp"
        assert thumb_path.exists()
        assert head_clip_path.exists()
        assert not proxy_path.exists()


def test_video_proxy_worker_retries_then_poisons_after_threshold(engine, _session_factory, tmp_path):
    """Retryable video-proxy errors are marked failed, then poisoned after retry_count exceeds threshold."""
    from src.video.clip_extractor import FFmpegAttempt
    from src.workers.video_proxy_worker import VideoProxyWorker

    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)

    lib_root = tmp_path / "vid-proxy-retry-lib"
    lib_root.mkdir()
    (lib_root / "clip.mp4").write_bytes(b"fake-video")

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="vid-proxy-retry-lib",
                name="Vid Proxy Retry Lib",
                absolute_path=str(lib_root),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(
        "vid-proxy-retry-lib", "clip.mp4", AssetType.video, 1000.0, 100
    )
    asset = asset_repo.get_asset("vid-proxy-retry-lib", "clip.mp4")
    assert asset is not None
    assert asset.id is not None
    asset_id = asset.id

    from src.repository.system_metadata_repo import SystemMetadataRepository
    from src.repository.video_scene_repo import VideoSceneRepository
    from src.repository.worker_repo import WorkerRepository

    worker_repo = WorkerRepository(_session_factory)
    system_metadata_repo = SystemMetadataRepository(_session_factory)
    scene_repo = VideoSceneRepository(_session_factory)

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        config_mock = MagicMock()
        config_mock.data_dir = str(data_dir)

        ok_transcode = FFmpegAttempt(cmd=["ffmpeg", "-c:v", "libx264"], returncode=0, stderr="")
        bad_frame = FFmpegAttempt(cmd=["ffmpeg", "-frames:v", "1"], returncode=1, stderr="decode error")

        with patch("src.core.config.get_config", return_value=config_mock):
            with patch("src.workers.video_proxy_worker.get_config", return_value=config_mock):
                with patch("src.core.storage.get_config", return_value=config_mock):
                    with patch(
                        "src.workers.video_proxy_worker.transcode_to_720p_h264_detailed",
                        return_value=[ok_transcode],
                    ):
                        with patch(
                            "src.workers.video_proxy_worker.extract_video_frame_detailed",
                            return_value=bad_frame,
                        ):
                            with patch(
                                "src.workers.video_proxy_worker.extract_head_clip_copy_detailed"
                            ) as _unused_head:
                                worker = VideoProxyWorker(
                                    worker_id="vid-proxy-worker",
                                    repository=worker_repo,
                                    heartbeat_interval_seconds=15.0,
                                    asset_repo=asset_repo,
                                    system_metadata_repo=system_metadata_repo,
                                    scene_repo=scene_repo,
                                    library_slug="vid-proxy-retry-lib",
                                )

                                # First attempt: pending -> failed
                                assert worker.process_task() is True
                                session = _session_factory()
                                try:
                                    row = session.execute(
                                        text("SELECT status, retry_count FROM asset WHERE id = :id"),
                                        {"id": asset_id},
                                    ).fetchone()
                                    assert row is not None
                                    assert row[0] == "failed"
                                    assert int(row[1]) == 1
                                finally:
                                    session.close()

                                # Retry until poisoned (retry_count becomes 6 because claim increments first).
                                for _ in range(10):
                                    session = _session_factory()
                                    try:
                                        st = session.execute(
                                            text("SELECT status FROM asset WHERE id = :id"),
                                            {"id": asset_id},
                                        ).scalar()
                                    finally:
                                        session.close()
                                    if st == "poisoned":
                                        break
                                    assert worker.process_task() is True

                                session = _session_factory()
                                try:
                                    row = session.execute(
                                        text("SELECT status, retry_count, error_message FROM asset WHERE id = :id"),
                                        {"id": asset_id},
                                    ).fetchone()
                                    assert row is not None
                                    assert row[0] == "poisoned"
                                    assert int(row[1]) > 5
                                    assert row[2] is not None
                                    assert "Retry limit exceeded" in row[2]
                                finally:
                                    session.close()


def test_video_proxy_worker_poisons_on_resolve_path_value_error(engine, _session_factory, tmp_path):
    """When resolve_path raises ValueError (path traversal), asset is marked poisoned."""
    from src.repository.system_metadata_repo import SystemMetadataRepository
    from src.repository.video_scene_repo import VideoSceneRepository
    from src.repository.worker_repo import WorkerRepository
    from src.workers.video_proxy_worker import VideoProxyWorker

    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)

    lib_root = tmp_path / "vid-path-resolution"
    lib_root.mkdir()
    (lib_root / "clip.mp4").write_bytes(b"fake-video")

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="vid-path-resolution-lib",
                name="Vid Path Resolution Lib",
                absolute_path=str(lib_root),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("vid-path-resolution-lib", "clip.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset("vid-path-resolution-lib", "clip.mp4")
    assert asset is not None
    assert asset.id is not None

    worker_repo = WorkerRepository(_session_factory)
    system_metadata_repo = SystemMetadataRepository(_session_factory)
    scene_repo = VideoSceneRepository(_session_factory)
    config_mock = MagicMock()
    config_mock.data_dir = str(tmp_path)

    with patch("src.core.config.get_config", return_value=config_mock):
        with patch("src.workers.video_proxy_worker.get_config", return_value=config_mock):
            with patch("src.core.storage.get_config", return_value=config_mock):
                with patch("src.workers.video_proxy_worker.resolve_path") as resolve_mock:
                    resolve_mock.side_effect = ValueError("Path escapes library root: '../etc/passwd'")
                    worker = VideoProxyWorker(
                        worker_id="vid-path-resolution-worker",
                        repository=worker_repo,
                        heartbeat_interval_seconds=15.0,
                        asset_repo=asset_repo,
                        system_metadata_repo=system_metadata_repo,
                        scene_repo=scene_repo,
                        library_slug="vid-path-resolution-lib",
                    )
                    result = worker.process_task()

    assert result is True
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE id = :id"),
            {"id": asset.id},
        ).fetchone()
        assert row is not None
        assert row[0] == "poisoned"
    finally:
        session.close()


def test_video_proxy_worker_poisons_on_resolve_path_file_not_found(engine, _session_factory, tmp_path):
    """When resolve_path raises FileNotFoundError, asset is marked poisoned."""
    from src.repository.system_metadata_repo import SystemMetadataRepository
    from src.repository.video_scene_repo import VideoSceneRepository
    from src.repository.worker_repo import WorkerRepository
    from src.workers.video_proxy_worker import VideoProxyWorker

    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)

    lib_root = tmp_path / "vid-path-missing"
    lib_root.mkdir()

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="vid-path-missing-lib",
                name="Vid Path Missing Lib",
                absolute_path=str(lib_root),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("vid-path-missing-lib", "nonexistent.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset("vid-path-missing-lib", "nonexistent.mp4")
    assert asset is not None
    assert asset.id is not None

    worker_repo = WorkerRepository(_session_factory)
    system_metadata_repo = SystemMetadataRepository(_session_factory)
    scene_repo = VideoSceneRepository(_session_factory)
    config_mock = MagicMock()
    config_mock.data_dir = str(tmp_path)

    with patch("src.core.config.get_config", return_value=config_mock):
        with patch("src.workers.video_proxy_worker.get_config", return_value=config_mock):
            with patch("src.core.storage.get_config", return_value=config_mock):
                with patch("src.workers.video_proxy_worker.resolve_path") as resolve_mock:
                    resolve_mock.side_effect = FileNotFoundError("Path does not exist")
                    worker = VideoProxyWorker(
                        worker_id="vid-path-missing-worker",
                        repository=worker_repo,
                        heartbeat_interval_seconds=15.0,
                        asset_repo=asset_repo,
                        system_metadata_repo=system_metadata_repo,
                        scene_repo=scene_repo,
                        library_slug="vid-path-missing-lib",
                    )
                    result = worker.process_task()

    assert result is True
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE id = :id"),
            {"id": asset.id},
        ).fetchone()
        assert row is not None
        assert row[0] == "poisoned"
    finally:
        session.close()


def test_video_proxy_worker_invalidates_stale_segmentation_version(engine, _session_factory, tmp_path):
    """When segmentation_version differs from current PHASH_THRESHOLD/DEBOUNCE_SEC, proxied videos are invalidated (scenes cleared, status=pending)."""
    from src.repository.system_metadata_repo import SystemMetadataRepository
    from src.repository.video_scene_repo import VideoSceneRepository
    from src.repository.worker_repo import WorkerRepository
    from src.video.scene_segmenter import compute_segmentation_version
    from src.workers.video_proxy_worker import VideoProxyWorker

    asset_repo = _create_tables_and_seed(engine, _session_factory)
    _set_all_asset_statuses_to(engine, _session_factory, AssetStatus.completed)

    lib_root = tmp_path / "vid-invalidate-lib"
    lib_root.mkdir()
    (lib_root / "clip.mp4").write_bytes(b"fake-video")

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="vid-invalidate-lib",
                name="Vid Invalidate Lib",
                absolute_path=str(lib_root),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("vid-invalidate-lib", "clip.mp4", AssetType.video, 1000.0, 100)
    asset = asset_repo.get_asset("vid-invalidate-lib", "clip.mp4")
    assert asset is not None
    assert asset.id is not None
    asset_id = asset.id

    # Simulate asset was proxied with old segmentation_version (different from current)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied', segmentation_version = :v WHERE id = :id"),
            {"v": 99999, "id": asset_id},
        )
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:id, 0.0, 5.0, 1.0, 'video_scenes/vid-invalidate-lib/1/0.0_5.0.jpg', 'forced')"
            ),
            {"id": asset_id},
        )
        session.commit()
    finally:
        session.close()

    worker_repo = WorkerRepository(_session_factory)
    system_metadata_repo = SystemMetadataRepository(_session_factory)
    scene_repo = VideoSceneRepository(_session_factory)
    config_mock = MagicMock()
    config_mock.data_dir = str(tmp_path)

    with patch("src.core.config.get_config", return_value=config_mock):
        with patch("src.workers.video_proxy_worker.get_config", return_value=config_mock):
            with patch("src.core.storage.get_config", return_value=config_mock):
                with patch(
                    "src.repository.asset_repo.AssetRepository.claim_asset_by_status",
                    return_value=None,
                ):
                    worker = VideoProxyWorker(
                        worker_id="vid-invalidate-worker",
                        repository=worker_repo,
                        heartbeat_interval_seconds=15.0,
                        asset_repo=asset_repo,
                        system_metadata_repo=system_metadata_repo,
                        scene_repo=scene_repo,
                        library_slug="vid-invalidate-lib",
                    )
                    result = worker.process_task()

    assert result is False
    current_version = compute_segmentation_version()
    assert current_version != 99999

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status, segmentation_version FROM asset WHERE id = :id"),
            {"id": asset_id},
        ).fetchone()
        assert row is not None
        assert row[0] == "pending"
        assert row[1] == 99999
        count = session.execute(
            text("SELECT COUNT(*) FROM video_scenes WHERE asset_id = :id"), {"id": asset_id}
        ).scalar()
        assert count == 0
    finally:
        session.close()
