"""Tests for proxy pipeline: claim_asset_by_status and update_asset_status (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AIModel, Asset, AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository

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
        "worker-1", AssetStatus.pending, [".jpg", ".jpeg", ".png"]
    )
    assert claimed is not None
    assert claimed.id is not None
    assert claimed.status == AssetStatus.processing
    assert claimed.worker_id == "worker-1"
    assert claimed.library is not None
    assert claimed.library.slug == "proxy-lib"
    assert claimed.library.absolute_path == "/tmp/proxy-lib"
    assert claimed.rel_path == "photo.jpg"


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
        "worker-1", AssetStatus.pending, [".jpg"]
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
        "worker-1", AssetStatus.pending, [".jpg", ".png"]
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
        "worker-1", AssetStatus.pending, [".jpg"]
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
        "ai-worker-1", AssetStatus.pending, [".jpg"]
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
        "worker-1", AssetStatus.pending, [".jpg"]
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

    assert asset_repo.count_pending() == 0
    asset_repo.upsert_asset("cnt-lib", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("cnt-lib", "b.jpg", AssetType.image, 1000.0, 200)
    assert asset_repo.count_pending() == 2
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

    assert asset_repo.count_pending() == 3
    assert asset_repo.count_pending("cnt-a") == 2
    assert asset_repo.count_pending("cnt-b") == 1
    assert asset_repo.count_pending("other") == 0


def test_count_pending_proxyable_excludes_video(engine, _session_factory):
    """count_pending_proxyable returns only pending image assets; videos are excluded."""
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
    assert asset_repo.count_pending_proxyable("proxyable-lib") == 2


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
    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT id, library_id FROM asset WHERE rel_path = 'a.jpg' AND library_id = 'repair-lib'"
            )
        ).fetchone()
        assert row is not None
        assert (row[0], row[1]) == ids[0]
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


def test_get_asset_ids_expecting_proxy_filters_by_image_extension(
    engine, _session_factory
):
    """get_asset_ids_expecting_proxy excludes video/non-image extensions (e.g. .mp4)."""
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
    assert len(ids) == 1
    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT id FROM asset WHERE rel_path = 'photo.jpg' AND library_id = 'ext-repair'"
            )
        ).fetchone()
        assert row is not None
        assert ids[0][0] == row[0]
    finally:
        session.close()
