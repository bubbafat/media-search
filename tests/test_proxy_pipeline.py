"""Tests for proxy pipeline: claim_asset_by_status and update_asset_status (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import Asset, AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository


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
