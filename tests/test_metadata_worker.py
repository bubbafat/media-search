"""Integration tests for MetadataWorker and EXIF metadata pipeline (Postgres via testcontainers)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.metadata import exif_adapter
from src.metadata.normalization import normalize_media_metadata
from src.models.entities import Asset, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.metadata_worker import MetadataWorker

pytestmark = [pytest.mark.slow]


def _create_tables_and_repos(engine, session_factory):
    """Create tables and seed schema_version; return asset_repo, worker_repo, system_metadata_repo."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return (
        AssetRepository(session_factory),
        WorkerRepository(session_factory),
        SystemMetadataRepository(session_factory),
    )


def test_claim_assets_for_exif_metadata_only_null_status(engine, _session_factory) -> None:
    asset_repo, _, _ = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="meta-lib",
                name="Meta",
                absolute_path="/tmp/meta-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Create two assets: one eligible (metadata_status NULL), one already exif_done.
    asset_repo.upsert_asset("meta-lib", "a.jpg", AssetType.image, 0.0, 100)
    asset_repo.upsert_asset("meta-lib", "b.jpg", AssetType.image, 0.0, 100)

    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET metadata_status = 'exif_done' "
                "WHERE library_id = 'meta-lib' AND rel_path = 'b.jpg'"
            )
        )
        session.commit()
    finally:
        session.close()

    claimed = asset_repo.claim_assets_for_exif_metadata(batch_size=10, library_slug="meta-lib")
    assert len(claimed) == 1

    session = _session_factory()
    try:
        rows = session.execute(
            text(
                "SELECT rel_path, metadata_status FROM asset WHERE library_id = 'meta-lib' ORDER BY rel_path"
            )
        ).fetchall()
        assert rows[0][0] == "a.jpg"
        assert rows[0][1] == "processing"
        assert rows[1][0] == "b.jpg"
        assert rows[1][1] == "exif_done"
    finally:
        session.close()


def test_write_exif_metadata_persists_fields(engine, _session_factory) -> None:
    asset_repo, _, _ = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="meta-write-lib",
                name="Meta Write",
                absolute_path="/tmp/meta-write",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("meta-write-lib", "c.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        asset_id = session.execute(
            text(
                "SELECT id FROM asset WHERE library_id = 'meta-write-lib' AND rel_path = 'c.jpg'"
            )
        ).scalar_one()
    finally:
        session.close()

    raw_exif: Dict[str, Any] = {"Make": "Canon", "Model": "R5"}
    media_metadata = {"metadata_version": 1, "camera_make": "Canon", "camera_model": "R5"}
    asset_repo.write_exif_metadata(asset_id, raw_exif, media_metadata)

    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT raw_exif, media_metadata, metadata_status FROM asset WHERE id = :id"
            ),
            {"id": asset_id},
        ).fetchone()
        assert row is not None
        assert row[0] == raw_exif
        assert row[1] == media_metadata
        assert row[2] == "exif_done"
    finally:
        session.close()


def test_metadata_worker_process_exif_batch_end_to_end(tmp_path, engine, _session_factory, monkeypatch):
    """_process_exif_batch claims assets, reads EXIF (mocked), normalizes, and writes metadata."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )

    # Library rooted at a temporary directory.
    source_dir = tmp_path / "meta-source"
    source_dir.mkdir(parents=True, exist_ok=True)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="meta-endtoend",
                name="Meta E2E",
                absolute_path=str(source_dir),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Create a dummy file and asset row.
    rel_path = "photo.jpg"
    source_path = source_dir / rel_path
    source_path.write_bytes(b"fake-jpeg-data")
    asset_repo.upsert_asset("meta-endtoend", rel_path, AssetType.image, 1000.0, source_path.stat().st_size)

    # Ensure metadata_status is NULL initially.
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET metadata_status = NULL "
                "WHERE library_id = 'meta-endtoend' AND rel_path = :rel_path"
            ),
            {"rel_path": rel_path},
        )
        session.commit()
    finally:
        session.close()

    # Monkeypatch exif_adapter.read_metadata to avoid calling real exiftool.
    fake_raw_exif = {"Make": "Canon", "Model": "R5", "ExifImageWidth": 4000, "ExifImageHeight": 3000}

    def _fake_read_metadata(path: Path) -> Dict[str, Any]:
        assert path == source_path
        return dict(fake_raw_exif)

    monkeypatch.setattr(exif_adapter, "read_metadata", _fake_read_metadata)

    worker = MetadataWorker(
        worker_id="metadata-test-worker",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        phase="exif",
        batch_size=16,
        library_slug="meta-endtoend",
    )

    processed = worker._process_exif_batch()
    assert processed is True

    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT raw_exif, media_metadata, metadata_status FROM asset "
                "WHERE library_id = 'meta-endtoend' AND rel_path = :rel_path"
            ),
            {"rel_path": rel_path},
        ).fetchone()
        assert row is not None
        stored_exif, stored_meta, status = row
        assert status == "exif_done"
        assert stored_exif == fake_raw_exif
        assert stored_meta is not None
        assert stored_meta.get("camera_make") == "Canon"
        assert stored_meta.get("camera_model") == "R5"
    finally:
        session.close()


def test_missing_source_file_is_logged_and_left_processing(tmp_path, engine, _session_factory):
    """Nonexistent source path should be logged and left in 'processing' state."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )

    source_dir = tmp_path / "missing-source"
    source_dir.mkdir(parents=True, exist_ok=True)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="meta-missing",
                name="Meta Missing",
                absolute_path=str(source_dir),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Asset row whose file does not exist on disk.
    rel_path = "does-not-exist.jpg"
    asset_repo.upsert_asset("meta-missing", rel_path, AssetType.image, 1000.0, 100)

    # Ensure metadata_status is NULL so claim will pick it up.
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET metadata_status = NULL "
                "WHERE library_id = 'meta-missing' AND rel_path = :rel_path"
            ),
            {"rel_path": rel_path},
        )
        session.commit()
    finally:
        session.close()

    worker = MetadataWorker(
        worker_id="metadata-missing-worker",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        phase="exif",
        batch_size=16,
        library_slug="meta-missing",
    )

    processed = worker._process_exif_batch()
    assert processed is True

    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT raw_exif, media_metadata, metadata_status FROM asset "
                "WHERE library_id = 'meta-missing' AND rel_path = :rel_path"
            ),
            {"rel_path": rel_path},
        ).fetchone()
        assert row is not None
        raw_exif, media_metadata, status = row
        # Asset should remain in 'processing' and not have EXIF/media_metadata written.
        assert status == "processing"
        assert raw_exif is None
        assert media_metadata is None
    finally:
        session.close()


def test_reset_stuck_cli_command_resets_processing_assets(engine, _session_factory, monkeypatch):
    """metadata reset-stuck resets processing assets older than the threshold back to NULL."""
    asset_repo, _, _ = _create_tables_and_repos(engine, _session_factory)

    session = _session_factory()
    try:
        session.add(
            Library(
                slug="meta-reset",
                name="Meta Reset",
                absolute_path="/tmp/meta-reset",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Old asset (mtime very small) and new asset (mtime now).
    import time

    asset_repo.upsert_asset("meta-reset", "old.jpg", AssetType.image, 0.0, 100)
    # New asset uses a recent mtime so it will not be considered "stuck".
    asset_repo.upsert_asset(
        "meta-reset",
        "new.jpg",
        AssetType.image,
        float(time.time()),
        100,
    )

    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET metadata_status = 'processing' "
                "WHERE library_id = 'meta-reset'"
            )
        )
        session.commit()
    finally:
        session.close()

    from src import cli as cli_module

    # Use a very small threshold so only the "old" asset qualifies based on mtime.
    cli_module.metadata_reset_stuck(older_than="1s")

    session = _session_factory()
    try:
        rows = session.execute(
            text(
                "SELECT rel_path, metadata_status FROM asset "
                "WHERE library_id = 'meta-reset' ORDER BY rel_path"
            )
        ).fetchall()
        assert rows[0][0] == "new.jpg"
        assert rows[0][1] == "processing"
        assert rows[1][0] == "old.jpg"
        assert rows[1][1] is None
    finally:
        session.close()

