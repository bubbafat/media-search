"""Scanner worker: discovery, idempotency, dirty detection, signal respect (testcontainers Postgres)."""

import os
import time
import unittest.mock

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.core.config import Settings
from src.models.entities import AssetStatus, Library, ScanStatus, SystemMetadata, WorkerCommand, WorkerState
from src.models.entities import WorkerStatus as WorkerStatusEntity
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.scanner import ScannerWorker


def _create_tables_and_repos(engine, session_factory):
    """Create all tables, seed schema_version if missing (idempotent), return (worker_repo, asset_repo, system_metadata_repo)."""
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
        WorkerRepository(session_factory),
        AssetRepository(session_factory),
        SystemMetadataRepository(session_factory),
    )


def _persist_library(
    session_factory,
    slug: str,
    absolute_path: str,
    scan_status: ScanStatus = ScanStatus.full_scan_requested,
):
    """Insert or update a library and commit so the worker can see it."""
    session = session_factory()
    try:
        existing = session.get(Library, slug)
        if existing is not None:
            existing.scan_status = scan_status
            existing.is_active = True
            existing.absolute_path = absolute_path
        else:
            session.add(
                Library(
                    slug=slug,
                    name=f"Test {slug}",
                    absolute_path=absolute_path,
                    is_active=True,
                    scan_status=scan_status,
                    sampling_limit=100,
                )
            )
        session.commit()
    finally:
        session.close()


def _get_assets(session_factory, library_id: str):
    """Return list of (rel_path, status, mtime, size) for a library."""
    session = session_factory()
    try:
        rows = session.execute(
            text(
                "SELECT rel_path, status, mtime, size FROM asset WHERE library_id = :lib ORDER BY rel_path"
            ),
            {"lib": library_id},
        ).fetchall()
        return [{"rel_path": r[0], "status": r[1], "mtime": r[2], "size": r[3]} for r in rows]
    finally:
        session.close()


def _set_worker_command(session_factory, worker_id: str, command: str):
    """Set worker command (e.g. 'pause') in DB."""
    session = session_factory()
    try:
        row = session.get(WorkerStatusEntity, worker_id)
        if row is not None:
            row.command = WorkerCommand(command)
            session.commit()
    finally:
        session.close()


def _get_library_scan_status(session_factory, slug: str) -> str | None:
    session = session_factory()
    try:
        row = session.get(Library, slug)
        return row.scan_status.value if row else None
    finally:
        session.close()


def _get_worker_state(session_factory, worker_id: str) -> str | None:
    session = session_factory()
    try:
        row = session.get(WorkerStatusEntity, worker_id)
        return row.state.value if row else None
    finally:
        session.close()


@pytest.fixture
def scanner_worker(engine, _session_factory, run_worker, tmp_path, request):
    """Create tables, persist a library, patch config with tmp_path as library root. Yields (worker, library_slug, run_worker, session_factory). Uses unique library_slug per test for isolation."""
    worker_repo, asset_repo, system_metadata_repo = _create_tables_and_repos(engine, _session_factory)
    # Unique library per test so assets from other tests don't leak
    library_slug = f"test-lib-{request.node.name}"
    _persist_library(_session_factory, library_slug, absolute_path=str(tmp_path))

    settings = Settings(
        database_url="",
        library_roots={library_slug: str(tmp_path)},
        worker_id="scanner-test",
    )
    # Patch get_config where it is used (path_resolver) so the worker thread sees it
    with unittest.mock.patch("src.core.path_resolver.get_config", return_value=settings):
        worker = ScannerWorker(
            "scanner-test",
            worker_repo,
            heartbeat_interval_seconds=60.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_metadata_repo,
        )
        yield worker, library_slug, run_worker, _session_factory


def test_new_discovery(scanner_worker, tmp_path):
    """Adding a file to the test directory results in a pending asset in the DB."""
    worker, library_slug, run_worker, session_factory = scanner_worker
    (tmp_path / "video.mp4").write_bytes(b"fake-mp4-content")

    with run_worker(worker):
        time.sleep(1.0)

    assets = _get_assets(session_factory, library_slug)
    assert len(assets) == 1
    assert assets[0]["rel_path"] == "video.mp4"
    assert assets[0]["status"] == AssetStatus.pending.value
    assert assets[0]["size"] == len(b"fake-mp4-content")
    assert assets[0]["mtime"] > 0


def test_fast_scan_requested_library_is_claimed(engine, _session_factory, run_worker, tmp_path):
    """A library with scan_status=fast_scan_requested is claimed and scanned like full_scan_requested."""
    worker_repo, asset_repo, system_metadata_repo = _create_tables_and_repos(engine, _session_factory)
    library_slug = "fast-scan-lib"
    _persist_library(
        _session_factory,
        library_slug,
        str(tmp_path),
        scan_status=ScanStatus.fast_scan_requested,
    )

    settings = Settings(
        database_url="",
        library_roots={library_slug: str(tmp_path)},
        worker_id="scanner-fast-test",
    )
    (tmp_path / "photo.jpg").write_bytes(b"jpeg")
    with unittest.mock.patch("src.core.path_resolver.get_config", return_value=settings):
        worker = ScannerWorker(
            "scanner-fast-test",
            worker_repo,
            heartbeat_interval_seconds=60.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_metadata_repo,
        )
        with run_worker(worker):
            time.sleep(1.0)

    assets = _get_assets(_session_factory, library_slug)
    assert len(assets) == 1, "Scanner should claim fast_scan_requested library and discover file"
    assert assets[0]["rel_path"] == "photo.jpg"


def test_scan_directory_tree_writes_rel_paths(scanner_worker, tmp_path):
    """Scanning a directory tree discovers files at all depths and writes correct rel_paths to the DB."""
    worker, library_slug, run_worker, session_factory = scanner_worker
    (tmp_path / "root.png").write_bytes(b"png")
    (tmp_path / "movies").mkdir()
    (tmp_path / "movies" / "a.mp4").write_bytes(b"video")
    (tmp_path / "photos" / "vacation").mkdir(parents=True)
    (tmp_path / "photos" / "vacation" / "beach.jpg").write_bytes(b"jpeg")

    with run_worker(worker):
        time.sleep(1.0)

    assets = _get_assets(session_factory, library_slug)
    assert len(assets) == 3
    expected_rel_paths = ["movies/a.mp4", "photos/vacation/beach.jpg", "root.png"]
    assert [a["rel_path"] for a in assets] == expected_rel_paths
    for a in assets:
        assert a["status"] == AssetStatus.pending.value
        assert a["size"] > 0
        assert a["mtime"] > 0


def test_scanner_discovers_raw_and_dng_extensions(scanner_worker, tmp_path):
    """Scanner discovers RAW (e.g. Fuji .raf) and DNG/TIFF as image assets."""
    worker, library_slug, run_worker, session_factory = scanner_worker
    (tmp_path / "fuji.raf").write_bytes(b"fake-raw")
    (tmp_path / "export.dng").write_bytes(b"fake-dng")
    (tmp_path / "scan.tiff").write_bytes(b"fake-tiff")

    with run_worker(worker):
        time.sleep(1.0)

    assets = _get_assets(session_factory, library_slug)
    assert len(assets) == 3
    rel_paths = sorted(a["rel_path"] for a in assets)
    assert rel_paths == ["export.dng", "fuji.raf", "scan.tiff"]
    for a in assets:
        assert a["status"] == AssetStatus.pending.value


def test_idempotency(scanner_worker, tmp_path):
    """Running the scanner twice on an unchanged file does NOT reset its status."""
    worker, library_slug, run_worker, session_factory = scanner_worker
    (tmp_path / "image.png").write_bytes(b"x")

    with run_worker(worker):
        time.sleep(0.8)
    assets_after_first = _get_assets(session_factory, library_slug)
    assert len(assets_after_first) == 1
    assert assets_after_first[0]["status"] == AssetStatus.pending.value

    # Set library back to full_scan_requested so the scanner will pick it again
    session = session_factory()
    try:
        lib = session.get(Library, library_slug)
        lib.scan_status = ScanStatus.full_scan_requested
        session.commit()
    finally:
        session.close()

    worker.should_exit = False
    with run_worker(worker):
        time.sleep(0.8)

    assets_after_second = _get_assets(session_factory, library_slug)
    assert len(assets_after_second) == 1
    # Status should still be pending (not reset); idempotent upsert did not change status
    assert assets_after_second[0]["status"] == AssetStatus.pending.value


def test_dirty_detection(scanner_worker, tmp_path):
    """Updating a file's mtime causes the scanner to reset its status to pending."""
    worker, library_slug, run_worker, session_factory = scanner_worker
    f = tmp_path / "a.mkv"
    f.write_bytes(b"video")
    with run_worker(worker):
        time.sleep(0.8)
    assets = _get_assets(session_factory, library_slug)
    assert len(assets) == 1
    # Simulate "completed" so we can see the reset
    session = session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'completed' WHERE library_id = :lib AND rel_path = 'a.mkv'"
            ),
            {"lib": library_slug},
        )
        session.commit()
    finally:
        session.close()

    # Touch file to change mtime
    os.utime(f, (time.time(), time.time() + 10))

    # Set library to full_scan_requested again
    session = session_factory()
    try:
        lib = session.get(Library, library_slug)
        lib.scan_status = ScanStatus.full_scan_requested
        session.commit()
    finally:
        session.close()

    worker.should_exit = False
    with run_worker(worker):
        time.sleep(0.8)

    assets_after = _get_assets(session_factory, library_slug)
    assert len(assets_after) == 1
    assert assets_after[0]["status"] == AssetStatus.pending.value


def test_signal_respect_pause(engine, _session_factory, run_worker, tmp_path):
    """Issue a pause command mid-scan; verify the scanner stops and waits."""
    worker_repo, asset_repo, system_metadata_repo = _create_tables_and_repos(engine, _session_factory)
    library_slug = "pause-lib"
    _persist_library(_session_factory, library_slug, str(tmp_path))
    # Create enough files to keep the scanner busy
    for i in range(120):
        (tmp_path / f"f{i:03d}.jpg").write_bytes(b"x")

    settings = Settings(
        database_url="",
        library_roots={library_slug: str(tmp_path)},
        worker_id="scanner-pause-test",
    )
    with unittest.mock.patch("src.core.path_resolver.get_config", return_value=settings):
        worker = ScannerWorker(
            "scanner-pause-test",
            worker_repo,
            heartbeat_interval_seconds=60.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_metadata_repo,
            idle_poll_interval_seconds=0.2,
        )
        worker_repo.register_worker("scanner-pause-test", WorkerState.idle)

    with run_worker(worker):
        time.sleep(0.5)
        _set_worker_command(_session_factory, "scanner-pause-test", "pause")
        time.sleep(1.5)
        state = _get_worker_state(_session_factory, "scanner-pause-test")
        assert state == WorkerState.paused.value
        scan_status = _get_library_scan_status(_session_factory, library_slug)
        assert scan_status == ScanStatus.idle.value


def test_scan_with_progress_interval_uses_smaller_interval(engine, _session_factory, tmp_path):
    """Scanner with progress_interval=10 reports correct file count after one-shot scan."""
    worker_repo, asset_repo, system_metadata_repo = _create_tables_and_repos(engine, _session_factory)
    library_slug = "progress-lib"
    _persist_library(_session_factory, library_slug, str(tmp_path))
    for i in range(25):
        (tmp_path / f"img{i:02d}.jpg").write_bytes(b"jpeg")

    settings = Settings(
        database_url="",
        library_roots={library_slug: str(tmp_path)},
        worker_id="scanner-progress-test",
    )
    with unittest.mock.patch("src.core.path_resolver.get_config", return_value=settings):
        worker = ScannerWorker(
            "scanner-progress-test",
            worker_repo,
            heartbeat_interval_seconds=60.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_metadata_repo,
            progress_interval=10,
        )
    assert worker._stats_interval == 10
    worker_repo.register_worker("scanner-progress-test", WorkerState.idle)

    worker.process_task(library_slug=library_slug)

    assert worker._last_files_processed == 25
    assets = _get_assets(_session_factory, library_slug)
    assert len(assets) == 25
