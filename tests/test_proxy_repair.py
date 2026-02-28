"""Tests for proxy --repair: _run_repair_pass and repair mode (testcontainers Postgres + temp data_dir)."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image
from sqlalchemy import text
from sqlmodel import SQLModel
from typer.testing import CliRunner

from src.cli import app
from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.proxy_worker import ImageProxyWorker
from src.workers.video_proxy_worker import VideoProxyWorker

pytestmark = [pytest.mark.slow]


def _create_tables_and_repos(engine, session_factory):
    """Create tables, seed schema_version, return asset_repo, worker_repo, system_metadata_repo."""
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


def test_repair_sets_pending_when_proxy_or_thumbnail_missing(engine, _session_factory):
    """Repair pass sets status to pending when proxy/thumbnail files are missing."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-missing-lib",
                name="Repair Missing",
                absolute_path="/tmp/repair-missing",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-missing-lib", "photo.jpg", AssetType.image, 1000.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE library_id = 'repair-missing-lib'")
        )
        session.commit()
    finally:
        session.close()

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(data_dir)
            worker = ImageProxyWorker(
                worker_id="repair-worker",
                repository=worker_repo,
                heartbeat_interval_seconds=15.0,
                asset_repo=asset_repo,
                system_metadata_repo=system_metadata_repo,
                library_slug="repair-missing-lib",
                repair=True,
            )
            worker._run_repair_pass()

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE library_id = 'repair-missing-lib'")
        ).fetchone()
        assert row is not None
        assert row[0] == "pending"
    finally:
        session.close()


def test_repair_leaves_status_when_both_files_exist(engine, _session_factory):
    """Repair pass leaves status unchanged when both proxy and thumbnail exist."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-ok-lib",
                name="Repair OK",
                absolute_path="/tmp/repair-ok",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-ok-lib", "photo.jpg", AssetType.image, 1000.0, 100)
    assets = asset_repo.get_assets_by_library("repair-ok-lib", limit=1)
    assert len(assets) == 1
    asset_id = assets[0].id
    assert asset_id is not None

    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE library_id = 'repair-ok-lib'")
        )
        session.commit()
    finally:
        session.close()

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(data_dir)
            store = LocalMediaStore()
            img = Image.new("RGB", (100, 100), color="blue")
            store.save_thumbnail("repair-ok-lib", asset_id, img)
            store.save_proxy("repair-ok-lib", asset_id, img)

            worker = ImageProxyWorker(
                worker_id="repair-worker",
                repository=worker_repo,
                heartbeat_interval_seconds=15.0,
                asset_repo=asset_repo,
                system_metadata_repo=system_metadata_repo,
                library_slug="repair-ok-lib",
                repair=True,
            )
            worker._run_repair_pass()

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE library_id = 'repair-ok-lib'")
        ).fetchone()
        assert row is not None
        assert row[0] == "proxied"
    finally:
        session.close()


def test_repair_respects_library_slug(engine, _session_factory):
    """Repair with --library only resets assets in that library."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-respect-a",
                name="Repair Respect A",
                absolute_path="/tmp/repair-respect-a",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.add(
            Library(
                slug="repair-respect-b",
                name="Repair Respect B",
                absolute_path="/tmp/repair-respect-b",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-respect-a", "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset("repair-respect-b", "b.jpg", AssetType.image, 1000.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'proxied' WHERE library_id IN ('repair-respect-a', 'repair-respect-b')"
            )
        )
        session.commit()
    finally:
        session.close()

    assets_a = asset_repo.get_assets_by_library("repair-respect-a", limit=1)
    assets_b = asset_repo.get_assets_by_library("repair-respect-b", limit=1)
    id_a, id_b = assets_a[0].id, assets_b[0].id
    assert id_a is not None and id_b is not None

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(data_dir)
            store = LocalMediaStore()
            img = Image.new("RGB", (100, 100), color="green")
            store.save_thumbnail("repair-respect-b", id_b, img)
            store.save_proxy("repair-respect-b", id_b, img)
            # repair-respect-a has no files; repair-respect-b has both

            worker = ImageProxyWorker(
                worker_id="repair-worker",
                repository=worker_repo,
                heartbeat_interval_seconds=15.0,
                asset_repo=asset_repo,
                system_metadata_repo=system_metadata_repo,
                library_slug="repair-respect-a",
                repair=True,
            )
            worker._run_repair_pass()

    session = _session_factory()
    try:
        row_a = session.execute(
            text("SELECT status FROM asset WHERE id = :id"), {"id": id_a}
        ).fetchone()
        row_b = session.execute(
            text("SELECT status FROM asset WHERE id = :id"), {"id": id_b}
        ).fetchone()
        assert row_a is not None and row_a[0] == "pending"
        assert row_b is not None and row_b[0] == "proxied"
    finally:
        session.close()


def test_repair_sets_pending_when_proxy_or_thumbnail_0_byte(engine, _session_factory):
    """Repair pass sets status to pending when proxy or thumbnail file is 0-byte."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-0byte-lib",
                name="Repair 0-byte",
                absolute_path="/tmp/repair-0byte",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-0byte-lib", "photo.jpg", AssetType.image, 1000.0, 100)
    assets = asset_repo.get_assets_by_library("repair-0byte-lib", limit=1)
    assert len(assets) == 1
    asset_id = assets[0].id
    assert asset_id is not None

    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE library_id = 'repair-0byte-lib'")
        )
        session.commit()
    finally:
        session.close()

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(data_dir)
            store = LocalMediaStore()
            # Create 0-byte proxy and thumbnail - repair should treat as missing
            proxy_path = data_dir / "repair-0byte-lib" / "proxies" / str(asset_id % 1000) / f"{asset_id}.webp"
            thumb_path = data_dir / "repair-0byte-lib" / "thumbnails" / str(asset_id % 1000) / f"{asset_id}.jpg"
            proxy_path.parent.mkdir(parents=True, exist_ok=True)
            thumb_path.parent.mkdir(parents=True, exist_ok=True)
            proxy_path.write_bytes(b"")
            thumb_path.write_bytes(b"")

            worker = ImageProxyWorker(
                worker_id="repair-worker",
                repository=worker_repo,
                heartbeat_interval_seconds=15.0,
                asset_repo=asset_repo,
                system_metadata_repo=system_metadata_repo,
                library_slug="repair-0byte-lib",
                repair=True,
            )
            worker._run_repair_pass()

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE library_id = 'repair-0byte-lib'")
        ).fetchone()
        assert row is not None
        assert row[0] == "pending"
    finally:
        session.close()


def test_video_proxy_repair_sets_pending_when_head_clip_0_byte(engine, _session_factory):
    """Video proxy repair sets status to pending when head_clip or thumbnail is 0-byte."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    scene_repo = VideoSceneRepository(_session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-video-0byte-lib",
                name="Repair Video 0-byte",
                absolute_path="/tmp/repair-video-0byte",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-video-0byte-lib", "video.mp4", AssetType.video, 1000.0, 100)
    assets = asset_repo.get_assets_by_library("repair-video-0byte-lib", limit=1)
    assert len(assets) == 1
    asset_id = assets[0].id
    assert asset_id is not None

    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'proxied' WHERE library_id = 'repair-video-0byte-lib'"
            )
        )
        session.commit()
    finally:
        session.close()

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as storage_cfg:
            with patch("src.workers.video_proxy_worker.get_config") as worker_cfg:
                m = MagicMock()
                m.data_dir = str(data_dir)
                storage_cfg.return_value = m
                worker_cfg.return_value = m

                shard = asset_id % 1000
                thumb_path = data_dir / "repair-video-0byte-lib" / "thumbnails" / str(shard) / f"{asset_id}.jpg"
                head_path = data_dir / "video_clips" / "repair-video-0byte-lib" / str(asset_id) / "head_clip.mp4"
                thumb_path.parent.mkdir(parents=True, exist_ok=True)
                head_path.parent.mkdir(parents=True, exist_ok=True)
                thumb_path.write_bytes(b"")
                head_path.write_bytes(b"")

                worker = VideoProxyWorker(
                    worker_id="repair-video-worker",
                    repository=worker_repo,
                    heartbeat_interval_seconds=15.0,
                    asset_repo=asset_repo,
                    system_metadata_repo=system_metadata_repo,
                    scene_repo=scene_repo,
                    library_slug="repair-video-0byte-lib",
                    repair=True,
                )
                worker._run_repair_pass()

    session = _session_factory()
    try:
        row = session.execute(
            text(
                "SELECT status FROM asset WHERE library_id = 'repair-video-0byte-lib'"
            )
        ).fetchone()
        assert row is not None
        assert row[0] == "pending"
    finally:
        session.close()


def test_cli_proxy_repair_passes_repair_true(engine, _session_factory):
    """proxy --repair invokes ImageProxyWorker with repair=True."""
    _create_tables_and_repos(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="cli-repair-unique-lib",
                name="CLI Repair Unique",
                absolute_path="/tmp/cli-repair-unique",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    with patch("src.cli._get_session_factory", return_value=_session_factory):
        with patch("src.cli.ImageProxyWorker") as MockProxyWorker:
            MockProxyWorker.return_value.run.side_effect = None
            runner = CliRunner()
            result = runner.invoke(
                app,
                ["proxy", "--repair", "--library", "cli-repair-unique-lib"],
            )
    assert result.exit_code == 0
    MockProxyWorker.assert_called_once()
    call_kwargs = MockProxyWorker.call_args[1]
    assert call_kwargs["repair"] is True
    assert call_kwargs["library_slug"] == "cli-repair-unique-lib"
