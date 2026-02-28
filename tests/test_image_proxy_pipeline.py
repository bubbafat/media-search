"""Integration tests for ImageProxyWorker proxy/thumbnail pipeline (testcontainers Postgres)."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from PIL import Image
from sqlalchemy import text
from sqlmodel import SQLModel

from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.proxy_worker import ImageProxyWorker

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


def test_image_proxy_worker_generates_cascaded_proxy_and_thumbnail(engine, _session_factory, tmp_path):
    """ImageProxyWorker processes a large image and writes proxy (<=768) and thumbnail (<=320)."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )

    # Create library pointing at a temporary source directory.
    source_dir = tmp_path / "source_large"
    source_dir.mkdir(parents=True, exist_ok=True)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="pipeline-large-lib",
                name="Pipeline Large",
                absolute_path=str(source_dir),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    # Create a large source image on disk.
    rel_path = "photo-large.jpg"
    source_path = source_dir / rel_path
    Image.new("RGB", (4000, 3000), color="red").save(source_path, "JPEG")

    asset_repo.upsert_asset(
        "pipeline-large-lib",
        rel_path,
        AssetType.image,
        1000.0,
        source_path.stat().st_size,
    )

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(data_dir)
            worker = ImageProxyWorker(
                worker_id="pipeline-large-worker",
                repository=worker_repo,
                heartbeat_interval_seconds=15.0,
                asset_repo=asset_repo,
                system_metadata_repo=system_metadata_repo,
                library_slug="pipeline-large-lib",
            )
            processed = worker.process_task()
            assert processed is True

            assets = asset_repo.get_assets_by_library("pipeline-large-lib", limit=1)
            assert len(assets) == 1
            asset = assets[0]
            assert asset.status == AssetStatus.proxied
            assert asset.id is not None

            proxy_path = worker.storage.get_proxy_path("pipeline-large-lib", asset.id)
            thumb_path = worker.storage.get_thumbnail_path("pipeline-large-lib", asset.id)
            assert proxy_path.exists()
            assert thumb_path.exists()

            with Image.open(proxy_path) as proxy_im:
                proxy_size = proxy_im.size
                assert max(proxy_size) == 768

            with Image.open(thumb_path) as thumb_im:
                thumb_size = thumb_im.size
                assert max(thumb_size) == 320
                assert thumb_size[0] <= proxy_size[0]
                assert thumb_size[1] <= proxy_size[1]


def test_image_proxy_worker_does_not_upscale_small_images(engine, _session_factory, tmp_path):
    """ImageProxyWorker preserves native resolution for icon-sized images for both proxy and thumbnail."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )

    source_dir = tmp_path / "source_small"
    source_dir.mkdir(parents=True, exist_ok=True)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="pipeline-small-lib",
                name="Pipeline Small",
                absolute_path=str(source_dir),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    rel_path = "icon.png"
    source_path = source_dir / rel_path
    Image.new("RGB", (32, 32), color="green").save(source_path, "PNG")

    asset_repo.upsert_asset(
        "pipeline-small-lib",
        rel_path,
        AssetType.image,
        1000.0,
        source_path.stat().st_size,
    )

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as m:
            m.return_value.data_dir = str(data_dir)
            worker = ImageProxyWorker(
                worker_id="pipeline-small-worker",
                repository=worker_repo,
                heartbeat_interval_seconds=15.0,
                asset_repo=asset_repo,
                system_metadata_repo=system_metadata_repo,
                library_slug="pipeline-small-lib",
            )
            processed = worker.process_task()
            assert processed is True

            assets = asset_repo.get_assets_by_library("pipeline-small-lib", limit=1)
            assert len(assets) == 1
            asset = assets[0]
            assert asset.status == AssetStatus.proxied
            assert asset.id is not None

            proxy_path = worker.storage.get_proxy_path("pipeline-small-lib", asset.id)
            thumb_path = worker.storage.get_thumbnail_path("pipeline-small-lib", asset.id)
            assert proxy_path.exists()
            assert thumb_path.exists()

            with Image.open(proxy_path) as proxy_im:
                assert proxy_im.size == (32, 32)
            with Image.open(thumb_path) as thumb_im:
                assert thumb_im.size == (32, 32)


def test_image_proxy_worker_poisons_on_resolve_path_value_error(engine, _session_factory, tmp_path):
    """When resolve_path raises ValueError (path traversal), asset is marked poisoned."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    source_dir = tmp_path / "path-resolution"
    source_dir.mkdir(parents=True, exist_ok=True)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="path-resolution-lib",
                name="Path Resolution Lib",
                absolute_path=str(source_dir),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    rel_path = "photo.jpg"
    (source_dir / rel_path).write_bytes(b"\xff\xd8\xff")
    asset_repo.upsert_asset(
        "path-resolution-lib",
        rel_path,
        AssetType.image,
        1000.0,
        100,
    )

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as storage_mock:
            storage_mock.return_value.data_dir = str(data_dir)
            with patch("src.workers.proxy_worker.resolve_path") as resolve_mock:
                resolve_mock.side_effect = ValueError("Path escapes library root: '../etc/passwd'")
                worker = ImageProxyWorker(
                    worker_id="path-resolution-worker",
                    repository=worker_repo,
                    heartbeat_interval_seconds=15.0,
                    asset_repo=asset_repo,
                    system_metadata_repo=system_metadata_repo,
                    library_slug="path-resolution-lib",
                )
                result = worker.process_task()

    assert result is True
    assets = asset_repo.get_assets_by_library("path-resolution-lib", limit=1)
    assert len(assets) == 1
    assert assets[0].status == AssetStatus.poisoned


def test_image_proxy_worker_poisons_on_resolve_path_file_not_found(engine, _session_factory, tmp_path):
    """When resolve_path raises FileNotFoundError, asset is marked poisoned."""
    asset_repo, worker_repo, system_metadata_repo = _create_tables_and_repos(
        engine, _session_factory
    )
    source_dir = tmp_path / "path-resolution-missing"
    source_dir.mkdir(parents=True, exist_ok=True)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="path-resolution-missing-lib",
                name="Path Resolution Missing Lib",
                absolute_path=str(source_dir),
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    rel_path = "nonexistent.jpg"
    asset_repo.upsert_asset(
        "path-resolution-missing-lib",
        rel_path,
        AssetType.image,
        1000.0,
        100,
    )

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        with patch("src.core.storage.get_config") as storage_mock:
            storage_mock.return_value.data_dir = str(data_dir)
            with patch("src.workers.proxy_worker.resolve_path") as resolve_mock:
                resolve_mock.side_effect = FileNotFoundError("Path does not exist")
                worker = ImageProxyWorker(
                    worker_id="path-resolution-missing-worker",
                    repository=worker_repo,
                    heartbeat_interval_seconds=15.0,
                    asset_repo=asset_repo,
                    system_metadata_repo=system_metadata_repo,
                    library_slug="path-resolution-missing-lib",
                )
                result = worker.process_task()

    assert result is True
    assets = asset_repo.get_assets_by_library("path-resolution-missing-lib", limit=1)
    assert len(assets) == 1
    assert assets[0].status == AssetStatus.poisoned

