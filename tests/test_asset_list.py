"""Tests for asset repository get_assets_by_library and CLI asset list (testcontainers Postgres)."""

import os

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.core import config as config_module
from src.models.entities import AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return (lib_repo, asset_repo)."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return LibraryRepository(session_factory), AssetRepository(session_factory)


def test_get_assets_by_library_returns_assets_ordered_by_id_desc(
    engine, _session_factory
):
    """get_assets_by_library returns assets for the library, ordered by id descending."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "asset-list-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Asset List Lib",
                absolute_path="/tmp/asset-list",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "a.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset(slug, "b.jpg", AssetType.image, 1001.0, 2048)
    asset_repo.upsert_asset(slug, "c.jpg", AssetType.video, 1002.0, 4096)

    assets = asset_repo.get_assets_by_library(slug, limit=10)
    assert len(assets) == 3
    ids = [a.id for a in assets if a.id is not None]
    assert ids == sorted(ids, reverse=True)
    rel_paths = [a.rel_path for a in assets]
    assert "a.jpg" in rel_paths and "b.jpg" in rel_paths and "c.jpg" in rel_paths


def test_get_assets_by_library_filters_by_status(engine, _session_factory):
    """get_assets_by_library with status= filters to that status only."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "status-filter-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Status Filter Lib",
                absolute_path="/tmp/status-filter",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "p.jpg", AssetType.image, 1000.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'completed' WHERE library_id = :lib AND rel_path = 'p.jpg'"
            ),
            {"lib": slug},
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "q.jpg", AssetType.image, 1001.0, 200)

    pending = asset_repo.get_assets_by_library(slug, limit=50, status=AssetStatus.pending)
    completed = asset_repo.get_assets_by_library(
        slug, limit=50, status=AssetStatus.completed
    )
    assert len(pending) == 1
    assert pending[0].rel_path == "q.jpg"
    assert len(completed) == 1
    assert completed[0].rel_path == "p.jpg"


def test_get_assets_by_library_respects_limit(engine, _session_factory):
    """get_assets_by_library respects limit."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "limit-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Limit Lib",
                absolute_path="/tmp/limit",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    for i in range(5):
        asset_repo.upsert_asset(slug, f"f{i}.jpg", AssetType.image, 1000.0 + i, 100)

    assets = asset_repo.get_assets_by_library(slug, limit=2)
    assert len(assets) == 2


def test_get_asset_returns_asset_when_found(engine, _session_factory):
    """get_asset returns the asset with correct rel_path and visual_analysis when present."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "show-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Show Lib",
                absolute_path="/tmp/show",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "photo.jpg", AssetType.image, 1000.0, 2048)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) WHERE library_id = :lib AND rel_path = 'photo.jpg'"
            ),
            {"va": '{"description": "A test", "tags": ["x"], "ocr_text": "Hello"}', "lib": slug},
        )
        session.commit()
    finally:
        session.close()

    asset = asset_repo.get_asset(slug, "photo.jpg")
    assert asset is not None
    assert asset.rel_path == "photo.jpg"
    assert asset.visual_analysis is not None
    assert asset.visual_analysis.get("ocr_text") == "Hello"


def test_get_asset_returns_none_when_path_missing(engine, _session_factory):
    """get_asset returns None when rel_path does not exist."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "show-missing-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Show Missing Lib",
                absolute_path="/tmp/show-missing",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset(slug, "only.jpg", AssetType.image, 1000.0, 100)

    assert asset_repo.get_asset(slug, "nonexistent.jpg") is None


def test_get_asset_returns_none_when_library_deleted(engine, _session_factory):
    """get_asset returns None for assets in a trashed (soft-deleted) library."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "trashed-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Trashed Lib",
                absolute_path="/tmp/trashed",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset(slug, "in-trash.jpg", AssetType.image, 1000.0, 100)

    lib_repo.soft_delete(slug)

    assert asset_repo.get_asset(slug, "in-trash.jpg") is None


@pytest.fixture
def asset_list_cli_db(postgres_container, engine, _session_factory, request):
    """Postgres with tables; set DATABASE_URL so CLI uses it. Yields (session_factory, library_slug)."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = f"cli-list-lib-{request.node.name}"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="CLI List Lib",
                absolute_path="/tmp/cli-list",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset(slug, "one.jpg", AssetType.image, 1000.0, 2048)
    asset_repo.upsert_asset(slug, "two.png", AssetType.image, 1001.0, 4096)

    url = postgres_container.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    config_module._config = None
    try:
        yield _session_factory, slug
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        config_module._config = None


def test_asset_list_cli_shows_table_and_summary(asset_list_cli_db):
    """asset list <slug> prints a table and summary line."""
    _session_factory, library_slug = asset_list_cli_db
    from typer.testing import CliRunner

    from src.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["asset", "list", library_slug])
    assert result.exit_code == 0
    assert "Rel Path" in result.stdout
    assert "one.jpg" in result.stdout
    assert "two.png" in result.stdout
    assert f"Showing 2 of 2 assets for library '{library_slug}'." in result.stdout


def test_asset_list_cli_exits_when_library_not_found(asset_list_cli_db):
    """asset list <bad_slug> prints error and exits with 1."""
    from typer.testing import CliRunner

    from src.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["asset", "list", "nonexistent-slug-12345"])
    assert result.exit_code == 1
    assert "not found or deleted" in (result.stdout + result.stderr)
