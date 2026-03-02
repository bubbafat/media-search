"""CLI tests for ai start with Moondream Station preflight."""

import os

import pytest
from typer.testing import CliRunner
from sqlmodel import SQLModel

from tests.conftest import clear_app_db_caches
from src.cli import app
from src.models.entities import AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository

pytestmark = [pytest.mark.slow]


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


@pytest.fixture
def ai_cli_db(postgres_container, engine, _session_factory, request):
    """Postgres with tables and one empty library. DATABASE_URL set for CLI."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = f"ai-cli-{request.node.name[:50]}"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="AI CLI Lib",
                absolute_path="/tmp/ai-cli",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    url = postgres_container.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    clear_app_db_caches()
    try:
        yield slug, lib_repo, asset_repo
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        clear_app_db_caches()


def test_ai_start_moondream_skips_preflight_when_no_work(ai_cli_db, monkeypatch):
    """ai start with moondream-station does not preflight when library has no claimable assets."""
    library_slug, _lib_repo, asset_repo = ai_cli_db

    # Sanity check: library has no proxied/analyzed_light assets.
    assert asset_repo.count_assets_by_library(library_slug, status=AssetStatus.proxied) == 0
    assert asset_repo.count_assets_by_library(library_slug, status=AssetStatus.analyzed_light) == 0

    # If preflight ran, this would be called; we assert it is NOT called.
    called = {"value": False}

    def fake_get(url, timeout=3):  # noqa: ARG001
        called["value"] = True
        raise AssertionError("Preflight should not be executed when there is no work.")

    monkeypatch.setenv("MEDIASEARCH_MOONDREAM_STATION_ENDPOINT", "http://localhost:2020/v1")
    # Patch requests.get in the cli module namespace; import happens inside ai_start.
    monkeypatch.setattr("requests.get", fake_get, raising=False)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "ai",
            "start",
            "--library",
            library_slug,
            "--analyzer",
            "moondream-station",
            "--once",
        ],
    )
    # Exit 0 or 1 is acceptable here; the key is that preflight was skipped.
    assert called["value"] is False

