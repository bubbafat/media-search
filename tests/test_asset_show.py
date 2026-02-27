"""Tests for CLI asset show (minimal and --metadata)."""

import json
import os

import pytest
from typer.testing import CliRunner
from sqlmodel import SQLModel

from tests.conftest import clear_app_db_caches
from src.cli import app
from src.models.entities import AssetType, Library, SystemMetadata
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
def asset_show_cli_db(postgres_container, engine, _session_factory, request):
    """Postgres with tables and one library/asset; DATABASE_URL set for CLI. Yields (library_slug, rel_path)."""
    lib_repo, asset_repo = _create_tables_and_seed(engine, _session_factory)
    slug = f"show-cli-{request.node.name}"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Show CLI Lib",
                absolute_path="/tmp/show-cli",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset(slug, "demo.jpg", AssetType.image, 1000.0, 4096)

    url = postgres_container.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    clear_app_db_caches()
    try:
        yield slug, "demo.jpg"
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        clear_app_db_caches()


def test_asset_show_minimal_output(asset_show_cli_db):
    """asset show <slug> <path> prints minimal key-value lines and no JSON."""
    library_slug, rel_path = asset_show_cli_db
    runner = CliRunner()
    result = runner.invoke(app, ["asset", "show", library_slug, rel_path])
    assert result.exit_code == 0
    out = result.stdout
    assert "id:" in out
    assert "library_id:" in out
    assert "rel_path:" in out
    assert "type:" in out
    assert "status:" in out
    assert "size:" in out
    assert "demo.jpg" in out
    assert out.strip().startswith("id:")  # not JSON
    assert "visual_analysis" not in out


def test_asset_show_metadata_output(asset_show_cli_db):
    """asset show <slug> <path> --metadata prints valid JSON with visual_analysis and expected keys."""
    library_slug, rel_path = asset_show_cli_db
    runner = CliRunner()
    result = runner.invoke(app, ["asset", "show", library_slug, rel_path, "--metadata"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert "visual_analysis" in data
    assert "id" in data
    assert "library_id" in data
    assert "rel_path" in data
    assert data["rel_path"] == rel_path
    assert "type" in data
    assert "status" in data
    assert "size" in data


def test_asset_show_library_not_found(asset_show_cli_db):
    """asset show with nonexistent library exits 1 and prints library not found."""
    _, rel_path = asset_show_cli_db
    runner = CliRunner()
    result = runner.invoke(app, ["asset", "show", "nonexistent-slug-999", rel_path])
    assert result.exit_code == 1
    assert "not found or deleted" in (result.stdout + result.stderr)


def test_asset_show_asset_not_found(asset_show_cli_db):
    """asset show with nonexistent rel_path exits 1 and prints asset not found."""
    library_slug, _ = asset_show_cli_db
    runner = CliRunner()
    result = runner.invoke(app, ["asset", "show", library_slug, "nonexistent.jpg"])
    assert result.exit_code == 1
    assert "Asset not found" in (result.stdout + result.stderr)
