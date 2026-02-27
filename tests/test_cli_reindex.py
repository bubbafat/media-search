"""Smoke tests for CLI asset reindex and library reindex-videos."""

import os

import pytest
from typer.testing import CliRunner
from sqlalchemy import text
from sqlmodel import SQLModel

from tests.conftest import clear_app_db_caches
from src.cli import app
from src.models.entities import AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.video_scene_repo import VideoSceneRepository, VideoSceneRow

pytestmark = [pytest.mark.slow]


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return (lib_repo, asset_repo, scene_repo)."""
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
        LibraryRepository(session_factory),
        AssetRepository(session_factory),
        VideoSceneRepository(session_factory),
    )


@pytest.fixture
def reindex_cli_db(postgres_container, engine, _session_factory, request):
    """Postgres with tables, one library, one video asset with one scene. DATABASE_URL set for CLI. Yields (library_slug, rel_path, lib_repo, asset_repo, scene_repo)."""
    lib_repo, asset_repo, scene_repo = _create_tables_and_seed(engine, _session_factory)
    slug = f"reindex-cli-{request.node.name[:50]}"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Reindex CLI Lib",
                absolute_path="/tmp/reindex-cli",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset(slug, "video.mp4", AssetType.video, 1000.0, 10_000)
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :s AND rel_path = 'video.mp4'"),
            {"s": slug},
        ).fetchone()
        asset_id = row[0]
    finally:
        session.close()
    scene_repo.save_scene_and_update_state(
        asset_id,
        VideoSceneRow(
            start_ts=0.0,
            end_ts=5.0,
            description="First scene",
            metadata=None,
            sharpness_score=10.0,
            rep_frame_path="video_scenes/lib/1/0.000_5.000.jpg",
            keep_reason="phash",
        ),
        None,
    )
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'completed' WHERE id = :id"),
            {"id": asset_id},
        )
        session.commit()
    finally:
        session.close()

    url = postgres_container.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    clear_app_db_caches()
    try:
        yield slug, "video.mp4", lib_repo, asset_repo, scene_repo
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        clear_app_db_caches()


def test_asset_reindex_clears_scenes_and_sets_pending(reindex_cli_db):
    """asset reindex exits 0 and leaves the asset pending with no scenes."""
    library_slug, rel_path, lib_repo, asset_repo, scene_repo = reindex_cli_db
    asset = asset_repo.get_asset(library_slug, rel_path)
    assert asset is not None
    assert asset.id is not None
    assert asset.status == AssetStatus.completed
    assert len(scene_repo.list_scenes(asset.id)) == 1

    runner = CliRunner()
    result = runner.invoke(app, ["asset", "reindex", library_slug, rel_path])
    assert result.exit_code == 0
    assert "pending" in (result.stdout + result.stderr)
    assert "ai video" in (result.stdout + result.stderr)

    asset_after = asset_repo.get_asset(library_slug, rel_path)
    assert asset_after is not None
    assert asset_after.status == AssetStatus.pending
    assert scene_repo.list_scenes(asset_after.id) == []


@pytest.fixture
def library_reindex_cli_db(postgres_container, engine, _session_factory, request):
    """Postgres with tables, one library, two video assets with scenes. DATABASE_URL set for CLI. Yields (library_slug, lib_repo, asset_repo, scene_repo, session_factory)."""
    lib_repo, asset_repo, scene_repo = _create_tables_and_seed(engine, _session_factory)
    slug = f"lib-reindex-{request.node.name[:45]}"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Lib Reindex CLI",
                absolute_path="/tmp/lib-reindex",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset(slug, "a.mp4", AssetType.video, 1000.0, 1000)
    asset_repo.upsert_asset(slug, "b.mov", AssetType.video, 1000.0, 2000)
    asset_ids = asset_repo.get_video_asset_ids_by_library(slug)
    assert len(asset_ids) == 2
    for aid in asset_ids:
        scene_repo.save_scene_and_update_state(
            aid,
            VideoSceneRow(
                start_ts=0.0,
                end_ts=3.0,
                description="Scene",
                metadata=None,
                sharpness_score=5.0,
                rep_frame_path="video_scenes/lib/2/0.000_3.000.jpg",
                keep_reason="forced",
            ),
            None,
        )
        asset_repo.update_asset_status(aid, AssetStatus.completed)

    url = postgres_container.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    clear_app_db_caches()
    try:
        yield slug, lib_repo, asset_repo, scene_repo, _session_factory
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        clear_app_db_caches()


def test_library_reindex_videos_clears_all_and_sets_pending(library_reindex_cli_db):
    """library reindex-videos exits 0 and leaves all video assets pending with no scenes."""
    library_slug, lib_repo, asset_repo, scene_repo, session_factory = library_reindex_cli_db
    asset_ids = asset_repo.get_video_asset_ids_by_library(library_slug)
    assert len(asset_ids) == 2
    for aid in asset_ids:
        assert len(scene_repo.list_scenes(aid)) == 1

    runner = CliRunner()
    result = runner.invoke(app, ["library", "reindex-videos", library_slug])
    assert result.exit_code == 0
    assert "2 video asset(s)" in (result.stdout + result.stderr)
    assert "pending" in (result.stdout + result.stderr)
    assert f"--library {library_slug}" in (result.stdout + result.stderr)

    asset_ids_after = asset_repo.get_video_asset_ids_by_library(library_slug)
    assert len(asset_ids_after) == 2
    for aid in asset_ids_after:
        session = session_factory()
        try:
            row = session.execute(
                text("SELECT status FROM asset WHERE id = :id"),
                {"id": aid},
            ).fetchone()
            assert row[0] == AssetStatus.pending.value
        finally:
            session.close()
        assert scene_repo.list_scenes(aid) == []
