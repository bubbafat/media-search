"""Tests for library repository add() and soft-delete slug collision handling (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AssetType, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository

pytestmark = [pytest.mark.slow]


def _create_tables_and_lib_repo(engine, session_factory):
    """Create all tables and seed schema_version. Return LibraryRepository."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return LibraryRepository(session_factory)


def test_add_with_no_collision_returns_slug(engine, _session_factory):
    """add() with no existing slug inserts and returns the slug."""
    lib_repo = _create_tables_and_lib_repo(engine, _session_factory)
    slug = lib_repo.add("My Library", "/tmp/my-lib")
    assert slug == "my-library"
    lib = lib_repo.get_by_slug(slug)
    assert lib is not None
    assert lib.name == "My Library"
    assert lib.absolute_path == "/tmp/my-lib"
    assert lib.deleted_at is None


def test_add_with_active_collision_raises(engine, _session_factory):
    """add() when an active library has the same slug raises ValueError."""
    lib_repo = _create_tables_and_lib_repo(engine, _session_factory)
    lib_repo.add("Foo", "/path/one")
    with pytest.raises(ValueError, match="An active library with the slug 'foo' already exists"):
        lib_repo.add("foo", "/path/two")


def test_add_with_deleted_collision_raises(engine, _session_factory):
    """add() when a deleted library has the same slug raises ValueError."""
    lib_repo = _create_tables_and_lib_repo(engine, _session_factory)
    lib_repo.add("Bar", "/path/one")
    lib_repo.soft_delete("bar")
    with pytest.raises(
        ValueError,
        match="A deleted library with the slug 'bar' exists in the trash. Please restore it or use a different name",
    ):
        lib_repo.add("bar", "/path/two")


def test_hard_delete_removes_library_and_assets(engine, _session_factory):
    """hard_delete() removes a trashed library and all its assets from the DB."""
    lib_repo = _create_tables_and_lib_repo(engine, _session_factory)
    asset_repo = AssetRepository(_session_factory)
    slug = lib_repo.add("Trash Me", "/tmp/trash-me")
    for i in range(10):
        asset_repo.upsert_asset(slug, f"file_{i}.jpg", AssetType.image, 12345.0 + i, 1024 * (i + 1))
    lib_repo.soft_delete(slug)
    lib_repo.hard_delete(slug)
    assert lib_repo.get_by_slug(slug, include_deleted=True) is None
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE library_id = :slug"),
            {"slug": slug},
        ).scalar()
        assert count == 0
    finally:
        session.close()


def test_hard_delete_removes_library_and_assets_with_video_scenes(engine, _session_factory):
    """hard_delete() removes a trashed library and its assets even when video_scenes/video_active_state reference them."""
    lib_repo = _create_tables_and_lib_repo(engine, _session_factory)
    asset_repo = AssetRepository(_session_factory)
    slug = lib_repo.add("Trash With Scenes", "/tmp/trash-scenes")
    asset_repo.upsert_asset(slug, "video.mp4", AssetType.video, 12345.0, 1024)
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :slug AND rel_path = 'video.mp4'"),
            {"slug": slug},
        ).fetchone()
        assert row is not None
        asset_id = row[0]
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:aid, 0.0, 5.0, 'scene one', NULL, 1.0, 'video_scenes/trash-with-scenes/1/0_5.jpg', 'phash')"
            ),
            {"aid": asset_id},
        )
        session.execute(
            text(
                "INSERT INTO video_active_state (asset_id, anchor_phash, scene_start_ts, current_best_pts, current_best_sharpness) "
                "VALUES (:aid, '', 0.0, 0.0, -1.0)"
            ),
            {"aid": asset_id},
        )
        session.commit()
    finally:
        session.close()
    lib_repo.soft_delete(slug)
    lib_repo.hard_delete(slug)
    assert lib_repo.get_by_slug(slug, include_deleted=True) is None
    session = _session_factory()
    try:
        count = session.execute(
            text("SELECT COUNT(*) FROM asset WHERE library_id = :slug"),
            {"slug": slug},
        ).scalar()
        assert count == 0
        scenes_count = session.execute(text("SELECT COUNT(*) FROM video_scenes")).scalar()
        assert scenes_count == 0
        state_count = session.execute(text("SELECT COUNT(*) FROM video_active_state")).scalar()
        assert state_count == 0
    finally:
        session.close()


def test_repair_orphan_assets_removes_assets_for_missing_library(engine, _session_factory):
    """get_orphaned_library_slugs finds orphaned library_id; delete_orphaned_assets_for_library removes them."""
    lib_repo = _create_tables_and_lib_repo(engine, _session_factory)
    asset_repo = AssetRepository(_session_factory)
    slug = lib_repo.add("Orphan Lib", "/tmp/orphan-lib")
    asset_repo.upsert_asset(slug, "a.jpg", AssetType.image, 1000.0, 100)
    asset_repo.upsert_asset(slug, "b.jpg", AssetType.image, 1000.0, 200)
    # Simulate bad state: drop FK so we can remove the library row and leave orphaned assets
    session = _session_factory()
    try:
        session.execute(text("ALTER TABLE asset DROP CONSTRAINT asset_library_id_fkey"))
        session.execute(text("DELETE FROM library WHERE slug = :slug"), {"slug": slug})
        session.commit()
    finally:
        session.close()
    assert lib_repo.get_orphaned_library_slugs() == [slug]
    assert lib_repo.get_orphaned_asset_count_for_library(slug) == 2
    deleted = lib_repo.delete_orphaned_assets_for_library(slug)
    assert deleted == 2
    assert lib_repo.get_orphaned_library_slugs() == []
    session = _session_factory()
    try:
        count = session.execute(text("SELECT COUNT(*) FROM asset WHERE library_id = :slug"), {"slug": slug}).scalar()
        assert count == 0
        # Restore FK so schema is intact for other tests / same engine
        session.execute(
            text("ALTER TABLE asset ADD CONSTRAINT asset_library_id_fkey FOREIGN KEY (library_id) REFERENCES library(slug)")
        )
        session.commit()
    finally:
        session.close()
