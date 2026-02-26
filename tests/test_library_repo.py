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
