"""Tests for library repository add() and soft-delete slug collision handling (testcontainers Postgres)."""

import pytest
from sqlmodel import SQLModel

from src.models.entities import SystemMetadata
from src.repository.library_repo import LibraryRepository


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
