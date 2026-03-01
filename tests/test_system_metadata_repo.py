"""Tests for SystemMetadataRepository (testcontainers Postgres)."""

import pytest
from sqlmodel import SQLModel

from src.models.entities import SystemMetadata
from src.repository.system_metadata_repo import SystemMetadataRepository

pytestmark = [pytest.mark.slow]


def _create_tables_and_repo(engine, session_factory) -> SystemMetadataRepository:
    """Create all tables, seed schema_version, return system_metadata repo."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return SystemMetadataRepository(session_factory)


def test_delete_value_removes_key_returns_true(engine, _session_factory):
    """delete_value on an existing key removes it and returns True."""
    repo = _create_tables_and_repo(engine, _session_factory)
    repo.set_value("test_key", "test_value")
    assert repo.get_value("test_key") == "test_value"
    result = repo.delete_value("test_key")
    assert result is True
    assert repo.get_value("test_key") is None


def test_delete_value_missing_key_returns_false(engine, _session_factory):
    """delete_value on a missing key returns False and leaves other keys unchanged."""
    repo = _create_tables_and_repo(engine, _session_factory)
    result = repo.delete_value("nonexistent_key")
    assert result is False
    assert repo.get_value("schema_version") == "1"
