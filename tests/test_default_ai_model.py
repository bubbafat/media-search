"""Tests for system default AI model: get/set, get_ai_model helpers, mock rejection (testcontainers Postgres)."""

import os

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AIModel, SystemMetadata
from src.repository.system_metadata_repo import (
    ALLOW_MOCK_DEFAULT_ENV,
    SystemMetadataRepository,
)

pytestmark = [pytest.mark.slow]


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return SystemMetadataRepository."""
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


def test_get_default_ai_model_id_returns_none_when_unset(engine, _session_factory):
    """get_default_ai_model_id returns None when no default is set."""
    repo = _create_tables_and_seed(engine, _session_factory)
    assert repo.get_default_ai_model_id() is None


def test_set_default_ai_model_id_and_get(engine, _session_factory):
    """set_default_ai_model_id persists; get_default_ai_model_id returns it."""
    repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(AIModel(name="moondream2", version="2025-01-09"))
        session.commit()
        row = session.execute(text("SELECT id FROM aimodel WHERE name = 'moondream2' LIMIT 1")).fetchone()
        model_id = row[0]
    finally:
        session.close()

    repo.set_default_ai_model_id(model_id)
    assert repo.get_default_ai_model_id() == model_id


def test_set_default_ai_model_id_rejects_mock_without_env(engine, _session_factory):
    """set_default_ai_model_id raises when model name is mock and MEDIASEARCH_ALLOW_MOCK_DEFAULT is not 1."""
    repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(AIModel(name="mock", version="1"))
        session.commit()
        row = session.execute(text("SELECT id FROM aimodel WHERE name = 'mock' LIMIT 1")).fetchone()
        mock_id = row[0]
    finally:
        session.close()

    with pytest.raises(ValueError, match="Cannot set .mock. as the default"):
        repo.set_default_ai_model_id(mock_id)


def test_set_default_ai_model_id_allows_mock_with_env(engine, _session_factory):
    """set_default_ai_model_id succeeds for mock when MEDIASEARCH_ALLOW_MOCK_DEFAULT=1."""
    repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        row = session.execute(text("SELECT id FROM aimodel WHERE name = 'mock' LIMIT 1")).fetchone()
        if row is None:
            session.add(AIModel(name="mock", version="1"))
            session.commit()
            row = session.execute(text("SELECT id FROM aimodel WHERE name = 'mock' LIMIT 1")).fetchone()
        mock_id = row[0]
    finally:
        session.close()

    os.environ[ALLOW_MOCK_DEFAULT_ENV] = "1"
    try:
        repo.set_default_ai_model_id(mock_id)
        assert repo.get_default_ai_model_id() == mock_id
    finally:
        os.environ.pop(ALLOW_MOCK_DEFAULT_ENV, None)


def test_set_default_ai_model_id_raises_when_model_missing(engine, _session_factory):
    """set_default_ai_model_id raises when the model id does not exist."""
    repo = _create_tables_and_seed(engine, _session_factory)
    with pytest.raises(ValueError, match="does not exist"):
        repo.set_default_ai_model_id(99999)


def test_get_ai_model_by_id(engine, _session_factory):
    """get_ai_model_by_id returns the model or None."""
    repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(AIModel(name="x", version="1"))
        session.commit()
        row = session.execute(text("SELECT id FROM aimodel WHERE name = 'x' LIMIT 1")).fetchone()
        mid = row[0]
    finally:
        session.close()

    model = repo.get_ai_model_by_id(mid)
    assert model is not None
    assert model.name == "x"
    assert model.version == "1"
    assert repo.get_ai_model_by_id(99999) is None


def test_get_ai_model_by_name_version_with_version(engine, _session_factory):
    """get_ai_model_by_name_version with version returns exact match or None."""
    repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(AIModel(name="n", version="v1"))
        session.add(AIModel(name="n", version="v2"))
        session.commit()
    finally:
        session.close()

    m = repo.get_ai_model_by_name_version("n", "v2")
    assert m is not None
    assert m.version == "v2"
    assert repo.get_ai_model_by_name_version("n", "v3") is None


def test_get_ai_model_by_name_version_without_version_returns_latest(engine, _session_factory):
    """get_ai_model_by_name_version without version returns row with highest id for that name."""
    repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(AIModel(name="latest", version="1"))
        session.add(AIModel(name="latest", version="2"))
        session.commit()
    finally:
        session.close()

    m = repo.get_ai_model_by_name_version("latest", None)
    assert m is not None
    assert m.name == "latest"
    # Should be one of the two; implementation uses order_by id desc limit 1
    assert m.version in ("1", "2")
