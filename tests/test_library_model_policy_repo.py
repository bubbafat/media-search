"""Tests for library_model_policy repository (testcontainers Postgres)."""

import pytest
from sqlmodel import SQLModel

from src.models.entities import LibraryModelPolicy, SystemMetadata
from src.repository.library_model_policy_repo import LibraryModelPolicyRepository
from src.repository.library_repo import LibraryRepository

pytestmark = [pytest.mark.slow]


def _create_tables_and_policy_repo(engine, session_factory) -> LibraryModelPolicyRepository:
    """Create all tables, seed schema_version, add a library for FK, return policy repo."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    lib_repo = LibraryRepository(session_factory)
    if lib_repo.get_by_slug("test-library") is None:
        lib_repo.add("Test Library", "/tmp/test-lib")
    return LibraryModelPolicyRepository(session_factory)


def test_get_returns_none_for_missing_slug(engine, _session_factory):
    """get on a slug that does not exist returns None."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    result = policy_repo.get("nonexistent-slug")
    assert result is None


def test_upsert_creates_new_row(engine, _session_factory):
    """upsert with a new slug inserts the row; get returns it with correct field values."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_v1",
        shadow_index_name=None,
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=0.0,
    )
    policy_repo.upsert(policy)
    retrieved = policy_repo.get("test-library")
    assert retrieved is not None
    assert retrieved.library_slug == "test-library"
    assert retrieved.active_index_name == "idx_v1"
    assert retrieved.shadow_index_name is None
    assert retrieved.previous_index_name is None
    assert retrieved.locked is False
    assert retrieved.locked_since is None
    assert retrieved.promotion_progress == 0.0


def test_upsert_updates_existing_row(engine, _session_factory):
    """upsert on an existing slug updates all fields."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_v1",
        shadow_index_name=None,
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=0.5,
    )
    policy_repo.upsert(policy)
    updated = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_v2",
        shadow_index_name="idx_v3",
        previous_index_name="idx_v1",
        locked=True,
        locked_since=None,
        promotion_progress=0.9,
    )
    policy_repo.upsert(updated)
    retrieved = policy_repo.get("test-library")
    assert retrieved is not None
    assert retrieved.active_index_name == "idx_v2"
    assert retrieved.shadow_index_name == "idx_v3"
    assert retrieved.previous_index_name == "idx_v1"
    assert retrieved.locked is True
    assert retrieved.promotion_progress == 0.9


def test_promote_moves_active_to_previous(engine, _session_factory):
    """after promote, active_index_name equals the former shadow, previous_index_name equals the former active, shadow_index_name is None, locked is False."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_active",
        shadow_index_name="idx_shadow",
        previous_index_name=None,
        locked=True,
        locked_since=None,
        promotion_progress=0.8,
    )
    policy_repo.upsert(policy)
    policy_repo.promote("test-library", "idx_shadow")
    retrieved = policy_repo.get("test-library")
    assert retrieved is not None
    assert retrieved.active_index_name == "idx_shadow"
    assert retrieved.previous_index_name == "idx_active"
    assert retrieved.shadow_index_name is None
    assert retrieved.locked is False


def test_rollback_restores_previous(engine, _session_factory):
    """after a promote followed by rollback, active_index_name is restored to the pre-promote value."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_original",
        shadow_index_name="idx_new",
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=1.0,
    )
    policy_repo.upsert(policy)
    policy_repo.promote("test-library", "idx_new")
    retrieved_after_promote = policy_repo.get("test-library")
    assert retrieved_after_promote.active_index_name == "idx_new"
    assert retrieved_after_promote.previous_index_name == "idx_original"
    policy_repo.rollback("test-library")
    retrieved_after_rollback = policy_repo.get("test-library")
    assert retrieved_after_rollback.active_index_name == "idx_original"


def test_rollback_raises_when_no_previous(engine, _session_factory):
    """rollback raises ValueError when previous_index_name is NULL."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_only",
        shadow_index_name=None,
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=1.0,
    )
    policy_repo.upsert(policy)
    with pytest.raises(ValueError, match="No previous index to roll back to for library 'test-library'"):
        policy_repo.rollback("test-library")


def test_begin_shadow_indexing_sets_lock(engine, _session_factory):
    """after begin_shadow_indexing, locked is True, locked_since is not None, promotion_progress is 0.0."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_v1",
        shadow_index_name=None,
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=0.5,
    )
    policy_repo.upsert(policy)
    policy_repo.begin_shadow_indexing("test-library", "idx_shadow")
    retrieved = policy_repo.get("test-library")
    assert retrieved is not None
    assert retrieved.locked is True
    assert retrieved.locked_since is not None
    assert retrieved.promotion_progress == 0.0
    assert retrieved.shadow_index_name == "idx_shadow"


def test_update_progress_clamps_above_one(engine, _session_factory):
    """passing 1.5 stores 1.0."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_v1",
        shadow_index_name=None,
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=0.0,
    )
    policy_repo.upsert(policy)
    policy_repo.update_progress("test-library", 1.5)
    retrieved = policy_repo.get("test-library")
    assert retrieved is not None
    assert retrieved.promotion_progress == 1.0


def test_update_progress_clamps_below_zero(engine, _session_factory):
    """passing -0.5 stores 0.0."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy = LibraryModelPolicy(
        library_slug="test-library",
        active_index_name="idx_v1",
        shadow_index_name=None,
        previous_index_name=None,
        locked=False,
        locked_since=None,
        promotion_progress=0.5,
    )
    policy_repo.upsert(policy)
    policy_repo.update_progress("test-library", -0.5)
    retrieved = policy_repo.get("test-library")
    assert retrieved is not None
    assert retrieved.promotion_progress == 0.0


def test_list_all_returns_all_policies(engine, _session_factory):
    """list_all returns all library_model_policy rows."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    lib_repo = LibraryRepository(_session_factory)
    # Ensure we have a second library for FK
    if lib_repo.get_by_slug("other-library") is None:
        lib_repo.add("Other Library", "/tmp/other-lib")
    assert policy_repo.list_all() == []
    policy_repo.upsert(
        LibraryModelPolicy(
            library_slug="test-library",
            active_index_name="idx_a",
            shadow_index_name=None,
            previous_index_name=None,
            locked=False,
            locked_since=None,
            promotion_progress=0.0,
        )
    )
    policy_repo.upsert(
        LibraryModelPolicy(
            library_slug="other-library",
            active_index_name="idx_b",
            shadow_index_name=None,
            previous_index_name=None,
            locked=False,
            locked_since=None,
            promotion_progress=0.0,
        )
    )
    all_policies = policy_repo.list_all()
    assert len(all_policies) == 2
    slugs = {p.library_slug for p in all_policies}
    assert slugs == {"test-library", "other-library"}


def test_delete_removes_row_returns_true(engine, _session_factory):
    """delete on an existing slug removes the row and returns True."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    policy_repo.upsert(
        LibraryModelPolicy(
            library_slug="test-library",
            active_index_name="idx_v1",
            shadow_index_name=None,
            previous_index_name=None,
            locked=False,
            locked_since=None,
            promotion_progress=0.0,
        )
    )
    assert policy_repo.get("test-library") is not None
    result = policy_repo.delete("test-library")
    assert result is True
    assert policy_repo.get("test-library") is None


def test_delete_unknown_slug_returns_false(engine, _session_factory):
    """delete on a slug with no policy returns False."""
    policy_repo = _create_tables_and_policy_repo(engine, _session_factory)
    result = policy_repo.delete("nonexistent-slug")
    assert result is False
