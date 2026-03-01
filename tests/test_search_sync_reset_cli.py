"""CLI test for search-sync --reset."""

import pytest
from sqlmodel import SQLModel
from typer.testing import CliRunner
from unittest.mock import patch

from src.cli import app
from src.models.entities import LibraryModelPolicy, SystemMetadata
from src.repository.library_model_policy_repo import LibraryModelPolicyRepository
from src.repository.library_repo import LibraryRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.workers.search_sync_worker import PROGRESS_KEY

pytestmark = [pytest.mark.slow]


def _bootstrap(engine, session_factory):
    """Create tables, seed schema_version and a library, return repos."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        if session.get(SystemMetadata, "schema_version") is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    lib_repo = LibraryRepository(session_factory)
    # add() generates slug from name: "Test Library" -> "test-library"
    if lib_repo.get_by_slug("test-library") is None:
        lib_repo.add("Test Library", "/tmp/test-lib")
    return {
        "policy_repo": LibraryModelPolicyRepository(session_factory),
        "system_metadata_repo": SystemMetadataRepository(session_factory),
    }


def test_search_sync_reset_clears_cursor_and_policy(engine, _session_factory):
    """search-sync --reset --library X clears cursor and deletes policy; Quickwit delete_index mocked."""
    repos = _bootstrap(engine, _session_factory)
    policy_repo = repos["policy_repo"]
    system_metadata_repo = repos["system_metadata_repo"]

    system_metadata_repo.set_value(PROGRESS_KEY, "12345")
    policy_repo.upsert(
        LibraryModelPolicy(
            library_slug="test-library",
            active_index_name="media_scenes_test_library_999",
            shadow_index_name=None,
            previous_index_name=None,
            locked=False,
            locked_since=None,
            promotion_progress=0.0,
        )
    )
    assert system_metadata_repo.get_value(PROGRESS_KEY) == "12345"
    assert policy_repo.get("test-library") is not None

    with patch(
        "src.repository.quickwit_search_repo.QuickwitSearchRepository.delete_index"
    ) as mock_delete:
        runner = CliRunner()
        result = runner.invoke(app, ["search-sync", "--reset", "--library", "test-library"])

    assert result.exit_code == 0
    mock_delete.assert_called_once_with("media_scenes_test_library_999")
    assert system_metadata_repo.get_value(PROGRESS_KEY) is None
    assert policy_repo.get("test-library") is None
