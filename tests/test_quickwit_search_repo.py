"""Tests for QuickwitSearchRepository against the dev Quickwit instance.

Requires the dev Quickwit instance to be running at http://127.0.0.1:7281.
All tests are marked slow and will be skipped if Quickwit is unavailable.

Tests use uniquely named indexes with a 'test_scenes_' prefix and always
delete them in teardown. Tests must never create an index whose name
starts with 'media_scenes'.
"""
import time
import uuid

import httpx
import pytest

from src.repository.quickwit_search_repo import QuickwitSearchRepository

pytestmark = [pytest.mark.slow]

_QUICKWIT_DEV_URL = "http://127.0.0.1:7281"
_SCHEMA_PATH = "quickwit/media_scenes_schema.json"
_PROD_INDEX_PREFIX = "media_scenes"


def _test_index_name() -> str:
    """Generate a unique test index name. Asserts it does not clash with prod."""
    name = f"test_scenes_{uuid.uuid4().hex[:8]}"
    assert not name.startswith(_PROD_INDEX_PREFIX), (
        f"Test index name '{name}' must not start with '{_PROD_INDEX_PREFIX}'"
    )
    return name


def _wait_for_commit(seconds: int = 12) -> None:
    """Wait for Quickwit to commit indexed documents.

    commit_timeout_secs in the schema is 10. Use 12 to provide a buffer
    and avoid intermittent failures under load.
    """
    time.sleep(seconds)


@pytest.fixture(autouse=True)
def require_quickwit():
    """Skip all tests in this module if the dev Quickwit instance is unavailable."""
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, "")
    if not repo.is_healthy():
        pytest.skip(
            f"Dev Quickwit instance not available at {_QUICKWIT_DEV_URL}. "
            "Start it before running these tests."
        )


def test_is_healthy_returns_true():
    """is_healthy() returns True when Quickwit is running."""
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, "")
    assert repo.is_healthy() is True


def test_create_and_delete_index():
    """Create an index, confirm it exists, delete it, confirm it is gone."""
    index_name = _test_index_name()
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)
    try:
        repo.create_index(index_name, _SCHEMA_PATH)
        # Confirm exists
        resp = httpx.get(f"{_QUICKWIT_DEV_URL}/api/v1/indexes/{index_name}")
        assert resp.status_code == 200
    finally:
        repo.delete_index(index_name)
    # Confirm gone
    resp = httpx.get(f"{_QUICKWIT_DEV_URL}/api/v1/indexes/{index_name}")
    assert resp.status_code == 404


def test_index_and_search_document():
    """Index one document, wait for commit, search returns it."""
    index_name = _test_index_name()
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)
    doc = {
        "id": f"asset_1001_model_1_v1",
        "asset_id": 1001,
        "library_slug": "test-lib",
        "description": "a bright red balloon floating in a blue sky",
        "tags": ["balloon", "sky", "outdoor"],
        "preview_ready": True,
        "playable": False,
        "searchable": True,
        "offline_ready": False,
        "indexed_at": 1700000000,
    }
    try:
        repo.create_index(index_name, _SCHEMA_PATH)
        repo.index_document(index_name, doc)
        _wait_for_commit(12)
        results = repo.search("balloon")
        assert len(results) == 1
        assert results[0].asset.id == 1001
    finally:
        repo.delete_index(index_name)


def test_search_returns_empty_for_no_match():
    """Index one document, search for a term not in it, returns empty list."""
    index_name = _test_index_name()
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)
    doc = {
        "id": "asset_1002_model_1_v1",
        "asset_id": 1002,
        "library_slug": "test-lib",
        "description": "a bright red balloon floating in a blue sky",
        "tags": ["balloon", "sky"],
        "preview_ready": True,
        "playable": False,
        "searchable": True,
        "offline_ready": False,
        "indexed_at": 1700000000,
    }
    try:
        repo.create_index(index_name, _SCHEMA_PATH)
        repo.index_document(index_name, doc)
        _wait_for_commit(12)
        results = repo.search("submarine")
        assert results == []
    finally:
        repo.delete_index(index_name)


def test_search_shadow_queries_named_index():
    """search_shadow() returns documents from the named index regardless of active_index_name."""
    index_a = _test_index_name()
    index_b = _test_index_name()
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_a)
    doc_a = {
        "id": "asset_2001_model_1_v1",
        "asset_id": 2001,
        "library_slug": "lib-a",
        "description": "a wooden sailing boat on calm water",
        "tags": ["boat", "water"],
        "preview_ready": True,
        "playable": False,
        "searchable": True,
        "offline_ready": False,
        "indexed_at": 1700000000,
    }
    doc_b = {
        "id": "asset_2002_model_1_v1",
        "asset_id": 2002,
        "library_slug": "lib-b",
        "description": "a mountain peak covered in fresh snow",
        "tags": ["mountain", "snow"],
        "preview_ready": True,
        "playable": False,
        "searchable": True,
        "offline_ready": False,
        "indexed_at": 1700000000,
    }
    try:
        repo.create_index(index_a, _SCHEMA_PATH)
        repo.create_index(index_b, _SCHEMA_PATH)
        repo.index_document(index_a, doc_a)
        repo.index_document(index_b, doc_b)
        _wait_for_commit(12)
        results_a = repo.search_shadow(index_a, "boat")
        assert len(results_a) == 1
        assert results_a[0].asset.id == 2001
        results_b = repo.search_shadow(index_b, "mountain")
        assert len(results_b) == 1
        assert results_b[0].asset.id == 2002
    finally:
        repo.delete_index(index_a)
        repo.delete_index(index_b)


def test_library_slug_filter():
    """search() with library_slugs filter returns only documents from matching library."""
    index_name = _test_index_name()
    repo = QuickwitSearchRepository(_QUICKWIT_DEV_URL, index_name)
    doc_1 = {
        "id": "asset_3001_model_1_v1",
        "asset_id": 3001,
        "library_slug": "library-one",
        "description": "a golden retriever playing fetch in a park",
        "tags": ["dog", "park", "fetch"],
        "preview_ready": True,
        "playable": False,
        "searchable": True,
        "offline_ready": False,
        "indexed_at": 1700000000,
    }
    doc_2 = {
        "id": "asset_3002_model_1_v1",
        "asset_id": 3002,
        "library_slug": "library-two",
        "description": "a golden retriever playing fetch in a park",
        "tags": ["dog", "park", "fetch"],
        "preview_ready": True,
        "playable": False,
        "searchable": True,
        "offline_ready": False,
        "indexed_at": 1700000000,
    }
    try:
        repo.create_index(index_name, _SCHEMA_PATH)
        repo.index_document(index_name, doc_1)
        repo.index_document(index_name, doc_2)
        _wait_for_commit(12)
        results = repo.search("golden retriever", library_slugs=["library-one"])
        assert len(results) == 1
        assert results[0].asset.library_id == "library-one"
    finally:
        repo.delete_index(index_name)
