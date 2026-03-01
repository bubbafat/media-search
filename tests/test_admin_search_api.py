"""Admin search API: shadow search, promote, rollback. All tests are fast (no Quickwit, no DB)."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import (
    app,
)
from src.core.config import get_config

pytestmark = [pytest.mark.fast]


def _client():
    return TestClient(app)


# --- Test 2: Admin 403 when key missing ---


@pytest.mark.parametrize("path,method", [
    ("/api/admin/search/shadow?q=test&index_name=idx", "get"),
    ("/api/admin/libraries/myslug/model/promote?shadow_index_name=idx", "post"),
    ("/api/admin/libraries/myslug/model/rollback", "post"),
])
def test_admin_403_when_key_missing(path, method):
    """Each admin endpoint returns 403 when admin_key query param is omitted."""
    cfg = get_config()
    patched = cfg.model_copy(update={"admin_key": "realkey"})
    with patch("src.api.main.get_config", return_value=patched):
        client = _client()
        if method == "get":
            res = client.get(path)
        else:
            res = client.post(path)
        assert res.status_code == 403
        assert "Invalid or missing" in res.json().get("detail", "")


# --- Test 3: Admin 403 when key wrong ---


@pytest.mark.parametrize("path,method", [
    ("/api/admin/search/shadow?q=test&index_name=idx&admin_key=wrongkey", "get"),
    ("/api/admin/libraries/myslug/model/promote?shadow_index_name=idx&admin_key=wrongkey", "post"),
    ("/api/admin/libraries/myslug/model/rollback?admin_key=wrongkey", "post"),
])
def test_admin_403_when_key_wrong(path, method):
    """Each admin endpoint returns 403 when admin_key does not match config."""
    cfg = get_config()
    patched = cfg.model_copy(update={"admin_key": "realkey"})
    with patch("src.api.main.get_config", return_value=patched):
        client = _client()
        if method == "get":
            res = client.get(path)
        else:
            res = client.post(path)
        assert res.status_code == 403
        assert "Invalid or missing" in res.json().get("detail", "")


# --- Test 4: Admin 403 when admin_key config is empty ---


@pytest.mark.parametrize("path,method", [
    ("/api/admin/search/shadow?q=test&index_name=idx&admin_key=anykey", "get"),
    ("/api/admin/libraries/myslug/model/promote?shadow_index_name=idx&admin_key=anykey", "post"),
    ("/api/admin/libraries/myslug/model/rollback?admin_key=anykey", "post"),
])
def test_admin_403_when_config_admin_key_empty(path, method):
    """Each admin endpoint returns 403 when Settings.admin_key is empty."""
    cfg = get_config()
    patched = cfg.model_copy(update={"admin_key": ""})
    with patch("src.api.main.get_config", return_value=patched):
        client = _client()
        if method == "get":
            res = client.get(path)
        else:
            res = client.post(path)
        assert res.status_code == 403
        assert "Invalid or missing" in res.json().get("detail", "")


# --- Test 5: Promote returns correct response shape ---


def test_admin_promote_returns_correct_response_shape():
    """POST promote with valid admin_key returns 200 and {status, library_slug, active_index_name}."""
    from src.api import main as main_module
    cfg = get_config()
    patched = cfg.model_copy(update={"admin_key": "testkey"})
    mock_policy_repo = MagicMock()
    mock_policy_repo.promote = MagicMock()

    with patch.object(main_module, "get_config", return_value=patched), \
         patch.object(main_module, "_get_library_model_policy_repo", return_value=mock_policy_repo):
        client = _client()
        res = client.post(
            "/api/admin/libraries/myslug/model/promote",
            params={"shadow_index_name": "test_idx", "admin_key": "testkey"},
        )
        assert res.status_code == 200
        data = res.json()
        assert data == {
            "status": "promoted",
            "library_slug": "myslug",
            "active_index_name": "test_idx",
        }
        mock_policy_repo.promote.assert_called_once_with("myslug", "test_idx")


# --- Test 6: Rollback returns 400 when no previous index ---


def test_admin_rollback_400_when_no_previous_index():
    """POST rollback when policy has no previous index returns 400 with error message."""
    from src.api import main as main_module
    cfg = get_config()
    patched = cfg.model_copy(update={"admin_key": "testkey"})
    mock_policy_repo = MagicMock()
    mock_policy_repo.rollback = MagicMock(
        side_effect=ValueError("No previous index to roll back to for library 'myslug'")
    )

    with patch.object(main_module, "get_config", return_value=patched), \
         patch.object(main_module, "_get_library_model_policy_repo", return_value=mock_policy_repo):
        client = _client()
        res = client.post(
            "/api/admin/libraries/myslug/model/rollback",
            params={"admin_key": "testkey"},
        )
        assert res.status_code == 400
        data = res.json()
        assert "detail" in data
        assert "No previous index" in data["detail"]
