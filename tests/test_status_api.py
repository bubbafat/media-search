"""Status API: GET /api/status. All tests are fast (mocked DB, Quickwit, workers)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.core.config import get_config

pytestmark = [pytest.mark.fast]


def _client():
    return TestClient(app)


def test_status_returns_200_always():
    """Even when PostgreSQL is mocked to fail, the endpoint returns HTTP 200."""
    with patch("src.api.main._get_session_factory") as mock_sf:
        mock_session = MagicMock()
        mock_session.execute = MagicMock(side_effect=Exception("connection refused"))
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_session)
        ctx.__exit__ = MagicMock(return_value=False)
        mock_sf.return_value.return_value = ctx
        mock_worker_repo = MagicMock()
        mock_worker_repo.list_all.return_value = []
        with patch("src.api.main._get_worker_repo", return_value=mock_worker_repo), \
             patch("src.api.main.get_config") as mock_cfg:
            mock_cfg.return_value = get_config().model_copy(update={"quickwit_enabled": False})
            client = _client()
            res = client.get("/api/status")
        assert res.status_code == 200
        data = res.json()
        assert "status" in data
        assert data["components"]["postgres"]["status"] == "unavailable"


def test_status_healthy_when_all_components_ok():
    """All components healthy, top-level status is 'healthy'."""
    with patch("src.api.main._get_session_factory") as mock_sf:
        mock_session = MagicMock()
        mock_session.execute = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_session)
        ctx.__exit__ = MagicMock(return_value=False)
        mock_sf.return_value.return_value = ctx
        mock_worker_repo = MagicMock()
        now = datetime.now(timezone.utc)
        worker = MagicMock()
        worker.last_seen_at = now
        mock_worker_repo.list_all.return_value = [worker]
        with patch("src.api.main._get_worker_repo", return_value=mock_worker_repo), \
             patch("src.repository.quickwit_search_repo.QuickwitSearchRepository") as mock_qw_cls:
            mock_qw_cls.return_value.is_healthy.return_value = True
            with patch("src.api.main.get_config") as mock_cfg:
                mock_cfg.return_value = get_config().model_copy(update={"quickwit_enabled": True})
                client = _client()
                res = client.get("/api/status")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "healthy"
        assert data["components"]["postgres"]["status"] == "healthy"
        assert data["components"]["quickwit"]["status"] == "healthy"
        assert data["components"]["workers"]["status"] == "healthy"


def test_status_degraded_when_quickwit_unavailable():
    """Quickwit is_healthy returns False, top-level status is 'degraded', quickwit component 'unavailable'."""
    with patch("src.api.main._get_session_factory") as mock_sf:
        mock_session = MagicMock()
        mock_session.execute = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_session)
        ctx.__exit__ = MagicMock(return_value=False)
        mock_sf.return_value.return_value = ctx
        mock_worker_repo = MagicMock()
        now = datetime.now(timezone.utc)
        worker = MagicMock()
        worker.last_seen_at = now
        mock_worker_repo.list_all.return_value = [worker]
        with patch("src.api.main._get_worker_repo", return_value=mock_worker_repo), \
             patch("src.repository.quickwit_search_repo.QuickwitSearchRepository") as mock_qw_cls:
            mock_qw_cls.return_value.is_healthy.return_value = False
            with patch("src.api.main.get_config") as mock_cfg:
                mock_cfg.return_value = get_config().model_copy(
                    update={"quickwit_enabled": True, "quickwit_url": "http://qw:7280"}
                )
                client = _client()
                res = client.get("/api/status")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "degraded"
        assert data["components"]["quickwit"]["status"] == "unavailable"
        assert "Quickwit is not reachable" in data["components"]["quickwit"]["detail"]


def test_status_quickwit_disabled_when_flag_off():
    """quickwit_enabled=False: quickwit component status is 'disabled', top-level status is 'healthy'."""
    with patch("src.api.main._get_session_factory") as mock_sf:
        mock_session = MagicMock()
        mock_session.execute = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=mock_session)
        ctx.__exit__ = MagicMock(return_value=False)
        mock_sf.return_value.return_value = ctx
        mock_worker_repo = MagicMock()
        now = datetime.now(timezone.utc)
        worker = MagicMock()
        worker.last_seen_at = now
        mock_worker_repo.list_all.return_value = [worker]
        with patch("src.api.main._get_worker_repo", return_value=mock_worker_repo), \
             patch("src.api.main.get_config") as mock_cfg:
            mock_cfg.return_value = get_config().model_copy(update={"quickwit_enabled": False})
            client = _client()
            res = client.get("/api/status")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert data["components"]["quickwit"]["status"] == "disabled"
    assert "quickwit_enabled is false" in data["components"]["quickwit"]["detail"]
