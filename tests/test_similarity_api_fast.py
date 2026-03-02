"""Fast tests for similarity API and Quickwit similarity algorithm (no DB, no Quickwit)."""

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, _get_asset_repo, _get_ui_repo
from src.core.config import get_config, reset_config
from src.models.entities import Asset, AssetStatus, AssetType
from src.models.similarity import SimilarityScope

pytestmark = [pytest.mark.fast]


def _client() -> TestClient:
    return TestClient(app)


def _make_asset(asset_id: int = 1) -> Asset:
    return Asset(
        id=asset_id,
        library_id="tuta",
        rel_path="photos/a.jpg",
        type=AssetType.image,
        mtime=0.0,
        size=1,
        status=AssetStatus.completed,
        visual_analysis={"description": "a red car", "tags": ["car"]},
    )


def test_invalid_scope_json_returns_422():
    """Invalid JSON in scope query parameter returns HTTP 422."""
    asset = _make_asset()
    asset_repo = MagicMock()
    asset_repo.get_asset_by_id.return_value = asset
    ui_repo = MagicMock()
    ui_repo.get_library_names.return_value = {}

    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo

    try:
        cfg = get_config()
        patched = cfg.model_copy(update={"quickwit_enabled": True})
        with patch("src.api.main.get_config", return_value=patched), patch(
            "src.api.main.LibraryModelPolicyRepository"
        ) as MockPolicyRepo, patch("src.api.main._get_quickwit_search_repo") as mock_get_qw:
            MockPolicyRepo.return_value.get_active_index_names_for_libraries.return_value = [
                "idx"
            ]
            mock_qw = MagicMock()
            mock_qw.is_healthy.return_value = True
            mock_qw.find_similar.return_value = ([], patched.similarity_min_score)
            mock_get_qw.return_value = mock_qw

            client = _client()
            res = client.get("/api/assets/1/similar", params={"scope": "not-json"})
            assert res.status_code == 422
            assert "Invalid scope parameter" in res.json().get("detail", "")
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)
        reset_config()


def test_source_asset_excluded_id_passed_through():
    """API passes the source asset id as exclude_asset_id to Quickwit repository."""
    asset = _make_asset(asset_id=123)
    asset_repo = MagicMock()
    asset_repo.get_asset_by_id.return_value = asset
    ui_repo = MagicMock()
    ui_repo.get_library_names.return_value = {}

    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo

    try:
        cfg = get_config()
        patched = cfg.model_copy(update={"quickwit_enabled": True})
        with patch("src.api.main.get_config", return_value=patched), patch(
            "src.api.main.LibraryModelPolicyRepository"
        ) as MockPolicyRepo, patch("src.api.main._get_quickwit_search_repo") as mock_get_qw:
            MockPolicyRepo.return_value.get_active_index_names_for_libraries.return_value = [
                "idx"
            ]
            mock_qw = MagicMock()
            mock_qw.is_healthy.return_value = True
            mock_qw.find_similar.return_value = ([], patched.similarity_min_score)
            mock_get_qw.return_value = mock_qw

            client = _client()
            res = client.get("/api/assets/123/similar")
            assert res.status_code == 200
            assert mock_qw.find_similar.called
            kwargs = mock_qw.find_similar.call_args.kwargs
            assert kwargs["exclude_asset_id"] == 123
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)
        reset_config()


def test_adaptive_threshold_algorithm_steps_down_until_min_results():
    """find_similar retries with decreasing threshold until enough results are found."""
    from src.repository.quickwit_search_repo import QuickwitSearchRepository

    # Prepare a scope that does not add extra filters.
    scope = SimilarityScope()

    calls: list[float] = []

    def fake_post(url, json=None, timeout=None):  # type: ignore[override]
        # json["query"] contains the combined query + filters; threshold is tracked separately.
        # We simulate three attempts via len(calls).
        attempt = len(calls) + 1
        # First two attempts: scores below high thresholds; third: above floor.
        if attempt < 3:
            score = 0.3
        else:
            score = 0.8
        payload = {
            "hits": [
                {
                    "asset_id": 10,
                    "library_slug": "tuta",
                    "scene_start_ts": None,
                    "rep_frame_path": None,
                    "score": score,
                }
            ]
        }

        class _Resp:
            def raise_for_status(self) -> None:
                return None

            def json(self):
                return payload

        calls.append(score)
        return _Resp()

    repo = QuickwitSearchRepository(base_url="http://quickwit", active_index_name="idx")

    with patch("src.repository.quickwit_search_repo.httpx.post", wraps=fake_post):
        results, threshold_used = repo.find_similar(
            description="a red car",
            exclude_asset_id=1,
            scope=scope,
            max_results=10,
            min_score=0.65,
            floor=0.35,
            step=0.10,
            min_results=1,
        )

    # We expect to reach the floor (0.35) on the third attempt before accepting results.
    assert threshold_used == pytest.approx(0.35)
    assert len(results) == 1


def test_sanitize_query_removes_double_quotes():
    """_sanitize_query removes double quotes from a description containing them."""
    from src.repository.quickwit_search_repo import QuickwitSearchRepository

    repo = QuickwitSearchRepository(base_url="http://qw", active_index_name="idx")
    assert repo._sanitize_query('a "quoted" phrase') == "a quoted phrase"


def test_sanitize_query_removes_all_special_characters():
    """_sanitize_query removes all Quickwit-special characters."""
    from src.repository.quickwit_search_repo import QuickwitSearchRepository

    repo = QuickwitSearchRepository(base_url="http://qw", active_index_name="idx")
    raw = '"()[]{}:^~*?\\/+\\-!&&||'
    assert repo._sanitize_query(raw) == ""


def test_sanitize_query_collapses_multiple_spaces():
    """_sanitize_query collapses multiple spaces into one."""
    from src.repository.quickwit_search_repo import QuickwitSearchRepository

    repo = QuickwitSearchRepository(base_url="http://qw", active_index_name="idx")
    assert repo._sanitize_query("a   red   car") == "a red car"


def test_sanitize_query_returns_clean_description_unchanged():
    """_sanitize_query on a clean description returns it unchanged (modulo stripping)."""
    from src.repository.quickwit_search_repo import QuickwitSearchRepository

    repo = QuickwitSearchRepository(base_url="http://qw", active_index_name="idx")
    assert repo._sanitize_query("a red car") == "a red car"
    assert repo._sanitize_query("  a red car  ") == "a red car"
