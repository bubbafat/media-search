"""Fast tests for AI worker transient vs asset-level error handling (mocks only, no DB)."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests.exceptions

from src.models.entities import AssetStatus
from src.workers.ai_worker import AIWorker

pytestmark = [pytest.mark.fast]


def _make_worker_and_claim_one_asset(asset_id: int = 42, library_slug: str = "lib", rel_path: str = "x.jpg"):
    """Build AIWorker with mocked repos; claim_assets_by_status returns one asset."""
    asset_repo = MagicMock()
    library = MagicMock()
    library.slug = library_slug
    asset = MagicMock()
    asset.id = asset_id
    asset.library = library
    asset.rel_path = rel_path
    asset_repo.claim_assets_by_status.return_value = [asset]

    system_repo = MagicMock()
    system_repo.get_or_create_ai_model.return_value = 1

    worker_repo = MagicMock()
    worker_repo.get_active_local_worker_count.return_value = 0

    worker = AIWorker(
        worker_id="ai-transient-test",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_repo,
        library_slug=library_slug,
        mode="light",
    )
    worker.storage = MagicMock()
    worker.storage.get_proxy_path.return_value = Path("/tmp/fake.jpg")
    return worker, asset_repo, asset_id


def test_ai_worker_transient_error_resets_status_and_logs_warning(caplog):
    """On ConnectionError, asset status is reset to pre-claim state; WARNING logged; not marked failed."""
    worker, asset_repo, asset_id = _make_worker_and_claim_one_asset()
    worker.analyzer.analyze_image = MagicMock(
        side_effect=requests.exceptions.ConnectionError("Connection refused")
    )

    with caplog.at_level(logging.WARNING, logger="src.workers.ai_worker"):
        result = worker.process_task()

    assert result is True
    # Reset to claim_status (proxied for light), no error message; must not be poisoned.
    asset_repo.update_asset_status.assert_called()
    calls = asset_repo.update_asset_status.call_args_list
    # Exactly one update: reset to proxied
    assert len(calls) == 1
    call = calls[0]
    assert call[0][0] == asset_id
    assert call[0][1] == AssetStatus.proxied
    assert call[0][2] is None
    assert call[1].get("owned_by") == worker.worker_id
    # No call with poisoned
    assert not any(c[0][1] == AssetStatus.poisoned for c in calls)
    # WARNING logged
    assert any(
        "transient" in record.getMessage().lower() and record.levelno == logging.WARNING
        for record in caplog.records
    )


def test_ai_worker_asset_error_still_marks_poisoned_and_stores_message():
    """On non-network exception (e.g. corrupt file), asset is marked poisoned and error message stored."""
    worker, asset_repo, asset_id = _make_worker_and_claim_one_asset()
    worker.analyzer.analyze_image = MagicMock(
        side_effect=ValueError("Unsupported format or corrupt file")
    )

    result = worker.process_task()

    assert result is True
    asset_repo.update_asset_status.assert_called_once()
    call = asset_repo.update_asset_status.call_args
    assert call[0][0] == asset_id
    assert call[0][1] == AssetStatus.poisoned
    assert "Unsupported format or corrupt file" in (call[0][2] or "")
    assert call[1].get("owned_by") == worker.worker_id
