"""Tests for AI worker process_task: claim proxied, analyze, save, mark completed or poisoned."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.ai_worker import AIWorker


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()


def test_ai_worker_process_task_returns_false_when_no_asset(engine, _session_factory):
    """process_task returns False when no proxied asset to claim."""
    _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="empty-ai",
                name="Empty",
                absolute_path="/tmp/empty",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo = AssetRepository(_session_factory)
    worker_repo = WorkerRepository(_session_factory)
    system_repo = SystemMetadataRepository(_session_factory)
    worker = AIWorker(
        worker_id="ai-test-1",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_repo,
    )
    result = worker.process_task()
    assert result is False


def test_ai_worker_process_task_completes_asset_and_saves_analysis(engine, _session_factory):
    """process_task claims proxied asset, runs analyzer, saves visual_analysis, marks completed."""
    _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="ai-lib",
                name="AI Lib",
                absolute_path="/tmp/ai-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo = AssetRepository(_session_factory)
    asset_repo.upsert_asset("ai-lib", "photo.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE library_id = 'ai-lib' AND rel_path = 'photo.jpg'")
        )
        session.commit()
    finally:
        session.close()

    worker_repo = WorkerRepository(_session_factory)
    system_repo = SystemMetadataRepository(_session_factory)
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        proxy_path = Path(f.name)
    try:
        worker = AIWorker(
            worker_id="ai-test-2",
            repository=worker_repo,
            heartbeat_interval_seconds=15.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_repo,
            library_slug="ai-lib",
        )
        worker.storage.get_proxy_path = MagicMock(return_value=proxy_path)
        with patch("src.ai.vision_base.time.sleep"):
            result = worker.process_task()
        assert result is True

        session = _session_factory()
        try:
            row = session.execute(
                text("SELECT id, status, analysis_model_id, worker_id, visual_analysis FROM asset WHERE library_id = 'ai-lib' AND rel_path = 'photo.jpg'")
            ).fetchone()
            assert row is not None
            asset_id, status, analysis_model_id, worker_id, visual_analysis = row
            assert status == "completed"
            assert analysis_model_id is not None
            assert worker_id is None
            assert visual_analysis is not None
            assert visual_analysis.get("description") == "A placeholder description."
            assert "mock" in visual_analysis.get("tags", [])
            assert visual_analysis.get("ocr_text") == "MOCK TEXT"
        finally:
            session.close()
    finally:
        proxy_path.unlink(missing_ok=True)


def test_ai_worker_process_task_poisons_on_exception(engine, _session_factory):
    """process_task sets status to poisoned and error_message when analyzer raises."""
    _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="poison-lib",
                name="Poison",
                absolute_path="/tmp/poison",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo = AssetRepository(_session_factory)
    asset_repo.upsert_asset("poison-lib", "bad.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'proxied' WHERE library_id = 'poison-lib' AND rel_path = 'bad.jpg'")
        )
        session.commit()
    finally:
        session.close()

    worker_repo = WorkerRepository(_session_factory)
    system_repo = SystemMetadataRepository(_session_factory)
    worker = AIWorker(
        worker_id="ai-test-3",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_repo,
        library_slug="poison-lib",
    )
    worker.storage.get_proxy_path = MagicMock(return_value=Path("/tmp/fake.jpg"))
    worker.analyzer.analyze_image = MagicMock(side_effect=RuntimeError("Analyzer failed"))

    result = worker.process_task()
    assert result is True

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status, error_message FROM asset WHERE library_id = 'poison-lib' AND rel_path = 'bad.jpg'")
        ).fetchone()
        assert row is not None
        assert row[0] == "poisoned"
        assert "Analyzer failed" in (row[1] or "")
    finally:
        session.close()
