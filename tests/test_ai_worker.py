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

pytestmark = [pytest.mark.slow]


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
    """process_task returns False when no asset to claim (proxied for light, analyzed_light for full)."""
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
        library_slug="empty-ai",
    )
    result = worker.process_task()
    assert result is False


def test_ai_worker_process_task_light_mode_marks_analyzed_light(engine, _session_factory):
    """process_task in light mode claims proxied, saves visual_analysis, marks analyzed_light."""
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
            mode="light",
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
            assert status == "analyzed_light"
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


def test_ai_worker_process_task_batch_processes_multiple_assets(engine, _session_factory):
    """process_task with batch_size>1 claims and processes multiple assets in parallel."""
    _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="batch-lib",
                name="Batch Lib",
                absolute_path="/tmp/batch-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo = AssetRepository(_session_factory)
    asset_repo.upsert_asset("batch-lib", "a.jpg", AssetType.image, 0.0, 100)
    asset_repo.upsert_asset("batch-lib", "b.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(text("UPDATE asset SET status = 'proxied' WHERE library_id = 'batch-lib'"))
        session.commit()
    finally:
        session.close()

    worker_repo = WorkerRepository(_session_factory)
    system_repo = SystemMetadataRepository(_session_factory)
    with tempfile.TemporaryDirectory() as tmpdir:
        proxy_a = Path(tmpdir) / "a.jpg"
        proxy_b = Path(tmpdir) / "b.jpg"
        proxy_a.touch()
        proxy_b.touch()
        worker = AIWorker(
            worker_id="ai-batch",
            repository=worker_repo,
            heartbeat_interval_seconds=15.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_repo,
            library_slug="batch-lib",
            batch_size=2,
            mode="light",
        )
        worker.storage.get_proxy_path = MagicMock(return_value=proxy_a)
        with patch("src.ai.vision_base.time.sleep"):
            result = worker.process_task()
        assert result is True

        session = _session_factory()
        try:
            rows = session.execute(
                text("SELECT rel_path, status FROM asset WHERE library_id = 'batch-lib' ORDER BY rel_path")
            ).fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "a.jpg" and rows[0][1] == "analyzed_light"
            assert rows[1][0] == "b.jpg" and rows[1][1] == "analyzed_light"
        finally:
            session.close()


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
        mode="light",
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


def test_ai_worker_process_task_full_mode_merges_ocr_and_marks_completed(engine, _session_factory):
    """process_task in full mode claims analyzed_light, merges OCR into visual_analysis, marks completed."""
    _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="full-lib",
                name="Full Lib",
                absolute_path="/tmp/full-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo = AssetRepository(_session_factory)
    asset_repo.upsert_asset("full-lib", "photo.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text("""
                UPDATE asset SET
                    status = 'analyzed_light',
                    visual_analysis = '{"description": "Light desc", "tags": ["light"], "ocr_text": null}'::jsonb
                WHERE library_id = 'full-lib' AND rel_path = 'photo.jpg'
            """)
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
            worker_id="ai-full",
            repository=worker_repo,
            heartbeat_interval_seconds=15.0,
            asset_repo=asset_repo,
            system_metadata_repo=system_repo,
            library_slug="full-lib",
            mode="full",
        )
        worker.storage.get_proxy_path = MagicMock(return_value=proxy_path)
        with patch("src.ai.vision_base.time.sleep"):
            result = worker.process_task()
        assert result is True

        session = _session_factory()
        try:
            row = session.execute(
                text("SELECT id, status, analysis_model_id, worker_id, visual_analysis FROM asset WHERE library_id = 'full-lib' AND rel_path = 'photo.jpg'")
            ).fetchone()
            assert row is not None
            asset_id, status, analysis_model_id, worker_id, visual_analysis = row
            assert status == "completed"
            assert analysis_model_id is not None
            assert worker_id is None
            assert visual_analysis is not None
            assert visual_analysis.get("description") == "Light desc"
            assert visual_analysis.get("tags") == ["light"]
            assert visual_analysis.get("ocr_text") == "MOCK TEXT"
        finally:
            session.close()
    finally:
        proxy_path.unlink(missing_ok=True)
