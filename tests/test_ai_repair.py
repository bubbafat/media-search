"""Tests for AI worker --repair: _run_repair_pass sets assets to proxied when effective model changed (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AIModel, AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.ai_worker import AIWorker

pytestmark = [pytest.mark.slow]


def _create_tables_and_repos(engine, session_factory):
    """Create tables, seed schema_version and default_ai_model_id, return repos."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()

    session = session_factory()
    try:
        session.add(AIModel(name="repair_model_a", version="1"))
        session.add(AIModel(name="repair_model_b", version="1"))
        session.commit()
        rows = session.execute(text("SELECT id FROM aimodel WHERE name IN ('repair_model_a', 'repair_model_b') ORDER BY name")).fetchall()
        id_a, id_b = rows[0][0], rows[1][0]
    finally:
        session.close()

    system_metadata_repo = SystemMetadataRepository(session_factory)
    system_metadata_repo.set_value(SystemMetadataRepository.DEFAULT_AI_MODEL_ID_KEY, str(id_a))

    return (
        AssetRepository(session_factory),
        WorkerRepository(session_factory),
        SystemMetadataRepository(session_factory),
        LibraryRepository(session_factory),
        id_a,
        id_b,
    )


def test_ai_repair_sets_to_proxied_when_analysis_model_differs_from_effective(engine, _session_factory):
    """Repair pass sets status to proxied when asset.analysis_model_id != library effective target."""
    asset_repo, worker_repo, system_metadata_repo, library_repo, id_a, id_b = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-ai-lib",
                name="Repair AI",
                absolute_path="/tmp/repair-ai",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=None,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-ai-lib", "photo.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'completed', analysis_model_id = :bid WHERE library_id = 'repair-ai-lib'"
            ),
            {"bid": id_b},
        )
        session.commit()
    finally:
        session.close()

    worker = AIWorker(
        worker_id="ai-repair-worker",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        library_slug="repair-ai-lib",
        system_default_model_id=id_a,
        repair=True,
        library_repo=library_repo,
    )
    worker._run_repair_pass()

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE library_id = 'repair-ai-lib'")
        ).fetchone()
        assert row is not None
        assert row[0] == "proxied"
    finally:
        session.close()


def test_ai_repair_leaves_status_when_analysis_model_matches_effective(engine, _session_factory):
    """Repair pass leaves status unchanged when analysis_model_id equals effective target."""
    asset_repo, worker_repo, system_metadata_repo, library_repo, id_a, _id_b = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-ok-ai-lib",
                name="Repair OK AI",
                absolute_path="/tmp/repair-ok-ai",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=None,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-ok-ai-lib", "photo.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'completed', analysis_model_id = :aid WHERE library_id = 'repair-ok-ai-lib'"
            ),
            {"aid": id_a},
        )
        session.commit()
    finally:
        session.close()

    worker = AIWorker(
        worker_id="ai-repair-worker",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        library_slug="repair-ok-ai-lib",
        system_default_model_id=id_a,
        repair=True,
        library_repo=library_repo,
    )
    worker._run_repair_pass()

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT status FROM asset WHERE library_id = 'repair-ok-ai-lib'")
        ).fetchone()
        assert row is not None
        assert row[0] == "completed"
    finally:
        session.close()


def test_ai_repair_respects_library_slug(engine, _session_factory):
    """Repair with library_slug only resets assets in that library."""
    asset_repo, worker_repo, system_metadata_repo, library_repo, id_a, id_b = _create_tables_and_repos(
        engine, _session_factory
    )
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="repair-slug-a",
                name="A",
                absolute_path="/tmp/repair-a",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=id_a,
            )
        )
        session.add(
            Library(
                slug="repair-slug-b",
                name="B",
                absolute_path="/tmp/repair-b",
                is_active=True,
                sampling_limit=100,
                target_tagger_id=id_a,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("repair-slug-a", "a.jpg", AssetType.image, 0.0, 100)
    asset_repo.upsert_asset("repair-slug-b", "b.jpg", AssetType.image, 0.0, 100)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET status = 'completed', analysis_model_id = :bid WHERE library_id IN ('repair-slug-a', 'repair-slug-b')"
            ),
            {"bid": id_b},
        )
        session.commit()
    finally:
        session.close()

    worker = AIWorker(
        worker_id="ai-repair-worker",
        repository=worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        library_slug="repair-slug-a",
        system_default_model_id=id_a,
        repair=True,
        library_repo=library_repo,
    )
    worker._run_repair_pass()

    session = _session_factory()
    try:
        row_a = session.execute(text("SELECT status FROM asset WHERE library_id = 'repair-slug-a'")).fetchone()
        row_b = session.execute(text("SELECT status FROM asset WHERE library_id = 'repair-slug-b'")).fetchone()
        assert row_a is not None and row_a[0] == "proxied"
        assert row_b is not None and row_b[0] == "completed"
    finally:
        session.close()
