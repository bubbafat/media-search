"""System version guardrail: worker fails fast when schema_version is missing or mismatched."""

import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from src.core import config as config_module
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker


class _ConcreteWorker(BaseWorker):
    def process_task(self) -> None:
        pass


@pytest.fixture(scope="module")
def guard_postgres():
    """Dedicated Postgres + run migrations so system_metadata exists and is seeded."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        config_module._config = None  # type: ignore[attr-defined]
        try:
            from alembic import command
            from alembic.config import Config

            alembic_cfg = Config("alembic.ini")
            alembic_cfg.set_main_option("script_location", "migrations")
            command.upgrade(alembic_cfg, "head")

            engine = create_engine(url, pool_pre_ping=True)
            session_factory = sessionmaker(
                engine, autocommit=False, autoflush=False, expire_on_commit=False
            )
            yield engine, session_factory
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            config_module._config = None  # type: ignore[attr-defined]


def test_compatibility_passes_when_schema_version_matches(guard_postgres):
    """Worker _check_compatibility() does not raise when schema_version is '1'."""
    engine, session_factory = guard_postgres
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)
    worker = _ConcreteWorker(
        "guard-test-1",
        worker_repo,
        heartbeat_interval_seconds=60,
        system_metadata_repo=system_metadata_repo,
    )
    worker._check_compatibility()  # no raise


def test_compatibility_fails_when_schema_version_mismatch(guard_postgres):
    """Worker _check_compatibility() raises RuntimeError when schema_version is '2'."""
    engine, session_factory = guard_postgres
    with engine.connect() as conn:
        conn.execute(text("UPDATE system_metadata SET value = '2' WHERE key = 'schema_version'"))
        conn.commit()

    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)
    worker = _ConcreteWorker(
        "guard-test-2",
        worker_repo,
        heartbeat_interval_seconds=60,
        system_metadata_repo=system_metadata_repo,
    )
    with pytest.raises(RuntimeError) as exc_info:
        worker._check_compatibility()
    assert "schema_version" in str(exc_info.value)
    assert "2" in str(exc_info.value)
    assert "1" in str(exc_info.value)


def test_compatibility_fails_when_schema_version_missing(guard_postgres):
    """Worker _check_compatibility() raises RuntimeError when schema_version row is missing."""
    engine, session_factory = guard_postgres
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM system_metadata WHERE key = 'schema_version'"))
        conn.commit()

    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)
    worker = _ConcreteWorker(
        "guard-test-3",
        worker_repo,
        heartbeat_interval_seconds=60,
        system_metadata_repo=system_metadata_repo,
    )
    with pytest.raises(RuntimeError) as exc_info:
        worker._check_compatibility()
    assert "missing" in str(exc_info.value).lower() or "schema_version" in str(exc_info.value)
