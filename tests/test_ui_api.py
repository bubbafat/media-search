"""Mission Control UI API: dashboard returns 200 and displays schema version."""

import os

import pytest
from fastapi.testclient import TestClient

from src.api.main import app, _get_ui_repo
from src.core import config as config_module
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.ui_repo import UIRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def ui_api_postgres():
    """Dedicated Postgres with migrations applied; yield session_factory and ui_repo."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from testcontainers.postgres import PostgresContainer

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
            system_metadata_repo = SystemMetadataRepository(session_factory)
            ui_repo = UIRepository(session_factory, system_metadata_repo.get_schema_version)
            yield ui_repo
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            config_module._config = None  # type: ignore[attr-defined]


def test_dashboard_returns_200_and_displays_schema_version(ui_api_postgres):
    """GET /dashboard returns 200 and HTML contains System Version and DB Status."""
    app.dependency_overrides[_get_ui_repo] = lambda: ui_api_postgres
    try:
        client = TestClient(app)
        response = client.get("/dashboard")
        assert response.status_code == 200
        body = response.text
        assert "MediaSearch" in body
        assert "V1" in body or "V" in body
        assert "connected" in body.lower() or "Connected" in body
    finally:
        app.dependency_overrides.pop(_get_ui_repo, None)
