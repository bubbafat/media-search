"""Project API: /api/projects endpoints for Project Bins."""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from tests.conftest import clear_app_db_caches
from src.api.main import app, _get_asset_repo, _get_library_repo, _get_project_repo
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.project_repo import ProjectRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def project_api_postgres():
    """Dedicated Postgres with migrations applied; seed library and assets."""
    from sqlalchemy import create_engine

    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        clear_app_db_caches()
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

            with session_factory() as session:
                session.execute(
                    text(
                        """
                        INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit, deleted_at)
                        VALUES ('api-lib', 'API Library', '/mnt/api', true, 'idle', 100, NULL)
                        """
                    )
                )
                # Seed one asset
                session.execute(
                    text(
                        """
                        INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count)
                        VALUES ('api-lib', 'asset1.jpg', 'image', 100.0, 1000, 'completed', 0)
                        """
                    )
                )
                session.commit()

            asset_repo = AssetRepository(session_factory)
            library_repo = LibraryRepository(session_factory)
            project_repo = ProjectRepository(session_factory)
            yield asset_repo, library_repo, project_repo
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            clear_app_db_caches()


def test_projects_list_and_create(project_api_postgres):
    """GET /api/projects and POST /api/projects."""
    asset_repo, library_repo, project_repo = project_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    app.dependency_overrides[_get_project_repo] = lambda: project_repo
    try:
        client = TestClient(app)

        # Initially empty
        res = client.get("/api/projects")
        assert res.status_code == 200
        assert res.json() == []

        # Create a project
        res = client.post(
            "/api/projects",
            json={"name": "Export Bin API", "export_path": "/exports/api-bin"},
        )
        assert res.status_code == 201
        created = res.json()
        assert created["id"] > 0
        assert created["name"] == "Export Bin API"
        assert created["export_path"] == "/exports/api-bin"
        assert isinstance(created["created_at"], str)

        # List again and ensure project is present
        res = client.get("/api/projects")
        assert res.status_code == 200
        items = res.json()
        ids = {p["id"] for p in items}
        assert created["id"] in ids
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)
        app.dependency_overrides.pop(_get_project_repo, None)


def test_add_asset_to_project(project_api_postgres):
    """POST /api/projects/{id}/assets associates an asset and is idempotent."""
    asset_repo, library_repo, project_repo = project_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    app.dependency_overrides[_get_project_repo] = lambda: project_repo
    try:
        client = TestClient(app)

        # Create a project
        res = client.post("/api/projects", json={"name": "Bin With Asset"})
        assert res.status_code == 201
        project = res.json()

        # Look up seeded asset id
        with project_repo._session_factory() as session:
            row = session.execute(
                text(
                    "SELECT id FROM asset WHERE library_id = 'api-lib' AND rel_path = 'asset1.jpg'"
                )
            ).fetchone()
            assert row is not None
            asset_id = int(row[0])

        # Add asset to project (twice, idempotent)
        res = client.post(
            f"/api/projects/{project['id']}/assets",
            json={"asset_id": asset_id},
        )
        assert res.status_code == 204
        res = client.post(
            f"/api/projects/{project['id']}/assets",
            json={"asset_id": asset_id},
        )
        assert res.status_code == 204

        # Confirm association exists exactly once
        with project_repo._session_factory() as session:
            row = session.execute(
                text(
                    "SELECT COUNT(*) FROM project_assets WHERE project_id = :pid AND asset_id = :aid"
                ),
                {"pid": project["id"], "aid": asset_id},
            ).fetchone()
            assert row is not None
            assert int(row[0]) == 1
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)
        app.dependency_overrides.pop(_get_project_repo, None)


def test_add_asset_to_project_404s_for_missing_entities(project_api_postgres):
    """POST /api/projects/{id}/assets returns 404 for missing project or asset."""
    asset_repo, library_repo, project_repo = project_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    app.dependency_overrides[_get_project_repo] = lambda: project_repo
    try:
        client = TestClient(app)

        # Missing project
        res = client.post("/api/projects/999999/assets", json={"asset_id": 1})
        assert res.status_code == 404

        # Create a project
        res = client.post("/api/projects", json={"name": "Bin 404"})
        assert res.status_code == 201
        project = res.json()

        # Missing asset
        res = client.post(
            f"/api/projects/{project['id']}/assets",
            json={"asset_id": 999999},
        )
        assert res.status_code == 404
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)
        app.dependency_overrides.pop(_get_project_repo, None)

