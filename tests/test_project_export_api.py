"""Project export API: /api/projects/{id}/export hard-linking."""

import errno
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from tests.conftest import clear_app_db_caches
from src.api.main import app, _get_project_repo
from src.repository.project_repo import ProjectRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def project_export_postgres():
    """Dedicated Postgres with migrations applied; seed library, assets, and project."""
    from sqlalchemy import create_engine

    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev_db = os.environ.get("DATABASE_URL")
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
                        VALUES ('export-lib', 'Export Library', :abs, true, 'idle', 100, NULL)
                        """
                    ),
                    {"abs": "/tmp/export-lib"},
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count)
                        VALUES ('export-lib', 'one.mov', 'video', 100.0, 1000, 'completed', 0),
                               ('export-lib', 'two.mov', 'video', 100.0, 2000, 'completed', 0)
                        """
                    )
                )
                session.execute(
                    text(
                        """
                        INSERT INTO project (name)
                        VALUES ('Export Test Project')
                        RETURNING id
                        """
                    )
                )
                row = session.execute(text("SELECT id FROM project WHERE name = 'Export Test Project'")).fetchone()
                assert row is not None
                project_id = int(row[0])
                session.execute(
                    text(
                        """
                        INSERT INTO project_assets (project_id, asset_id)
                        SELECT :pid, id FROM asset WHERE library_id = 'export-lib'
                        """
                    ),
                    {"pid": project_id},
                )
                session.commit()

            project_repo = ProjectRepository(session_factory)
            yield project_repo
        finally:
            if prev_db is not None:
                os.environ["DATABASE_URL"] = prev_db
            else:
                os.environ.pop("DATABASE_URL", None)
            clear_app_db_caches()


def test_export_project_hard_links(project_export_postgres, monkeypatch):
    """POST /api/projects/{id}/export creates hard links under EXPORT_ROOT_PATH."""
    project_repo = project_export_postgres
    app.dependency_overrides[_get_project_repo] = lambda: project_repo
    prev_export_root = os.environ.get("EXPORT_ROOT_PATH")
    with TemporaryDirectory() as src_dir, TemporaryDirectory() as export_root:
        try:
            # Point library absolute_path to src_dir
            with project_repo._session_factory() as session:
                session.execute(
                    text(
                        "UPDATE library SET absolute_path = :abs WHERE slug = 'export-lib'"
                    ),
                    {"abs": src_dir},
                )
                session.commit()

            # Create source files that match rel_paths
            src_one = Path(src_dir) / "one.mov"
            src_two = Path(src_dir) / "two.mov"
            src_one.write_bytes(b"aaa")
            src_two.write_bytes(b"bbb")

            os.environ["EXPORT_ROOT_PATH"] = export_root
            clear_app_db_caches()

            client = TestClient(app)

            # Look up project id
            with project_repo._session_factory() as session:
                row = session.execute(
                    text("SELECT id FROM project WHERE name = 'Export Test Project'")
                ).fetchone()
                assert row is not None
                project_id = int(row[0])

            res = client.post(f"/api/projects/{project_id}/export")
            assert res.status_code == 200
            data = res.json()
            export_path = Path(data["export_path"])
            assert export_path.is_dir()

            dest_one = export_path / "one.mov"
            dest_two = export_path / "two.mov"
            assert dest_one.exists()
            assert dest_two.exists()

            # Confirm hard links by inode
            assert os.stat(src_one).st_ino == os.stat(dest_one).st_ino
            assert os.stat(src_two).st_ino == os.stat(dest_two).st_ino
        finally:
            app.dependency_overrides.pop(_get_project_repo, None)
            if prev_export_root is not None:
                os.environ["EXPORT_ROOT_PATH"] = prev_export_root
            else:
                os.environ.pop("EXPORT_ROOT_PATH", None)
            clear_app_db_caches()


def test_export_project_missing_config_returns_400(project_export_postgres):
    """Missing EXPORT_ROOT_PATH should result in a 400-style error."""
    project_repo = project_export_postgres
    app.dependency_overrides[_get_project_repo] = lambda: project_repo
    prev_export_root = os.environ.get("EXPORT_ROOT_PATH")
    try:
        os.environ.pop("EXPORT_ROOT_PATH", None)
        clear_app_db_caches()

        client = TestClient(app)
        with project_repo._session_factory() as session:
            row = session.execute(
                text("SELECT id FROM project WHERE name = 'Export Test Project'")
            ).fetchone()
            assert row is not None
            project_id = int(row[0])

        res = client.post(f"/api/projects/{project_id}/export")
        assert res.status_code == 400
        body = res.json()
        assert "EXPORT_ROOT_PATH" in body.get("detail", "")
    finally:
        app.dependency_overrides.pop(_get_project_repo, None)
        if prev_export_root is not None:
            os.environ["EXPORT_ROOT_PATH"] = prev_export_root
        clear_app_db_caches()


def test_export_project_cross_device_error_returns_400(project_export_postgres, monkeypatch):
    """Cross-device link errors should be surfaced as 400 with guidance."""
    project_repo = project_export_postgres
    app.dependency_overrides[_get_project_repo] = lambda: project_repo
    prev_export_root = os.environ.get("EXPORT_ROOT_PATH")
    with TemporaryDirectory() as export_root:
        try:
            os.environ["EXPORT_ROOT_PATH"] = export_root
            clear_app_db_caches()

            # Monkeypatch os.link used in the API module
            def fake_link(src, dst):
                raise OSError(errno.EXDEV, "Invalid cross-device link")

            monkeypatch.setattr("src.api.main.os.link", fake_link)

            client = TestClient(app)
            with project_repo._session_factory() as session:
                row = session.execute(
                    text("SELECT id FROM project WHERE name = 'Export Test Project'")
                ).fetchone()
                assert row is not None
                project_id = int(row[0])

            res = client.post(f"/api/projects/{project_id}/export")
            assert res.status_code == 400
            body = res.json()
            assert "cross-device link error" in body.get("detail", "").lower()
        finally:
            app.dependency_overrides.pop(_get_project_repo, None)
            if prev_export_root is not None:
                os.environ["EXPORT_ROOT_PATH"] = prev_export_root
            else:
                os.environ.pop("EXPORT_ROOT_PATH", None)
            clear_app_db_caches()

