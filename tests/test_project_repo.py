"""Project repository tests: create projects and manage project assets (testcontainers Postgres)."""

import os
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from tests.conftest import clear_app_db_caches
from src.repository.project_repo import ProjectRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def project_repo_postgres():
    """Dedicated Postgres with migrations applied; yield ProjectRepository and session_factory."""
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

            project_repo = ProjectRepository(session_factory)
            yield project_repo, session_factory
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            clear_app_db_caches()


def _seed_library_and_assets(session_factory) -> tuple[str, list[int]]:
    """Seed one library and a couple of assets; return (library_slug, asset_ids)."""
    with session_factory() as session:
        session.execute(
            text(
                """
                INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit, deleted_at)
                VALUES ('proj-lib', 'Project Library', '/mnt/proj', true, 'idle', 100, NULL)
                ON CONFLICT (slug) DO NOTHING
                """
            )
        )
        asset_ids: list[int] = []
        for idx, rel_path in enumerate(["a.jpg", "b.mp4"], start=1):
            row = session.execute(
                text(
                    """
                    INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count)
                    VALUES ('proj-lib', :rel_path, :type, :mtime, :size, 'completed', 0)
                    ON CONFLICT (library_id, rel_path) DO UPDATE SET
                        type = EXCLUDED.type,
                        mtime = EXCLUDED.mtime,
                        size = EXCLUDED.size,
                        status = EXCLUDED.status
                    RETURNING id
                    """
                ),
                {
                    "rel_path": rel_path,
                    "type": "image" if rel_path.endswith(".jpg") else "video",
                    "mtime": float(100 + idx),
                    "size": 1000 * idx,
                },
            ).fetchone()
            assert row is not None
            asset_ids.append(int(row[0]))
        session.commit()
    return "proj-lib", asset_ids


def test_create_and_list_projects(project_repo_postgres):
    """create_project() persists a project and list_projects() returns it."""
    project_repo, _session_factory = project_repo_postgres
    project = project_repo.create_project("Export Bin 1", "/exports/bin1")
    assert project.id is not None
    assert project.name == "Export Bin 1"
    assert project.export_path == "/exports/bin1"
    assert project.created_at is not None

    projects = project_repo.list_projects()
    ids = {p.id for p in projects}
    assert project.id in ids


def test_add_remove_and_get_project_assets(project_repo_postgres):
    """add_asset_to_project(), remove_asset_from_project(), get_project_assets() behave correctly."""
    project_repo, session_factory = project_repo_postgres
    _, asset_ids = _seed_library_and_assets(session_factory)
    project = project_repo.create_project("Export Bin 2", None)

    # Add two assets, including a duplicate insert (idempotent)
    project_repo.add_asset_to_project(project.id or 0, asset_ids[0])
    project_repo.add_asset_to_project(project.id or 0, asset_ids[1])
    project_repo.add_asset_to_project(project.id or 0, asset_ids[0])

    paths = project_repo.get_project_assets(project.id or 0)
    expected_paths = {
        str(Path("/mnt/proj") / "a.jpg"),
        str(Path("/mnt/proj") / "b.mp4"),
    }
    assert set(paths) == expected_paths

    # Remove one asset and ensure paths update
    project_repo.remove_asset_from_project(project.id or 0, asset_ids[0])
    paths_after = project_repo.get_project_assets(project.id or 0)
    assert set(paths_after) == {str(Path("/mnt/proj") / "b.mp4")}


def test_get_project_assets_skips_deleted_libraries(project_repo_postgres):
    """get_project_assets() does not return paths for assets in soft-deleted libraries."""
    project_repo, session_factory = project_repo_postgres
    slug, asset_ids = _seed_library_and_assets(session_factory)
    project = project_repo.create_project("Export Bin Deleted Lib", None)
    project_repo.add_asset_to_project(project.id or 0, asset_ids[0])

    # Soft delete the library
    with session_factory() as session:
        session.execute(
            text("UPDATE library SET deleted_at = NOW() AT TIME ZONE 'UTC' WHERE slug = :slug"),
            {"slug": slug},
        )
        session.commit()

    paths = project_repo.get_project_assets(project.id or 0)
    assert paths == []

