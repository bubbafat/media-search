"""UIRepository: get_library_stats, list_libraries_with_status, any_libraries_analyzing."""

import os

import pytest
from sqlalchemy import text

from tests.conftest import clear_app_db_caches
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.ui_repo import UIRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def ui_repo_postgres():
    """Postgres with migrations; seed libraries and assets; yield UIRepository."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from testcontainers.postgres import PostgresContainer

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
                        VALUES ('lib1', 'Lib One', '/mnt/1', true, 'idle', 100, NULL),
                               ('lib2', 'Lib Two', '/mnt/2', true, 'scanning', 100, NULL),
                               ('lib3', 'Lib Three', '/mnt/3', true, 'idle', 100, NULL)
                        """
                    )
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count)
                        VALUES ('lib1', 'a.jpg', 'image', 0, 1, 'completed', 0),
                               ('lib1', 'b.jpg', 'image', 0, 1, 'completed', 0),
                               ('lib2', 'c.jpg', 'image', 0, 1, 'pending', 0),
                               ('lib3', 'd.jpg', 'image', 0, 1, 'completed', 0),
                               ('lib3', 'e.jpg', 'image', 0, 1, 'analyzing', 0)
                        """
                    )
                )
                session.commit()

            system_metadata_repo = SystemMetadataRepository(session_factory)
            ui_repo = UIRepository(session_factory, system_metadata_repo.get_schema_version)
            yield ui_repo
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            clear_app_db_caches()


def test_get_library_stats_returns_pending_ai_and_is_analyzing(ui_repo_postgres):
    """get_library_stats includes pending_ai_count and is_analyzing."""
    stats = ui_repo_postgres.get_library_stats()
    assert stats.total_assets == 5
    assert stats.pending_assets == 1
    assert stats.pending_ai_count == 2
    assert stats.is_analyzing is True


def test_list_libraries_with_status_per_library(ui_repo_postgres):
    """list_libraries_with_status returns is_analyzing per library."""
    libs = ui_repo_postgres.list_libraries_with_status()
    by_slug = {l.slug: l for l in libs}
    assert by_slug["lib1"].is_analyzing is False
    assert by_slug["lib2"].is_analyzing is True
    assert by_slug["lib3"].is_analyzing is True


def test_any_libraries_analyzing_all(ui_repo_postgres):
    """any_libraries_analyzing(None) checks all libraries."""
    assert ui_repo_postgres.any_libraries_analyzing(None) is True


def test_any_libraries_analyzing_filtered_true(ui_repo_postgres):
    """any_libraries_analyzing(['lib2']) is True when lib2 is analyzing."""
    assert ui_repo_postgres.any_libraries_analyzing(["lib2"]) is True


def test_any_libraries_analyzing_filtered_false(ui_repo_postgres):
    """any_libraries_analyzing(['lib1']) is False when lib1 is fully analyzed."""
    assert ui_repo_postgres.any_libraries_analyzing(["lib1"]) is False
