"""Verify Alembic migrations: empty DB → upgrade head → downgrade base (own Postgres, run with pytest -m migration)."""

import os

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from src.core import config as config_module

EXPECTED_TABLES = [
    "aimodel",
    "asset",
    "library",
    "system_metadata",
    "videoframe",
    "workerstatus",
]


@pytest.fixture(scope="module")
def migration_postgres():
    """Dedicated Postgres 16 container for migration tests only (empty DB)."""
    with PostgresContainer("postgres:16") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def migration_engine(migration_postgres):
    """Engine bound to the migration test container."""
    url = migration_postgres.get_connection_url()
    return create_engine(url, pool_pre_ping=True)


@pytest.mark.migration
@pytest.mark.order(1)
def test_migration_01_upgrade_head(migration_postgres, migration_engine):
    """Run Alembic upgrade head on empty DB; verify expected tables exist."""
    url = migration_postgres.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    config_module._config = None  # type: ignore[attr-defined]

    try:
        from alembic import command
        from alembic.config import Config

        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("script_location", "migrations")
        command.upgrade(alembic_cfg, "head")

        with migration_engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' "
                    "AND table_name IN ('aimodel', 'library', 'asset', 'system_metadata', 'videoframe', 'workerstatus') "
                    "ORDER BY table_name"
                )
            )
            tables = [row[0] for row in result]
        assert tables == EXPECTED_TABLES

        # Assert system_metadata has seeded schema_version
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT key, value FROM system_metadata WHERE key = 'schema_version'"
                )
            ).fetchone()
            assert row is not None, "schema_version must be seeded in system_metadata"
            assert row[1] == "1", "schema_version value must be '1'"

        # Assert asset has unique index on (library_id, rel_path) for ON CONFLICT
        with migration_engine.connect() as conn:
            idx = conn.execute(
                text(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE tablename = 'asset' AND indexname = 'ix_asset_library_rel_path'"
                )
            ).fetchone()
            assert idx is not None, "ix_asset_library_rel_path index must exist on asset"
            assert "UNIQUE" in idx[1], "ix_asset_library_rel_path must be a unique index"
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        config_module._config = None  # type: ignore[attr-defined]


@pytest.mark.migration
@pytest.mark.order(2)
def test_migration_02_downgrade_base(migration_postgres, migration_engine):
    """Run Alembic downgrade base; verify migration is reversible (tables dropped)."""
    url = migration_postgres.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    config_module._config = None  # type: ignore[attr-defined]

    try:
        from alembic import command
        from alembic.config import Config

        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("script_location", "migrations")
        command.downgrade(alembic_cfg, "base")

        with migration_engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' "
                    "AND table_name IN ('aimodel', 'library', 'asset', 'system_metadata', 'videoframe', 'workerstatus') "
                    "ORDER BY table_name"
                )
            )
            tables = [row[0] for row in result]
        assert tables == []
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        config_module._config = None  # type: ignore[attr-defined]
