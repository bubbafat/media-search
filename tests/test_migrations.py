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
    "worker_status",
]


@pytest.fixture(scope="module")
def migration_postgres():
    """Dedicated Postgres 16 container for migration tests only (empty DB)."""
    with PostgresContainer("postgres:16-alpine") as postgres:
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
                    "AND table_name IN ('aimodel', 'library', 'asset', 'system_metadata', 'videoframe', 'worker_status') "
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

        # Assert videoframe.search_vector column is tsvector (spec Section 6)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT data_type, udt_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'videoframe' AND column_name = 'search_vector'"
                )
            ).fetchone()
            assert row is not None, "videoframe.search_vector column must exist"
            assert row[1] == "tsvector", (
                f"videoframe.search_vector must be tsvector (data_type={row[0]!r}, udt_name={row[1]!r})"
            )

        # Assert library has absolute_path and deleted_at (paths in DB, soft delete)
        with migration_engine.connect() as conn:
            cols = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'library' "
                    "AND column_name IN ('absolute_path', 'deleted_at') ORDER BY column_name"
                )
            ).fetchall()
            column_names = [c[0] for c in cols]
            assert "absolute_path" in column_names, "library.absolute_path column must exist"
            assert "deleted_at" in column_names, "library.deleted_at column must exist"

        # Assert all timestamp columns are WITH TIME ZONE (project rule: never store local time)
        with migration_engine.connect() as conn:
            for table, column in [("asset", "lease_expires_at"), ("worker_status", "last_seen_at"), ("library", "deleted_at")]:
                row = conn.execute(
                    text(
                        "SELECT udt_name FROM information_schema.columns "
                        "WHERE table_schema = 'public' AND table_name = :t AND column_name = :c"
                    ),
                    {"t": table, "c": column},
                ).fetchone()
                assert row is not None, f"{table}.{column} must exist"
                assert row[0] == "timestamptz", (
                    f"{table}.{column} must be timestamp with time zone (udt_name={row[0]!r})"
                )

        # Assert asset.error_message column exists and is nullable (migration 008)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT column_name, is_nullable FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'asset' AND column_name = 'error_message'"
                )
            ).fetchone()
            assert row is not None, "asset.error_message column must exist"
            assert row[1] == "YES", "asset.error_message must be nullable"

        # Assert asset.status accepts proxied and processing (migration 008 enum values)
        with migration_engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit) "
                    "VALUES ('mig08', 'Mig08', '/tmp/mig08', true, 'idle', 100)"
                )
            )
            conn.commit()
        with migration_engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count) "
                    "VALUES ('mig08', 'x.jpg', 'image', 0, 0, 'proxied', 0)"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count) "
                    "VALUES ('mig08', 'y.png', 'image', 0, 0, 'processing', 0)"
                )
            )
            conn.commit()
            row = conn.execute(
                text("SELECT status FROM asset WHERE rel_path = 'x.jpg'")
            ).fetchone()
            assert row is not None and row[0] == "proxied"
            row = conn.execute(
                text("SELECT status FROM asset WHERE rel_path = 'y.png'")
            ).fetchone()
            assert row is not None and row[0] == "processing"
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
                    "AND table_name IN ('aimodel', 'library', 'asset', 'system_metadata', 'videoframe', 'worker_status') "
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
