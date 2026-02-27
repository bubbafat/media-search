"""Verify Alembic migrations: empty DB → upgrade head → downgrade base (own Postgres, run with pytest -m migration)."""

import os

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from tests.conftest import clear_app_db_caches

EXPECTED_TABLES = [
    "aimodel",
    "asset",
    "library",
    "project",
    "project_assets",
    "system_metadata",
    "video_active_state",
    "video_scenes",
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
    clear_app_db_caches()

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
                    "AND table_name IN ('aimodel', 'library', 'asset', 'system_metadata', 'videoframe', 'video_active_state', 'video_scenes', 'worker_status', 'project', 'project_assets') "
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

        # Assert aimodel has name (no slug) and unique constraint (name, version) (migration 009)
        with migration_engine.connect() as conn:
            cols = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'aimodel' "
                    "ORDER BY ordinal_position"
                )
            ).fetchall()
            column_names = [c[0] for c in cols]
            assert "name" in column_names, "aimodel.name must exist"
            assert "slug" not in column_names, "aimodel.slug must be removed"
            row = conn.execute(
                text(
                    "SELECT constraint_name FROM information_schema.table_constraints "
                    "WHERE table_schema = 'public' AND table_name = 'aimodel' "
                    "AND constraint_type = 'UNIQUE' AND constraint_name = 'uq_aimodel_name_version'"
                )
            ).fetchone()
            assert row is not None, "uq_aimodel_name_version unique constraint must exist on aimodel"

        # Assert asset has analysis_model_id and visual_analysis (migration 009)
        with migration_engine.connect() as conn:
            for col, udt in [("analysis_model_id", "int4"), ("visual_analysis", "jsonb")]:
                row = conn.execute(
                    text(
                        "SELECT column_name, udt_name FROM information_schema.columns "
                        "WHERE table_schema = 'public' AND table_name = 'asset' AND column_name = :col"
                    ),
                    {"col": col},
                ).fetchone()
                assert row is not None, f"asset.{col} must exist"
                assert row[1] == udt, f"asset.{col} must be {udt} (udt_name={row[1]!r})"

        # Assert asset has GIN FTS index on visual_analysis (migration 010)
        with migration_engine.connect() as conn:
            idx = conn.execute(
                text(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE tablename = 'asset' AND indexname = 'ix_asset_fts'"
                )
            ).fetchone()
            assert idx is not None, "ix_asset_fts index must exist on asset"
            assert "gin" in idx[1].lower(), "ix_asset_fts must be a GIN index"

        # Assert video_scenes and video_active_state exist with FKs (migration 011)
        with migration_engine.connect() as conn:
            for table in ("video_scenes", "video_active_state"):
                row = conn.execute(
                    text(
                        "SELECT constraint_name FROM information_schema.table_constraints "
                        "WHERE table_schema = 'public' AND table_name = :t AND constraint_type = 'FOREIGN KEY'"
                    ),
                    {"t": table},
                ).fetchone()
                assert row is not None, f"{table} must have a foreign key constraint"
        assert "video_scenes" in EXPECTED_TABLES and "video_active_state" in EXPECTED_TABLES

        # Assert project and project_assets exist with FKs (migration 017)
        with migration_engine.connect() as conn:
            # project.created_at must be timestamptz
            row = conn.execute(
                text(
                    "SELECT udt_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'project' AND column_name = 'created_at'"
                )
            ).fetchone()
            assert row is not None, "project.created_at must exist"
            assert row[0] == "timestamptz", "project.created_at must be timestamp with time zone"

            # project_assets must have FKs to project and asset
            fk_rows = conn.execute(
                text(
                    "SELECT constraint_name FROM information_schema.table_constraints "
                    "WHERE table_schema = 'public' AND table_name = 'project_assets' AND constraint_type = 'FOREIGN KEY'"
                )
            ).fetchall()
            assert fk_rows, "project_assets must have foreign key constraints"

        # Assert default AI model is moondream2 (migration 012)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT key, value FROM system_metadata WHERE key = 'default_ai_model_id'"
                )
            ).fetchone()
            assert row is not None, "default_ai_model_id must be set in system_metadata"
            default_id = int(row[1])
            model_row = conn.execute(
                text(
                    "SELECT id, name, version FROM aimodel WHERE id = :id"
                ),
                {"id": default_id},
            ).fetchone()
            assert model_row is not None, "default_ai_model_id must reference an existing aimodel row"
            assert model_row[1] == "moondream2", "default AI model must be moondream2"
            assert model_row[2] == "2025-01-09", "moondream2 version must be 2025-01-09"

        # Assert moondream3 aimodel row exists (migration 018)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, name, version FROM aimodel WHERE name = 'moondream3' AND version = 'preview'"
                )
            ).fetchone()
            assert row is not None, "aimodel row for moondream3/preview must exist"
            assert row[1] == "moondream3", "moondream3 model name must be correct"
            assert row[2] == "preview", "moondream3 version must be preview"

        # Assert video_scenes has GIN FTS index (migration 013)
        with migration_engine.connect() as conn:
            idx = conn.execute(
                text(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE tablename = 'video_scenes' AND indexname = 'ix_video_scenes_fts'"
                )
            ).fetchone()
            assert idx is not None, "ix_video_scenes_fts index must exist on video_scenes"
            assert "gin" in idx[1].lower(), "ix_video_scenes_fts must be a GIN index"

        # Assert asset has preview_path column (migration 014)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT column_name, is_nullable, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'asset' AND column_name = 'preview_path'"
                )
            ).fetchone()
            assert row is not None, "asset.preview_path column must exist"
            assert row[1] == "YES", "asset.preview_path must be nullable"
            assert row[2] == "character varying", "asset.preview_path must be string type"

        # Assert asset has video_preview_path column (migration 015)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT column_name, is_nullable, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'asset' AND column_name = 'video_preview_path'"
                )
            ).fetchone()
            assert row is not None, "asset.video_preview_path column must exist"
            assert row[1] == "YES", "asset.video_preview_path must be nullable"
            assert row[2] == "character varying", "asset.video_preview_path must be string type"

        # Assert asset.size is BIGINT (migration 016)
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT udt_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'asset' AND column_name = 'size'"
                )
            ).fetchone()
            assert row is not None, "asset.size column must exist"
            assert row[0] == "int8", "asset.size must be BIGINT (udt_name=int8)"
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        clear_app_db_caches()


@pytest.mark.migration
@pytest.mark.order(2)
def test_migration_02_downgrade_base(migration_postgres, migration_engine):
    """Run Alembic downgrade base; verify migration is reversible (tables dropped)."""
    url = migration_postgres.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    clear_app_db_caches()

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
                    "AND table_name IN ('aimodel', 'library', 'asset', 'system_metadata', 'videoframe', 'video_active_state', 'video_scenes', 'worker_status', 'project', 'project_assets') "
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
        clear_app_db_caches()
