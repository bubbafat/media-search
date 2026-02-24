"""Verify Alembic migrations run and produce the expected schema (testcontainers Postgres)."""

import os

from sqlalchemy import text

from src.core import config as config_module


def test_migration_upgrade_head(engine, postgres_container):
    """Run Alembic upgrade head against testcontainer; verify expected tables exist."""
    url = postgres_container.get_connection_url()
    # Ensure config picks up the test DB URL when env.py calls get_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    config_module._config = None  # type: ignore[attr-defined]

    try:
        from alembic import command
        from alembic.config import Config

        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("script_location", "migrations")
        command.upgrade(alembic_cfg, "head")

        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' "
                    "AND table_name IN ('aimodel', 'library', 'asset', 'videoframe', 'workerstatus') "
                    "ORDER BY table_name"
                )
            )
            tables = [row[0] for row in result]
        assert tables == ["aimodel", "asset", "library", "videoframe", "workerstatus"]
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        config_module._config = None  # type: ignore[attr-defined]
