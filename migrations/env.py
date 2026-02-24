"""Alembic env: run migrations against PostgreSQL. URL from config."""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Import SQLModel metadata for autogenerate when models exist
# from sqlmodel import SQLModel
# from src.models import *  # noqa: F401, F403
# target_metadata = SQLModel.metadata
target_metadata = None

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

def get_url() -> str:
    """Database URL from app config (PostgreSQL only)."""
    from src.core.config import get_config
    return get_config().database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (SQL only)."""
    context.configure(url=get_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (connect to DB)."""
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
