"""Pytest fixtures. Use testcontainers-python for PostgreSQL in tests."""

import pytest


@pytest.fixture(scope="session")
def postgres_container():
    """Session-scoped PostgreSQL container (testcontainers)."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as postgres:
        yield postgres
