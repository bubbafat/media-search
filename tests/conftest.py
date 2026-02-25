"""Pytest fixtures. Use testcontainers-python for PostgreSQL in tests."""

import contextlib
import threading

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container():
    """Session-scoped PostgreSQL 16 container (testcontainers)."""
    with PostgresContainer("postgres:16") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def engine(postgres_container):
    """Session-scoped SQLAlchemy engine bound to the Postgres testcontainer."""
    url = postgres_container.get_connection_url()
    return create_engine(url, pool_pre_ping=True)


@pytest.fixture(scope="session")
def _session_factory(engine):
    """Session-scoped session factory (used to create per-test sessions)."""
    return sessionmaker(engine, autocommit=False, autoflush=False, expire_on_commit=False)


@pytest.fixture
def session(engine, _session_factory):
    """Function-scoped, clean SQLAlchemy session. Each test runs in a transaction that is rolled back."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture
def run_worker():
    """
    Yields a context manager that starts a worker in a daemon thread and stops it on exit.
    Usage: with run_worker(worker): ... (do assertions while worker runs).
    """

    @contextlib.contextmanager
    def _run(worker):
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        try:
            yield worker
        finally:
            worker.should_exit = True
            thread.join(timeout=5.0)

    return _run
