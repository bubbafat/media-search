"""Pytest fixtures. Use testcontainers-python for PostgreSQL in tests."""

import contextlib
import os
import threading

import pytest
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer

from src.core.config import get_config, reset_config
from src.core.path_resolver import _reset_session_factory_for_tests


def clear_app_db_caches() -> None:
    """
    Clear the app's config and DB-related caches. Call this in any fixture that
    sets DATABASE_URL (e.g. to a testcontainer URL) so the app uses the new URL
    instead of a previously cached connection.
    """
    from src.api.main import (
        _get_asset_repo,
        _get_library_model_policy_repo,
        _get_library_repo,
        _get_quickwit_search_repo,
        _get_search_repo,
        _get_session_factory,
        _get_system_metadata_repo,
        _get_ui_repo,
        _get_video_scene_repo,
    )
    from src.core import config as config_module

    config_module._config = None  # type: ignore[attr-defined]
    _get_session_factory.cache_clear()
    _get_system_metadata_repo.cache_clear()
    _get_ui_repo.cache_clear()
    _get_search_repo.cache_clear()
    _get_asset_repo.cache_clear()
    _get_video_scene_repo.cache_clear()
    _get_library_repo.cache_clear()
    _get_library_model_policy_repo.cache_clear()
    _get_quickwit_search_repo.cache_clear()


@pytest.fixture(scope="module")
def postgres_container():
    """Module-scoped PostgreSQL 16 container (testcontainers).

    Module scope reduces container lifetime per test file, avoiding connection
    refused errors when the session-scoped container is torn down or becomes
    unreachable during long test runs (e.g. ./test.sh --all).
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def engine(postgres_container):
    """Session-scoped SQLAlchemy engine bound to the Postgres testcontainer."""
    url = postgres_container.get_connection_url()
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    clear_app_db_caches()
    _reset_session_factory_for_tests()
    try:
        yield create_engine(url, pool_pre_ping=True)
    finally:
        if prev is not None:
            os.environ["DATABASE_URL"] = prev
        else:
            os.environ.pop("DATABASE_URL", None)
        clear_app_db_caches()
        _reset_session_factory_for_tests()


@pytest.fixture(scope="module")
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


# Dev Quickwit URL for tests. Prod stays on 7280; tests never touch it.
QUICKWIT_TEST_URL = "http://127.0.0.1:7281"


@pytest.fixture
def require_quickwit():
    """
    Point config at dev Quickwit (7281), check liveness, skip if unreachable.
    Restore env and config after the test. Use for Stage 5 / Quickwit-backed tests only.
    """
    prev_url = os.environ.get("QUICKWIT_URL")
    prev_worker_config = os.environ.get("WORKER_CONFIG")
    os.environ["QUICKWIT_URL"] = QUICKWIT_TEST_URL
    os.environ.pop("WORKER_CONFIG", None)
    reset_config()
    clear_app_db_caches()
    try:
        cfg = get_config()
        url = (cfg.quickwit_url or "").rstrip("/")
        health = f"{url}/health/livez"
        try:
            r = requests.get(health, timeout=2)
            if not r.ok:
                pytest.skip("Quickwit dev not reachable at 7281")
        except requests.RequestException:
            pytest.skip("Quickwit dev not reachable at 7281")
        yield
    finally:
        if prev_url is not None:
            os.environ["QUICKWIT_URL"] = prev_url
        else:
            os.environ.pop("QUICKWIT_URL", None)
        if prev_worker_config is not None:
            os.environ["WORKER_CONFIG"] = prev_worker_config
        else:
            os.environ.pop("WORKER_CONFIG", None)
        reset_config()
        clear_app_db_caches()
