"""BaseWorker lifecycle and signal handling (testcontainers Postgres)."""

import os
import signal
import subprocess
import sys
import threading
import time

import pytest
from sqlmodel import SQLModel

from src.models.entities import SystemMetadata, WorkerCommand, WorkerState
from src.models.entities import WorkerStatus as WorkerStatusEntity
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker


def _create_repo_and_tables(engine, session_factory) -> tuple[WorkerRepository, SystemMetadataRepository]:
    """Create all tables, seed schema_version for pre-flight check (idempotent), return worker_repo and system_metadata_repo."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return WorkerRepository(session_factory), SystemMetadataRepository(session_factory)


class _ConcreteWorker(BaseWorker):
    """Concrete worker for tests: implements handle_signal and optional process_task counter."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.process_task_calls: list[float] = []

    def process_task(self) -> None:
        self.process_task_calls.append(time.monotonic())


def _start_worker_in_thread(worker: BaseWorker) -> threading.Thread:
    """Start a worker.run() loop in a daemon thread and return the thread."""
    thread = threading.Thread(target=worker.run)
    thread.start()
    return thread


def test_worker_start_creates_worker_status_record(engine, _session_factory):
    """Starting a worker creates a WorkerStatus row."""
    repo, system_metadata_repo = _create_repo_and_tables(engine, _session_factory)
    worker = _ConcreteWorker(
        "test-worker-1", repo, heartbeat_interval_seconds=60, system_metadata_repo=system_metadata_repo
    )
    worker.should_exit = True

    thread = _start_worker_in_thread(worker)
    time.sleep(0.3)
    thread.join(timeout=2)

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "test-worker-1")
        assert row is not None
        assert row.worker_id == "test-worker-1"
        assert row.state == WorkerState.offline
    finally:
        session.close()


def test_heartbeat_updates_last_seen_at(engine, _session_factory):
    """Heartbeat thread updates last_seen_at periodically."""
    repo, system_metadata_repo = _create_repo_and_tables(engine, _session_factory)
    worker = _ConcreteWorker(
        "heartbeat-worker",
        repo,
        heartbeat_interval_seconds=0.5,
        system_metadata_repo=system_metadata_repo,
    )

    thread = _start_worker_in_thread(worker)
    time.sleep(0.2)

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "heartbeat-worker")
        assert row is not None
        first_seen = row.last_seen_at
    finally:
        session.close()

    time.sleep(0.6)

    session2 = _session_factory()
    try:
        row2 = session2.get(WorkerStatusEntity, "heartbeat-worker")
        assert row2 is not None
        assert row2.last_seen_at >= first_seen
    finally:
        session2.close()

    worker.should_exit = True
    thread.join(timeout=2)


def test_pause_command_transitions_state_and_stops_process_task(engine, _session_factory):
    """When DB command is 'pause', worker transitions to paused and stops calling process_task."""
    repo, system_metadata_repo = _create_repo_and_tables(engine, _session_factory)
    worker = _ConcreteWorker(
        "pause-worker",
        repo,
        heartbeat_interval_seconds=10,
        system_metadata_repo=system_metadata_repo,
    )

    session = _session_factory()
    try:
        repo.register_worker("pause-worker", WorkerState.idle)
        session.commit()
    finally:
        session.close()

    thread = _start_worker_in_thread(worker)

    time.sleep(0.25)
    n_before_pause = len(worker.process_task_calls)

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "pause-worker")
        assert row is not None
        row.command = WorkerCommand.pause
        session.commit()
    finally:
        session.close()

    time.sleep(1.5)

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "pause-worker")
        assert row is not None
        assert row.state == WorkerState.paused
    finally:
        session.close()

    n_after_pause = len(worker.process_task_calls)
    worker.should_exit = True
    thread.join(timeout=2)

    assert n_after_pause <= n_before_pause + 2


def test_shutdown_command_causes_graceful_exit(engine, _session_factory):
    """Setting command to 'shutdown' causes worker to set state offline and exit the loop."""
    repo, system_metadata_repo = _create_repo_and_tables(engine, _session_factory)
    worker = _ConcreteWorker(
        "shutdown-worker",
        repo,
        heartbeat_interval_seconds=10,
        system_metadata_repo=system_metadata_repo,
    )

    thread = threading.Thread(target=worker.run)
    thread.start()
    time.sleep(0.2)

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "shutdown-worker")
        assert row is not None
        row.command = WorkerCommand.shutdown
        session.commit()
    finally:
        session.close()

    thread.join(timeout=3)
    assert not thread.is_alive()

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "shutdown-worker")
        assert row is not None
        assert row.state == WorkerState.offline
    finally:
        session.close()


def test_sigterm_causes_graceful_exit(postgres_container, engine, _session_factory):
    """SIGTERM to a process running the worker causes clean exit and state=offline."""
    # Create tables and seed schema_version so subprocess pre-flight check passes
    _create_repo_and_tables(engine, _session_factory)
    url = postgres_container.get_connection_url()
    env = os.environ.copy()
    env["DATABASE_URL"] = url

    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            """
import os
import sys
sys.path.insert(0, os.getcwd())
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker
from src.models.entities import WorkerState


class MinimalWorker(BaseWorker):
    # Use the default BaseWorker.handle_signal implementation.
    pass


engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
session_factory = sessionmaker(engine, autocommit=False, autoflush=False, expire_on_commit=False)
repo = WorkerRepository(session_factory)
system_metadata_repo = SystemMetadataRepository(session_factory)
worker = MinimalWorker("sigterm-worker", repo, heartbeat_interval_seconds=60, system_metadata_repo=system_metadata_repo)
worker.run()
""",
        ],
        env=env,
        cwd=os.getcwd(),
    )
    time.sleep(0.5)
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)
    assert proc.returncode in (0, -signal.SIGTERM, 128 + signal.SIGTERM)

    session = _session_factory()
    try:
        row = session.get(WorkerStatusEntity, "sigterm-worker")
        assert row is not None
        assert row.state == WorkerState.offline
    finally:
        session.close()
