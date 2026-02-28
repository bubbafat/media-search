"""WorkerRepository tests (testcontainers Postgres)."""

from datetime import datetime, timedelta, timezone

import pytest
from sqlmodel import SQLModel

from src.models.entities import Asset, AssetStatus, Library, SystemMetadata, WorkerState
from src.models.entities import WorkerStatus as WorkerStatusEntity
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository

pytestmark = [pytest.mark.slow]


def _create_repo_and_tables(engine, session_factory) -> WorkerRepository:
    """Create all tables, seed schema_version, return WorkerRepository."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return WorkerRepository(session_factory)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def test_get_active_local_worker_count_excludes_self(engine, _session_factory):
    """Count excludes the exclude_worker_id."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    host = "host-excludes-self-test"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-a",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.idle,
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-b",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.processing,
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.get_active_local_worker_count(host, "worker-a") == 1
    assert repo.get_active_local_worker_count(host, "worker-b") == 1
    assert repo.get_active_local_worker_count(host, "worker-c") == 2


def test_get_active_local_worker_count_excludes_offline(engine, _session_factory):
    """Count excludes workers with state offline."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    host = "host-offline-test"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-online",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.idle,
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-offline",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.offline,
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.get_active_local_worker_count(host, "worker-x") == 1


def test_get_active_local_worker_count_excludes_stale(engine, _session_factory):
    """Count excludes workers with last_seen_at older than 60 seconds."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    stale = now - timedelta(seconds=61)
    host = "host-stale-test"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-fresh",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.idle,
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-stale",
                hostname=host,
                last_seen_at=stale,
                state=WorkerState.idle,
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.get_active_local_worker_count(host, "worker-x") == 1


def test_get_active_local_worker_count_excludes_other_hosts(engine, _session_factory):
    """Count only includes workers on the same hostname."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    host1, host2, host3 = "host-hosts-test-1", "host-hosts-test-2", "host-hosts-test-3"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-host1",
                hostname=host1,
                last_seen_at=now,
                state=WorkerState.idle,
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-host2",
                hostname=host2,
                last_seen_at=now,
                state=WorkerState.idle,
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.get_active_local_worker_count(host1, "worker-x") == 1
    assert repo.get_active_local_worker_count(host2, "worker-x") == 1
    assert repo.get_active_local_worker_count(host3, "worker-x") == 0


def test_has_active_local_transcodes_returns_true_when_transcoding(engine, _session_factory):
    """Returns True when a worker has current_stage transcode, recent heartbeat, and valid lease."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    lease_expires = now + timedelta(minutes=5)
    host = "host-transcode-test"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug="transcode-lib",
                name="Transcode Lib",
                absolute_path="/tmp/transcode-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.flush()
        session.add(
            Asset(
                library_id="transcode-lib",
                rel_path="video.mp4",
                type="video",
                status=AssetStatus.processing,
                worker_id="worker-transcoding",
                lease_expires_at=lease_expires,
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-transcoding",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.processing,
                stats={"current_stage": "transcode"},
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.has_active_local_transcodes(host) is True


def test_has_active_local_transcodes_returns_false_when_not_transcoding(engine, _session_factory):
    """Returns False when worker has different stage, offline, or no stats."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    host = "host-not-transcode-test"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-not-transcode-1",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.processing,
                stats={"current_stage": "thumbnail"},
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-not-transcode-2",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.offline,
                stats={"current_stage": "transcode"},
            )
        )
        session.add(
            WorkerStatusEntity(
                worker_id="worker-not-transcode-3",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.processing,
                stats=None,
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.has_active_local_transcodes(host) is False


def test_has_active_local_transcodes_excludes_stale(engine, _session_factory):
    """Returns False when transcode worker has last_seen > 120 seconds ago."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    stale = now - timedelta(seconds=121)
    host = "host-stale-transcode-test"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-stale-transcode",
                hostname=host,
                last_seen_at=stale,
                state=WorkerState.processing,
                stats={"current_stage": "transcode"},
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.has_active_local_transcodes(host) is False


def test_has_active_local_transcodes_returns_false_when_ghost(engine, _session_factory):
    """Returns False when transcode worker has no valid lease (ghost/crashed worker)."""
    repo = _create_repo_and_tables(engine, _session_factory)
    now = _utcnow()
    host = "host-ghost-transcode-test"
    session = _session_factory()
    try:
        session.add(
            WorkerStatusEntity(
                worker_id="worker-ghost-transcode",
                hostname=host,
                last_seen_at=now,
                state=WorkerState.processing,
                stats={"current_stage": "transcode"},
            )
        )
        session.commit()
    finally:
        session.close()

    assert repo.has_active_local_transcodes(host) is False

