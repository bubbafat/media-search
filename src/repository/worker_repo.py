"""Worker status repository: register, heartbeat, command, state. No ORM in business logic."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from src.models.entities import WorkerCommand, WorkerState
from src.models.entities import WorkerStatus as WorkerStatusEntity


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerRepository:
    """
    Database access for worker_status.

    All worker status reads/writes go through this coarse-grained repository, so workers and
    higher-level components do not talk to the ORM directly.
    """

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self, write: bool = False) -> Iterator[Session]:
        """
        Provide a transactional scope for a series of ORM operations.

        When write=True, the session is committed on successful exit; otherwise it is treated
        as read-only. In all cases, the session is closed in a finally block.
        """
        session = self._session_factory()
        try:
            yield session
            if write:
                session.commit()
        finally:
            session.close()

    def register_worker(self, worker_id: str, state: str | WorkerState, hostname: str = "") -> None:
        """Upsert a row in worker_status (insert or update state/command/hostname for a single worker)."""
        state_str = state.value if isinstance(state, WorkerState) else state
        with self._session_scope(write=True) as session:
            row = session.get(WorkerStatusEntity, worker_id)
            now = _utcnow()
            if row is None:
                session.add(
                    WorkerStatusEntity(
                        worker_id=worker_id,
                        hostname=hostname,
                        last_seen_at=now,
                        state=WorkerState(state_str),
                        command=WorkerCommand.none,
                        stats=None,
                    )
                )
            else:
                row.state = WorkerState(state_str)
                row.hostname = hostname
                row.last_seen_at = now

    def update_heartbeat(self, worker_id: str, stats: dict[str, Any] | None = None) -> None:
        """Update last_seen_at and optional stats for the worker."""
        with self._session_scope(write=True) as session:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                row.last_seen_at = _utcnow()
                if stats is not None:
                    row.stats = stats

    def get_command(self, worker_id: str) -> str:
        """Return the current value of the command column (e.g. 'none', 'pause', 'shutdown')."""
        with self._session_scope(write=False) as session:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is None:
                return WorkerCommand.none.value
            return row.command.value

    def set_state(self, worker_id: str, state: str | WorkerState) -> None:
        """Update the worker's current state."""
        state_str = state.value if isinstance(state, WorkerState) else state
        with self._session_scope(write=True) as session:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                row.state = WorkerState(state_str)

    def clear_command(self, worker_id: str) -> None:
        """Set the worker's command back to 'none' after handling."""
        with self._session_scope(write=True) as session:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                row.command = WorkerCommand.none

    def unregister_worker(self, worker_id: str) -> None:
        """Remove the worker row from worker_status on graceful shutdown."""
        with self._session_scope(write=True) as session:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                session.delete(row)

    def count_stale_workers(self, max_age_hours: int = 24) -> int:
        """Count worker_status rows with last_seen_at older than max_age_hours. Read-only."""
        cutoff = _utcnow() - timedelta(hours=max_age_hours)
        with self._session_scope(write=False) as session:
            result = session.scalar(
                select(func.count()).select_from(WorkerStatusEntity).where(
                    WorkerStatusEntity.last_seen_at < cutoff
                )
            )
            return result or 0

    def prune_stale_workers(self, max_age_hours: int = 24) -> int:
        """Delete worker_status rows with last_seen_at older than max_age_hours. Returns count deleted."""
        cutoff = _utcnow() - timedelta(hours=max_age_hours)
        with self._session_scope(write=True) as session:
            result = session.execute(
                delete(WorkerStatusEntity).where(WorkerStatusEntity.last_seen_at < cutoff)
            )
            return result.rowcount or 0

    def get_active_local_worker_count(self, hostname: str, exclude_worker_id: str) -> int:
        """Count workers on the same host that are active (not offline, seen in last 60s)."""
        now = _utcnow()
        cutoff = now - timedelta(seconds=60)
        with self._session_scope(write=False) as session:
            result = session.scalar(
                select(func.count()).select_from(WorkerStatusEntity).where(
                    WorkerStatusEntity.hostname == hostname,
                    WorkerStatusEntity.worker_id != exclude_worker_id,
                    WorkerStatusEntity.state != WorkerState.offline,
                    WorkerStatusEntity.last_seen_at >= cutoff,
                )
            )
            return result or 0

    def has_active_local_transcodes(self, hostname: str) -> bool:
        """Return True if any worker on this host is actively transcoding (seen in last 120s)."""
        now = _utcnow()
        cutoff = now - timedelta(seconds=120)
        with self._session_scope(write=False) as session:
            result = session.scalar(
                select(func.count()).select_from(WorkerStatusEntity).where(
                    WorkerStatusEntity.hostname == hostname,
                    WorkerStatusEntity.state != WorkerState.offline,
                    WorkerStatusEntity.last_seen_at >= cutoff,
                    WorkerStatusEntity.stats.isnot(None),
                    WorkerStatusEntity.stats["current_stage"].astext == "transcode",
                )
            )
            return (result or 0) > 0
