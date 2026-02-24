"""Worker status repository: register, heartbeat, command, state. No ORM in business logic."""

from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy.orm import Session

from src.models.entities import WorkerCommand, WorkerState
from src.models.entities import WorkerStatus as WorkerStatusEntity


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerRepository:
    """Database access for worker_status. All worker status reads/writes go through this."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    def _session(self) -> Session:
        return self._session_factory()

    def register_worker(self, worker_id: str, state: str | WorkerState) -> None:
        """Upsert a row in worker_status (insert or update state/command)."""
        state_str = state.value if isinstance(state, WorkerState) else state
        session = self._session()
        try:
            row = session.get(WorkerStatusEntity, worker_id)
            now = _utcnow()
            if row is None:
                session.add(
                    WorkerStatusEntity(
                        worker_id=worker_id,
                        last_seen_at=now,
                        state=WorkerState(state_str),
                        command=WorkerCommand.none,
                        stats=None,
                    )
                )
            else:
                row.state = WorkerState(state_str)
                row.last_seen_at = now
            session.commit()
        finally:
            session.close()

    def update_heartbeat(self, worker_id: str, stats: dict[str, Any] | None = None) -> None:
        """Update last_seen_at and optional stats for the worker."""
        session = self._session()
        try:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                row.last_seen_at = _utcnow()
                if stats is not None:
                    row.stats = stats
                session.commit()
        finally:
            session.close()

    def get_command(self, worker_id: str) -> str:
        """Return the current value of the command column (e.g. 'none', 'pause', 'shutdown')."""
        session = self._session()
        try:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is None:
                return WorkerCommand.none.value
            return row.command.value
        finally:
            session.close()

    def set_state(self, worker_id: str, state: str | WorkerState) -> None:
        """Update the worker's current state."""
        state_str = state.value if isinstance(state, WorkerState) else state
        session = self._session()
        try:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                row.state = WorkerState(state_str)
                session.commit()
        finally:
            session.close()

    def clear_command(self, worker_id: str) -> None:
        """Set the worker's command back to 'none' after handling."""
        session = self._session()
        try:
            row = session.get(WorkerStatusEntity, worker_id)
            if row is not None:
                row.command = WorkerCommand.none
                session.commit()
        finally:
            session.close()
