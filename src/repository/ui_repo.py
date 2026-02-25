"""Read-only repository for the Mission Control dashboard. Returns Pydantic/SQLModel types only."""

from contextlib import contextmanager
from typing import Callable, Iterator

from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.models.entities import WorkerState
from src.models.entities import WorkerStatus as WorkerStatusEntity


# --- Response models (never raw dicts) ---


class SystemHealth(BaseModel):
    schema_version: str
    db_status: str


class WorkerFleetItem(BaseModel):
    worker_id: str
    state: str
    version: str


class LibraryStats(BaseModel):
    total_assets: int
    pending_assets: int


class UIRepository:
    """
    Read-only repository for dashboard: system health, worker fleet, library stats.
    All methods return Pydantic models or structured types.
    """

    def __init__(
        self,
        session_factory: Callable[[], Session],
        schema_version_provider: Callable[[], str | None],
    ) -> None:
        self._session_factory = session_factory
        self._schema_version_provider = schema_version_provider

    @contextmanager
    def _session_scope(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()

    def get_system_health(self) -> SystemHealth:
        """Return schema version and DB status (connected or error)."""
        schema_version = self._schema_version_provider()
        if schema_version is None:
            schema_version = "unknown"
        try:
            with self._session_scope() as session:
                session.execute(text("SELECT 1"))
            db_status = "connected"
        except Exception:
            db_status = "error"
        return SystemHealth(schema_version=schema_version, db_status=db_status)

    def get_worker_fleet(self) -> list[WorkerFleetItem]:
        """Return all workers with worker_id, state, and version (schema version, same for all)."""
        version = self._schema_version_provider() or "unknown"
        with self._session_scope() as session:
            rows = session.execute(select(WorkerStatusEntity)).scalars().all()
            return [
                WorkerFleetItem(
                    worker_id=r.worker_id,
                    state=r.state.value if isinstance(r.state, WorkerState) else str(r.state),
                    version=version,
                )
                for r in rows
            ]

    def get_library_stats(self) -> LibraryStats:
        """Return total and pending asset counts."""
        with self._session_scope() as session:
            total = session.execute(text("SELECT COUNT(*) FROM asset")).scalar()
            pending = session.execute(
                text("SELECT COUNT(*) FROM asset WHERE status = 'pending'")
            ).scalar()
            return LibraryStats(
                total_assets=int(total) if total is not None else 0,
                pending_assets=int(pending) if pending is not None else 0,
            )
