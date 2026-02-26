"""Read-only repository for the Mission Control dashboard. Returns Pydantic/SQLModel types only."""

from contextlib import contextmanager
from typing import Callable, Iterator

from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.models.entities import Library
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
    stats: dict | None = None


class LibraryStats(BaseModel):
    total_assets: int
    pending_assets: int
    pending_ai_count: int
    is_analyzing: bool


class LibraryWithStatus(BaseModel):
    slug: str
    name: str
    is_analyzing: bool


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
                    stats=r.stats if isinstance(r.stats, dict) or r.stats is None else dict(r.stats),
                )
                for r in rows
            ]

    def get_library_stats(self) -> LibraryStats:
        """Return total, pending asset counts, pending_ai_count, and is_analyzing."""
        with self._session_scope() as session:
            total = session.execute(text("SELECT COUNT(*) FROM asset")).scalar()
            pending = session.execute(
                text("SELECT COUNT(*) FROM asset WHERE status = 'pending'")
            ).scalar()
            pending_ai = session.execute(
                text(
                    "SELECT COUNT(*) FROM asset WHERE status NOT IN ('completed', 'failed', 'poisoned')"
                )
            ).scalar()
            any_scanning = session.execute(
                text(
                    "SELECT EXISTS(SELECT 1 FROM library WHERE deleted_at IS NULL AND scan_status = 'scanning')"
                )
            ).scalar()
            pending_ai_count = int(pending_ai) if pending_ai is not None else 0
            is_analyzing = pending_ai_count > 0 or bool(any_scanning)
            return LibraryStats(
                total_assets=int(total) if total is not None else 0,
                pending_assets=int(pending) if pending is not None else 0,
                pending_ai_count=pending_ai_count,
                is_analyzing=is_analyzing,
            )

    def list_libraries_with_status(self) -> list[LibraryWithStatus]:
        """Return non-deleted libraries with is_analyzing per library."""
        with self._session_scope() as session:
            rows = session.execute(
                text("""
                    SELECT l.slug, l.name, l.scan_status,
                        COALESCE(agg.pending_ai, 0)::int AS pending_ai
                    FROM library l
                    LEFT JOIN (
                        SELECT library_id,
                            COUNT(*) FILTER (WHERE status NOT IN ('completed', 'failed', 'poisoned')) AS pending_ai
                        FROM asset
                        GROUP BY library_id
                    ) agg ON agg.library_id = l.slug
                    WHERE l.deleted_at IS NULL
                    ORDER BY l.slug
                """)
            ).fetchall()
            return [
                LibraryWithStatus(
                    slug=row[0],
                    name=row[1] or row[0],
                    is_analyzing=row[2] == "scanning" or (row[3] or 0) > 0,
                )
                for row in rows
            ]

    def any_libraries_analyzing(self, slugs: list[str] | None) -> bool:
        """Return True if any library in scope has is_analyzing. slugs=None means all non-deleted libraries."""
        libs = self.list_libraries_with_status()
        if slugs is not None:
            slug_set = set(slugs)
            libs = [l for l in libs if l.slug in slug_set]
        return any(l.is_analyzing for l in libs)

    def get_library_names(self, slugs: list[str]) -> dict[str, str]:
        """Return slug -> name for the given library slugs. Empty list returns {}."""
        if not slugs:
            return {}
        with self._session_scope() as session:
            rows = (
                session.execute(select(Library).where(Library.slug.in_(slugs)))
                .scalars()
                .all()
            )
            return {r.slug: r.name or r.slug for r in rows}
