"""System metadata repository: read-only access to schema_version and other system keys."""

from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy.orm import Session

from src.models.entities import SystemMetadata as SystemMetadataEntity


class SystemMetadataRepository:
    """
    Read-only access to system_metadata (e.g. schema_version).
    Used by BaseWorker for compatibility check and by UI for dashboard.
    """

    SCHEMA_VERSION_KEY = "schema_version"

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()

    def get_value(self, key: str) -> str | None:
        """Return the value for key, or None if missing."""
        with self._session_scope() as session:
            row = session.get(SystemMetadataEntity, key)
            return row.value if row is not None else None

    def get_schema_version(self) -> str | None:
        """Return the schema_version value, or None if missing."""
        return self.get_value(self.SCHEMA_VERSION_KEY)
