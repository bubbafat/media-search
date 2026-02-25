"""System metadata repository: schema_version, AI models, and visual analysis storage."""

import json
from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.ai.schema import ModelCard, VisualAnalysis
from src.models.entities import AIModel, SystemMetadata as SystemMetadataEntity


class SystemMetadataRepository:
    """
    Access to system_metadata (e.g. schema_version), aimodel CRUD, and visual analysis.
    Used by BaseWorker for compatibility check and by AI worker / CLI.
    """

    SCHEMA_VERSION_KEY = "schema_version"

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self, write: bool = False) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            if write:
                session.commit()
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

    def get_or_create_ai_model(self, card: ModelCard) -> int:
        """Return aimodel id for (name, version); insert if not present."""
        with self._session_scope(write=True) as session:
            row = session.execute(
                select(AIModel.id).where(
                    AIModel.name == card.name,
                    AIModel.version == card.version,
                )
            ).fetchone()
            if row is not None:
                return int(row[0])
            model = AIModel(name=card.name, version=card.version)
            session.add(model)
            session.flush()
            assert model.id is not None
            return model.id

    def get_all_ai_models(self) -> list[AIModel]:
        """Return all aimodel rows ordered by name, version."""
        with self._session_scope() as session:
            result = session.execute(
                select(AIModel).order_by(AIModel.name, AIModel.version)
            )
            return list(result.scalars().all())

    def add_ai_model(self, name: str, version: str) -> AIModel:
        """Insert an aimodel row; return the created entity."""
        with self._session_scope(write=True) as session:
            model = AIModel(name=name, version=version)
            session.add(model)
            session.flush()
            session.refresh(model)
            return model

    def remove_ai_model(self, name: str) -> bool:
        """
        Delete all aimodels with this name. If any asset references them, raise ValueError.
        Return True if at least one row was deleted, False if none found.
        """
        with self._session_scope(write=True) as session:
            count_result = session.execute(
                text("""
                    SELECT COUNT(*) FROM asset a
                    JOIN aimodel m ON a.analysis_model_id = m.id
                    WHERE m.name = :name
                """),
                {"name": name},
            )
            n = int(count_result.scalar() or 0)
            if n > 0:
                raise ValueError(
                    f"Cannot delete model '{name}'. It is currently referenced by {n} assets. "
                    "You must re-process or clear these assets first."
                )
            result = session.execute(
                text("DELETE FROM aimodel WHERE name = :name"),
                {"name": name},
            )
            return result.rowcount is not None and result.rowcount > 0

    def save_visual_analysis(self, asset_id: int, analysis: VisualAnalysis) -> None:
        """Upsert visual analysis for an asset (JSONB on asset)."""
        payload = {
            "description": analysis.description,
            "tags": analysis.tags,
            "ocr_text": analysis.ocr_text,
        }
        payload_json = json.dumps(payload)
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    UPDATE asset SET visual_analysis = CAST(:payload AS jsonb) WHERE id = :asset_id
                """),
                {"payload": payload_json, "asset_id": asset_id},
            )
