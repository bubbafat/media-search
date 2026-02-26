"""System metadata repository: schema_version, AI models, and visual analysis storage."""

import json
import os
from contextlib import contextmanager
from typing import Callable, Iterator

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.ai.schema import ModelCard, VisualAnalysis
from src.models.entities import AIModel, SystemMetadata as SystemMetadataEntity

ALLOW_MOCK_DEFAULT_ENV = "MEDIASEARCH_ALLOW_MOCK_DEFAULT"


class SystemMetadataRepository:
    """
    Access to system_metadata (e.g. schema_version), aimodel CRUD, and visual analysis.
    Used by BaseWorker for compatibility check and by AI worker / CLI.
    """

    SCHEMA_VERSION_KEY = "schema_version"
    DEFAULT_AI_MODEL_ID_KEY = "default_ai_model_id"

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

    def set_value(self, key: str, value: str) -> None:
        """Set key to value (upsert)."""
        with self._session_scope(write=True) as session:
            row = session.get(SystemMetadataEntity, key)
            if row is not None:
                row.value = value
            else:
                session.add(SystemMetadataEntity(key=key, value=value))

    def get_schema_version(self) -> str | None:
        """Return the schema_version value, or None if missing."""
        return self.get_value(self.SCHEMA_VERSION_KEY)

    def get_default_ai_model_id(self) -> int | None:
        """Return the default AI model id from system_metadata, or None if unset."""
        raw = self.get_value(self.DEFAULT_AI_MODEL_ID_KEY)
        if raw is None or not raw.strip():
            return None
        try:
            return int(raw.strip())
        except ValueError:
            return None

    def set_default_ai_model_id(self, model_id: int) -> None:
        """
        Set the system default AI model. The model must exist.
        Rejects model name 'mock' unless MEDIASEARCH_ALLOW_MOCK_DEFAULT=1 (for tests).
        """
        model = self.get_ai_model_by_id(model_id)
        if model is None:
            raise ValueError(f"AI model with id {model_id} does not exist.")
        if model.name in ("mock", "mock-analyzer") and os.environ.get(ALLOW_MOCK_DEFAULT_ENV, "").strip() != "1":
            raise ValueError(
                "Cannot set 'mock' as the default AI model. "
                f"Set {ALLOW_MOCK_DEFAULT_ENV}=1 only in tests if needed."
            )
        self.set_value(self.DEFAULT_AI_MODEL_ID_KEY, str(model_id))

    def get_ai_model_by_id(self, model_id: int) -> AIModel | None:
        """Return the aimodel row for the given id, or None if not found."""
        with self._session_scope() as session:
            return session.get(AIModel, model_id)

    def get_ai_model_by_name_version(self, name: str, version: str | None = None) -> AIModel | None:
        """
        Resolve name (and optional version) to an aimodel row.
        If version is None, returns the row with the highest id for that name (latest registered).
        """
        with self._session_scope() as session:
            if version is not None:
                return session.execute(
                    select(AIModel).where(
                        AIModel.name == name,
                        AIModel.version == version,
                    )
                ).scalar_one_or_none()
            row = session.execute(
                select(AIModel)
                .where(AIModel.name == name)
                .order_by(AIModel.id.desc())
                .limit(1)
            ).fetchone()
            return row[0] if row else None

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

    def save_visual_analysis(
        self,
        asset_id: int,
        analysis: VisualAnalysis,
        *,
        model_name: str | None = None,
        model_version: str | None = None,
    ) -> None:
        """Upsert visual analysis for an asset (JSONB on asset). Optionally stamp model_name/model_version."""
        payload = {
            "description": analysis.description,
            "tags": analysis.tags,
            "ocr_text": analysis.ocr_text,
        }
        if model_name is not None:
            payload["model_name"] = model_name
        if model_version is not None:
            payload["model_version"] = model_version
        payload_json = json.dumps(payload)
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    UPDATE asset SET visual_analysis = CAST(:payload AS jsonb) WHERE id = :asset_id
                """),
                {"payload": payload_json, "asset_id": asset_id},
            )
