"""Tests for SystemMetadataRepository AI model and visual analysis methods (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.ai.schema import ModelCard, VisualAnalysis
from src.models.entities import AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return SystemMetadataRepository."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return SystemMetadataRepository(session_factory)


def test_get_or_create_ai_model_creates_then_returns_same_id(engine, _session_factory):
    """get_or_create_ai_model inserts on first call and returns same id on second."""
    repo = _create_tables_and_seed(engine, _session_factory)
    card = ModelCard(name="mock-analyzer", version="1.0")
    id1 = repo.get_or_create_ai_model(card)
    id2 = repo.get_or_create_ai_model(card)
    assert id1 == id2
    assert id1 is not None


def test_get_all_ai_models_returns_all_after_add(engine, _session_factory):
    """get_all_ai_models returns all models after add (may include models from other tests)."""
    repo = _create_tables_and_seed(engine, _session_factory)
    repo.add_ai_model("a", "1")
    repo.add_ai_model("b", "2")
    models = repo.get_all_ai_models()
    names_versions = [(m.name, m.version) for m in models]
    assert ("a", "1") in names_versions
    assert ("b", "2") in names_versions


def test_add_ai_model_returns_entity(engine, _session_factory):
    """add_ai_model inserts and returns the created AIModel."""
    repo = _create_tables_and_seed(engine, _session_factory)
    model = repo.add_ai_model("clip", "2.0")
    assert model.id is not None
    assert model.name == "clip"
    assert model.version == "2.0"


def test_remove_ai_model_success_when_no_refs(engine, _session_factory):
    """remove_ai_model deletes and returns True when no assets reference the model."""
    repo = _create_tables_and_seed(engine, _session_factory)
    repo.add_ai_model("gone", "1")
    removed = repo.remove_ai_model("gone")
    assert removed is True
    names = [m.name for m in repo.get_all_ai_models()]
    assert "gone" not in names


def test_remove_ai_model_returns_false_when_not_found(engine, _session_factory):
    """remove_ai_model returns False when no row matches name."""
    repo = _create_tables_and_seed(engine, _session_factory)
    removed = repo.remove_ai_model("nonexistent")
    assert removed is False


def test_remove_ai_model_raises_when_referenced(engine, _session_factory):
    """remove_ai_model raises ValueError when assets reference the model."""
    repo = _create_tables_and_seed(engine, _session_factory)
    asset_repo = AssetRepository(_session_factory)
    session = _session_factory()
    try:
        session.add(Library(slug="lib1", name="Lib1", absolute_path="/tmp/lib1", is_active=True, sampling_limit=100))
        session.commit()
    finally:
        session.close()

    model = repo.add_ai_model("used", "1")
    asset_repo.upsert_asset("lib1", "x.jpg", AssetType.image, 0.0, 0)
    session = _session_factory()
    try:
        session.execute(
            text("UPDATE asset SET status = 'completed', analysis_model_id = :mid WHERE library_id = 'lib1' AND rel_path = 'x.jpg'"),
            {"mid": model.id},
        )
        session.commit()
    finally:
        session.close()

    with pytest.raises(ValueError, match="Cannot delete model 'used'. It is currently referenced by 1 assets"):
        repo.remove_ai_model("used")


def test_save_visual_analysis_updates_asset_jsonb(engine, _session_factory):
    """save_visual_analysis writes description, tags, ocr_text to asset.visual_analysis."""
    repo = _create_tables_and_seed(engine, _session_factory)
    asset_repo = AssetRepository(_session_factory)
    session = _session_factory()
    try:
        session.add(Library(slug="valib", name="Va", absolute_path="/tmp/va", is_active=True, sampling_limit=100))
        session.commit()
    finally:
        session.close()
    asset_repo.upsert_asset("valib", "img.jpg", AssetType.image, 0.0, 0)
    session = _session_factory()
    try:
        row = session.execute(text("SELECT id FROM asset WHERE library_id = 'valib' AND rel_path = 'img.jpg'")).fetchone()
        asset_id = row[0]
    finally:
        session.close()

    analysis = VisualAnalysis(
        description="A cat on a mat.",
        tags=["cat", "mat"],
        ocr_text="HELLO",
    )
    repo.save_visual_analysis(asset_id, analysis)

    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT visual_analysis FROM asset WHERE id = :id"),
            {"id": asset_id},
        ).fetchone()
        assert row is not None
        va = row[0]
        assert va is not None
        assert va["description"] == "A cat on a mat."
        assert va["tags"] == ["cat", "mat"]
        assert va["ocr_text"] == "HELLO"
    finally:
        session.close()
