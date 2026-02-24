"""Verify Core Data Models: tables, FKs, and relationships (testcontainers Postgres)."""

from sqlmodel import SQLModel

from src.models.entities import AIModel, Asset, AssetStatus, AssetType, Library


def test_asset_fk_to_library_and_aimodel(session, engine):
    """Create Library and AIModel, insert an Asset, verify FK linkage."""
    SQLModel.metadata.create_all(engine)

    aimodel = AIModel(slug="clip", version="1")
    session.add(aimodel)
    session.flush()
    assert aimodel.id is not None

    library = Library(
        slug="lib1",
        name="Library 1",
        target_tagger_id=aimodel.id,
    )
    session.add(library)
    session.flush()

    asset = Asset(
        library_id=library.slug,
        rel_path="a/b.jpg",
        type=AssetType.image,
        status=AssetStatus.pending,
        tags_model_id=aimodel.id,
    )
    session.add(asset)
    session.flush()
    assert asset.id is not None

    assert asset.library_id == library.slug
    assert asset.tags_model_id == aimodel.id
