"""Slow/integration tests for similarity API with Postgres + Quickwit."""

import json
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from testcontainers.postgres import PostgresContainer

from tests.conftest import QUICKWIT_TEST_URL, clear_app_db_caches
from src.api.main import app, _get_asset_repo, _get_ui_repo
from src.core.config import get_config, reset_config
from src.models.entities import AssetStatus, AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_model_policy_repo import LibraryModelPolicyRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.ui_repo import UIRepository
from src.repository.quickwit_search_repo import QuickwitSearchRepository

pytestmark = [pytest.mark.slow, pytest.mark.quickwit]


@pytest.fixture(scope="module")
def similarity_api_env():
    """Postgres with migrations applied plus Quickwit index + policy for similarity tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev_db = os.environ.get("DATABASE_URL")
        prev_qw = os.environ.get("QUICKWIT_URL")
        os.environ["DATABASE_URL"] = url
        os.environ["QUICKWIT_URL"] = QUICKWIT_TEST_URL
        clear_app_db_caches()
        reset_config()
        try:
            from alembic import command
            from alembic.config import Config

            alembic_cfg = Config("alembic.ini")
            alembic_cfg.set_main_option("script_location", "migrations")
            command.upgrade(alembic_cfg, "head")

            from sqlalchemy import create_engine

            engine = create_engine(url, pool_pre_ping=True)
            SQLModel.metadata.create_all(engine)
            session_factory = sessionmaker(
                engine, autocommit=False, autoflush=False, expire_on_commit=False
            )

            # Seed schema_version
            with session_factory() as session:
                if session.get(SystemMetadata, "schema_version") is None:
                    session.add(SystemMetadata(key="schema_version", value="1"))
                    session.commit()

            asset_repo = AssetRepository(session_factory)
            system_metadata_repo = SystemMetadataRepository(session_factory)
            ui_repo = UIRepository(session_factory, system_metadata_repo.get_schema_version)
            policy_repo = LibraryModelPolicyRepository(session_factory)

            # Create library and assets.
            with session_factory() as session:
                lib = Library(
                    slug="tuta",
                    name="Tuta Library",
                    absolute_path="/mnt/tuta",
                    is_active=True,
                )
                session.add(lib)
                session.commit()

            # Source asset with description.
            asset_repo.upsert_asset("tuta", "photos/source.jpg", AssetType.image, 0.0, 1)
            # Similar target asset.
            asset_repo.upsert_asset("tuta", "photos/target.jpg", AssetType.image, 0.0, 1)

            with session_factory() as session:
                session.execute(
                    text(
                        """
                        UPDATE asset
                        SET status = :status,
                            visual_analysis = CAST(:va AS jsonb)
                        WHERE rel_path = :rel_path
                        """
                    ),
                    {
                        "status": AssetStatus.completed.value,
                        "va": json.dumps({"description": "a red car in a field"}),
                        "rel_path": "photos/source.jpg",
                    },
                )
                session.execute(
                    text(
                        """
                        UPDATE asset
                        SET status = :status,
                            visual_analysis = CAST(:va AS jsonb)
                        WHERE rel_path = :rel_path
                        """
                    ),
                    {
                        "status": AssetStatus.completed.value,
                        "va": json.dumps({"description": "a bright red car"}),
                        "rel_path": "photos/target.jpg",
                    },
                )
                session.commit()

                src_id = session.execute(
                    text(
                        "SELECT id FROM asset WHERE library_id = 'tuta' AND rel_path = 'photos/source.jpg'"
                    )
                ).scalar_one()
                tgt_id = session.execute(
                    text(
                        "SELECT id FROM asset WHERE library_id = 'tuta' AND rel_path = 'photos/target.jpg'"
                    )
                ).scalar_one()

            # Create Quickwit index and policy.
            index_name = f"media_scenes_tuta_similarity"
            qw = QuickwitSearchRepository(get_config().quickwit_url, index_name)
            qw.create_index(index_name, "quickwit/media_scenes_schema.json")

            policy_repo.upsert(
                library_slug="tuta",
                active_index_name=index_name,
                locked=False,
                promotion_progress=0.0,
            )

            # Index minimal image documents for source and target.
            now = int(0)
            qw.index_document(
                index_name,
                {
                    "id": f"asset_{src_id}",
                    "scene_id": 0,
                    "asset_id": src_id,
                    "library_slug": "tuta",
                    "capture_ts": None,
                    "country": None,
                    "region": None,
                    "city": None,
                    "camera_make": None,
                    "camera_model": None,
                    "color_space": None,
                    "generation_hint": None,
                    "resolution_w": None,
                    "resolution_h": None,
                    "duration_sec": None,
                    "frame_rate": None,
                    "has_face": None,
                    "sharpness_score": None,
                    "scene_start_ts": None,
                    "scene_end_ts": None,
                    "description": "a red car in a field",
                    "ocr_text": None,
                    "tags": ["car"],
                    "rep_frame_path": "/previews/source.jpg",
                    "head_clip_path": None,
                    "preview_ready": True,
                    "playable": False,
                    "searchable": True,
                    "offline_ready": False,
                    "indexed_at": now,
                },
            )
            qw.index_document(
                index_name,
                {
                    "id": f"asset_{tgt_id}",
                    "scene_id": 0,
                    "asset_id": tgt_id,
                    "library_slug": "tuta",
                    "capture_ts": None,
                    "country": None,
                    "region": None,
                    "city": None,
                    "camera_make": None,
                    "camera_model": None,
                    "color_space": None,
                    "generation_hint": None,
                    "resolution_w": None,
                    "resolution_h": None,
                    "duration_sec": None,
                    "frame_rate": None,
                    "has_face": None,
                    "sharpness_score": None,
                    "scene_start_ts": None,
                    "scene_end_ts": None,
                    "description": "a bright red car",
                    "ocr_text": None,
                    "tags": ["car"],
                    "rep_frame_path": "/previews/target.jpg",
                    "head_clip_path": None,
                    "preview_ready": True,
                    "playable": False,
                    "searchable": True,
                    "offline_ready": False,
                    "indexed_at": now,
                },
            )

            app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
            app.dependency_overrides[_get_ui_repo] = lambda: ui_repo

            yield {
                "session_factory": session_factory,
                "asset_repo": asset_repo,
                "ui_repo": ui_repo,
                "src_id": src_id,
                "tgt_id": tgt_id,
                "index_name": index_name,
                "quickwit": qw,
            }
        finally:
            try:
                if "qw" in locals():
                    qw.delete_index(index_name)  # type: ignore[arg-type]
            except Exception:
                pass
            if prev_db is not None:
                os.environ["DATABASE_URL"] = prev_db
            else:
                os.environ.pop("DATABASE_URL", None)
            if prev_qw is not None:
                os.environ["QUICKWIT_URL"] = prev_qw
            else:
                os.environ.pop("QUICKWIT_URL", None)
            app.dependency_overrides.pop(_get_asset_repo, None)
            app.dependency_overrides.pop(_get_ui_repo, None)
            reset_config()
            clear_app_db_caches()


def test_asset_with_no_description_returns_422(similarity_api_env):
    """Asset exists but has no description -> 422."""
    session_factory = similarity_api_env["session_factory"]
    with session_factory() as session:
        session.execute(
            text(
                """
                INSERT INTO asset (id, library_id, rel_path, type, mtime, size, status, retry_count, visual_analysis)
                VALUES (9999, 'tuta', 'photos/nodec.jpg', 'image', 0.0, 1, 'completed', 0, '{}'::jsonb)
                """
            )
        )
        session.commit()

    client = TestClient(app)
    res = client.get("/api/assets/9999/similar")
    assert res.status_code == 422
    assert "Asset has no description" in res.json().get("detail", "")


def test_asset_not_found_returns_404(similarity_api_env):
    """Non-existent asset id -> 404."""
    client = TestClient(app)
    res = client.get("/api/assets/424242/similar")
    assert res.status_code == 404


def test_asset_with_description_returns_similarity_result_with_correct_shape(similarity_api_env):
    """Happy path: SimilarityResultOut shape and source asset exclusion."""
    src_id = similarity_api_env["src_id"]
    tgt_id = similarity_api_env["tgt_id"]

    client = TestClient(app)
    res = client.get(f"/api/assets/{src_id}/similar")
    assert res.status_code == 200
    data = res.json()

    assert data["source_asset_id"] == src_id
    assert "results" in data
    assert isinstance(data["results"], list)
    assert "threshold_used" in data
    assert "scope" in data

    ids = {item["asset_id"] for item in data["results"]}
    # Source asset must not appear in its own results.
    assert src_id not in ids
    # Target asset is a strong candidate; should be present.
    assert tgt_id in ids

