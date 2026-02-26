"""Asset detail API: GET /api/asset/{id} returns description, tags, ocr_text."""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.main import app, _get_asset_repo, _get_video_scene_repo
from src.core import config as config_module
from src.repository.asset_repo import AssetRepository
from src.repository.video_scene_repo import VideoSceneRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def asset_detail_api_postgres():
    """Postgres with migrations applied; seed one image (visual_analysis) and one video (scene metadata)."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        config_module._config = None  # type: ignore[attr-defined]
        try:
            from alembic import command
            from alembic.config import Config

            alembic_cfg = Config("alembic.ini")
            alembic_cfg.set_main_option("script_location", "migrations")
            command.upgrade(alembic_cfg, "head")

            engine = create_engine(url, pool_pre_ping=True)
            session_factory = sessionmaker(
                engine, autocommit=False, autoflush=False, expire_on_commit=False
            )

            with session_factory() as session:
                session.execute(
                    text(
                        """
                        INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit, deleted_at)
                        VALUES ('detail-lib', 'Detail Library', '/mnt/detail', true, 'idle', 100, NULL)
                        """
                    )
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (
                          id, library_id, rel_path, type, mtime, size, status, retry_count, visual_analysis, preview_path
                        ) VALUES (
                          301, 'detail-lib', 'photos/cat.jpg', 'image', 0.0, 1, 'completed', 0,
                          '{"description":"A cat on a mat", "tags": ["cat", "mat"], "ocr_text": "MEOW"}'::jsonb, NULL
                        )
                        """
                    )
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (
                          id, library_id, rel_path, type, mtime, size, status, retry_count, visual_analysis, preview_path
                        ) VALUES (
                          302, 'detail-lib', 'clips/sunset.mp4', 'video', 0.0, 1, 'completed', 0,
                          NULL, NULL
                        )
                        """
                    )
                )
                session.execute(
                    text(
                        """
                        INSERT INTO video_scenes (
                          asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason
                        ) VALUES (
                          302, 5.0, 10.0, 'sunset beach',
                          '{"moondream":{"description":"sunset beach","tags":["beach","sunset"],"ocr_text":"SUNSET"}}'::jsonb,
                          1.0, 'video_scenes/detail-lib/302/5.000_10.000.jpg', 'forced'
                        )
                        """
                    )
                )
                session.commit()

            asset_repo = AssetRepository(session_factory)
            video_scene_repo = VideoSceneRepository(session_factory)
            yield asset_repo, video_scene_repo
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            config_module._config = None  # type: ignore[attr-defined]


def test_api_asset_detail_image_returns_visual_analysis(asset_detail_api_postgres):
    """GET /api/asset/{id} for an image returns description, tags, ocr_text from visual_analysis."""
    asset_repo, video_scene_repo = asset_detail_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_video_scene_repo] = lambda: video_scene_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/301")
        assert res.status_code == 200
        data = res.json()
        assert data["description"] == "A cat on a mat"
        assert data["tags"] == ["cat", "mat"]
        assert data["ocr_text"] == "MEOW"
        assert data["library_slug"] == "detail-lib"
        assert data["filename"] == "cat.jpg"
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_video_scene_repo, None)


def test_api_asset_detail_video_with_best_scene_ts_returns_scene_metadata(
    asset_detail_api_postgres,
):
    """GET /api/asset/{id}?best_scene_ts=5.0 for a video returns that scene's moondream metadata."""
    asset_repo, video_scene_repo = asset_detail_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_video_scene_repo] = lambda: video_scene_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/302", params={"best_scene_ts": 5.0})
        assert res.status_code == 200
        data = res.json()
        assert data["description"] == "sunset beach"
        assert data["tags"] == ["beach", "sunset"]
        assert data["ocr_text"] == "SUNSET"
        assert data["library_slug"] == "detail-lib"
        assert data["filename"] == "sunset.mp4"
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_video_scene_repo, None)


def test_api_asset_detail_missing_returns_404(asset_detail_api_postgres):
    """GET /api/asset/{id} for non-existent id returns 404."""
    asset_repo, video_scene_repo = asset_detail_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_video_scene_repo] = lambda: video_scene_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/999999")
        assert res.status_code == 404
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_video_scene_repo, None)
