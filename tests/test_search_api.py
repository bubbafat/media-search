"""Search API: /api/search returns structured results for semantic and OCR modes."""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.main import app, _get_library_repo, _get_search_repo, _get_ui_repo
from src.repository.library_repo import LibraryRepository
from src.core import config as config_module
from src.repository.search_repo import SearchRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.ui_repo import UIRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def search_api_postgres():
    """Dedicated Postgres with migrations applied; yield SearchRepository bound to it."""
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

            # Seed: one library, one image, one video + one scene.
            with session_factory() as session:
                session.execute(
                    text(
                        """
                        INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit, deleted_at)
                        VALUES ('testlib', 'Test Library', '/mnt/test', true, 'idle', 100, NULL)
                        """
                    )
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (
                          id, library_id, rel_path, type, mtime, size, status, retry_count, visual_analysis, preview_path
                        ) VALUES (
                          101, 'testlib', 'images/a.jpg', 'image', 0.0, 1, 'completed', 0,
                          '{"description":"a red car", "tags": ["car", "red"], "ocr_text":"HELLO WORLD"}'::jsonb, NULL
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
                          202, 'testlib', 'videos/b.mp4', 'video', 0.0, 1, 'completed', 0,
                          NULL, 'video_scenes/testlib/202/preview.webp'
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
                          202, 3.0, 6.0, 'a car driving',
                          '{"moondream":{"description":"a car driving","ocr_text":"HELLO FROM VIDEO"}}'::jsonb,
                          1.0, '/tmp/rep.jpg', 'forced'
                        )
                        """
                    )
                )
                session.commit()

            search_repo = SearchRepository(session_factory)
            system_metadata_repo = SystemMetadataRepository(session_factory)
            ui_repo = UIRepository(session_factory, system_metadata_repo.get_schema_version)
            yield search_repo, ui_repo
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            config_module._config = None  # type: ignore[attr-defined]


def test_api_search_semantic_returns_image_and_video(search_api_postgres):
    search_repo, ui_repo = search_api_postgres
    app.dependency_overrides[_get_search_repo] = lambda: search_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo
    try:
        client = TestClient(app)
        res = client.get("/api/search", params={"q": "car"})
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        ids = {item["asset_id"] for item in data}
        assert 101 in ids
        assert 202 in ids

        by_id = {item["asset_id"]: item for item in data}
        assert by_id[101]["type"] == "image"
        assert by_id[101]["thumbnail_url"].endswith("/media/testlib/thumbnails/101/101.jpg")
        assert by_id[101]["preview_url"] is None
        assert by_id[101]["match_ratio"] == 100.0
        assert by_id[101]["library_slug"] == "testlib"
        assert by_id[101]["library_name"] == "Test Library"
        assert by_id[101]["filename"] == "a.jpg"

        assert by_id[202]["type"] == "video"
        assert by_id[202]["preview_url"].endswith("/media/video_scenes/testlib/202/preview.webp")
        assert by_id[202]["best_scene_ts"] == "00:03"
        assert by_id[202]["best_scene_ts_seconds"] == 3.0
        assert 0.0 <= by_id[202]["match_ratio"] <= 100.0
        assert by_id[202]["library_slug"] == "testlib"
        assert by_id[202]["library_name"] == "Test Library"
        assert by_id[202]["filename"] == "b.mp4"
    finally:
        app.dependency_overrides.pop(_get_search_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)


def test_api_search_ocr_uses_ocr_text(search_api_postgres):
    search_repo, ui_repo = search_api_postgres
    app.dependency_overrides[_get_search_repo] = lambda: search_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo
    try:
        client = TestClient(app)
        res = client.get("/api/search", params={"ocr": "hello"})
        assert res.status_code == 200
        data = res.json()
        ids = {item["asset_id"] for item in data}
        assert 101 in ids
        assert 202 in ids
    finally:
        app.dependency_overrides.pop(_get_search_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)


def test_api_search_tag_returns_matching_assets(search_api_postgres):
    """GET /api/search?tag=car returns assets that have that tag."""
    search_repo, ui_repo = search_api_postgres
    app.dependency_overrides[_get_search_repo] = lambda: search_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo
    try:
        client = TestClient(app)
        res = client.get("/api/search", params={"tag": "car"})
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        ids = [item["asset_id"] for item in data]
        assert 101 in ids
    finally:
        app.dependency_overrides.pop(_get_search_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)


def test_api_search_library_filter(search_api_postgres):
    """GET /api/search?library=testlib restricts to that library."""
    search_repo, ui_repo = search_api_postgres
    app.dependency_overrides[_get_search_repo] = lambda: search_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo
    try:
        client = TestClient(app)
        res = client.get("/api/search", params={"q": "car", "library": "testlib"})
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        for item in data:
            assert item["library_slug"] == "testlib"
    finally:
        app.dependency_overrides.pop(_get_search_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)


def test_api_search_type_filter_image(search_api_postgres):
    """GET /api/search?q=car&type=image returns only images."""
    search_repo, ui_repo = search_api_postgres
    app.dependency_overrides[_get_search_repo] = lambda: search_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo
    try:
        client = TestClient(app)
        res = client.get("/api/search", params={"q": "car", "type": "image"})
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        for item in data:
            assert item["type"] == "image"
    finally:
        app.dependency_overrides.pop(_get_search_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)


def test_api_search_type_filter_video(search_api_postgres):
    """GET /api/search?q=car&type=video returns only videos."""
    search_repo, ui_repo = search_api_postgres
    app.dependency_overrides[_get_search_repo] = lambda: search_repo
    app.dependency_overrides[_get_ui_repo] = lambda: ui_repo
    try:
        client = TestClient(app)
        res = client.get("/api/search", params={"q": "car", "type": "video"})
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        for item in data:
            assert item["type"] == "video"
    finally:
        app.dependency_overrides.pop(_get_search_repo, None)
        app.dependency_overrides.pop(_get_ui_repo, None)


def test_api_libraries_returns_list(search_api_postgres):
    """GET /api/libraries returns non-deleted libraries."""
    search_repo, ui_repo = search_api_postgres
    library_repo = LibraryRepository(search_repo._session_factory)
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/libraries")
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        for item in data:
            assert "slug" in item
            assert "name" in item
    finally:
        app.dependency_overrides.pop(_get_library_repo, None)

