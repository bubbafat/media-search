"""Library API: /api/library-assets returns paginated assets for a library."""

import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.main import app, _get_asset_repo, _get_library_repo
from src.core import config as config_module
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository

pytestmark = [pytest.mark.slow]


@pytest.fixture(scope="module")
def library_api_postgres():
    """Dedicated Postgres with migrations applied; seed library and assets."""
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
                        VALUES ('lib1', 'Library One', '/mnt/lib1', true, 'idle', 100, NULL)
                        """
                    )
                )
                for i, (path, t) in enumerate(
                    [("a.jpg", "image"), ("b.mp4", "video"), ("c.png", "image")]
                ):
                    session.execute(
                        text(
                            """
                            INSERT INTO asset (library_id, rel_path, type, mtime, size, status, retry_count)
                            VALUES (:lib, :path, :type, :mtime, :size, 'completed', 0)
                            """
                        ),
                        {"lib": "lib1", "path": path, "type": t, "mtime": 100.0 + i, "size": 1000 * (i + 1)},
                    )
                session.commit()

            asset_repo = AssetRepository(session_factory)
            library_repo = LibraryRepository(session_factory)
            yield asset_repo, library_repo
        finally:
            if prev is not None:
                os.environ["DATABASE_URL"] = prev
            else:
                os.environ.pop("DATABASE_URL", None)
            config_module._config = None  # type: ignore[attr-defined]


def test_library_assets_returns_items_and_has_more(library_api_postgres):
    """GET /api/library-assets returns items and has_more."""
    asset_repo, library_repo = library_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/library-assets", params={"library": "lib1"})
        assert res.status_code == 200
        data = res.json()
        assert "items" in data
        assert "has_more" in data
        items = data["items"]
        assert len(items) >= 3
        has_more = data["has_more"]
        # With only 3 assets and limit 50, has_more should be False
        assert has_more is False
        ids = {item["asset_id"] for item in items}
        assert len(ids) == 3
        for item in items:
            assert item["library_slug"] == "lib1"
            assert item["library_name"] == "Library One"
            assert item["type"] in ("image", "video")
            assert "thumbnail_url" in item
            assert item["match_ratio"] == 100.0
            assert item["best_scene_ts"] is None
            assert item["best_scene_ts_seconds"] is None
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_library_assets_sort_and_order(library_api_postgres):
    """GET /api/library-assets respects sort and order."""
    asset_repo, library_repo = library_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/library-assets", params={"library": "lib1", "sort": "name", "order": "asc"})
        assert res.status_code == 200
        data = res.json()
        items = data["items"]
        filenames = [item["filename"] for item in items]
        assert filenames == sorted(filenames)
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_library_assets_pagination(library_api_postgres):
    """GET /api/library-assets paginates with offset and limit."""
    asset_repo, library_repo = library_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res1 = client.get("/api/library-assets", params={"library": "lib1", "limit": 2, "offset": 0})
        assert res1.status_code == 200
        d1 = res1.json()
        assert len(d1["items"]) == 2
        assert d1["has_more"] is True

        res2 = client.get("/api/library-assets", params={"library": "lib1", "limit": 2, "offset": 2})
        assert res2.status_code == 200
        d2 = res2.json()
        assert len(d2["items"]) == 1
        assert d2["has_more"] is False

        ids1 = {item["asset_id"] for item in d1["items"]}
        ids2 = {item["asset_id"] for item in d2["items"]}
        assert ids1.isdisjoint(ids2)
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_library_assets_requires_library():
    """GET /api/library-assets returns 422 when library is missing."""
    client = TestClient(app)
    res = client.get("/api/library-assets")
    assert res.status_code == 422
