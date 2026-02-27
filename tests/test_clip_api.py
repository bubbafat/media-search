"""Tests for clip API: GET /api/asset/{id}/clip."""

import os
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from tests.conftest import clear_app_db_caches
from src.api.main import app, _get_asset_repo, _get_library_repo
from src.core.path_resolver import _reset_session_factory_for_tests
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository

pytestmark = [pytest.mark.slow]


def _create_test_video(tmp_path: Path, rel_path: str, duration: float = 5.0) -> Path:
    """Create a minimal test video. Returns full path."""
    full = tmp_path / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration}:size=320x240:rate=30",
        "-t",
        str(duration),
        str(full),
    ]
    r = subprocess.run(cmd, capture_output=True, timeout=10)
    assert r.returncode == 0, r.stderr.decode()
    assert full.exists()
    return full


@pytest.fixture(scope="module")
def clip_api_postgres(tmp_path_factory):
    """Postgres with migrations; library pointing to tmp dir with real video; asset 302 video."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from testcontainers.postgres import PostgresContainer

    lib_root = tmp_path_factory.mktemp("clip_lib")
    _create_test_video(lib_root, "clips/sunset.mp4", duration=10.0)
    lib_path = str(lib_root.resolve())

    with PostgresContainer("postgres:16-alpine") as postgres:
        url = postgres.get_connection_url()
        prev_db = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        clear_app_db_caches()
        _reset_session_factory_for_tests()
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
                        VALUES ('clip-lib', 'Clip Library', :path, true, 'idle', 100, NULL)
                        """
                    ),
                    {"path": lib_path},
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (
                          id, library_id, rel_path, type, mtime, size, status, retry_count, visual_analysis, preview_path
                        ) VALUES (
                          401, 'clip-lib', 'photos/cat.jpg', 'image', 0.0, 1, 'completed', 0, NULL, NULL
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
                          402, 'clip-lib', 'clips/sunset.mp4', 'video', 0.0, 1, 'completed', 0, NULL, NULL
                        )
                        """
                    )
                )
                # Library with non-existent source file
                empty_dir = tmp_path_factory.mktemp("empty_clip_lib")
                session.execute(
                    text(
                        """
                        INSERT INTO library (slug, name, absolute_path, is_active, scan_status, sampling_limit, deleted_at)
                        VALUES ('missing-src-lib', 'Missing', :path, true, 'idle', 100, NULL)
                        """
                    ),
                    {"path": str(empty_dir.resolve())},
                )
                session.execute(
                    text(
                        """
                        INSERT INTO asset (
                          id, library_id, rel_path, type, mtime, size, status, retry_count, visual_analysis, preview_path
                        ) VALUES (
                          403, 'missing-src-lib', 'nonexistent.mp4', 'video', 0.0, 1, 'completed', 0, NULL, NULL
                        )
                        """
                    )
                )
                session.commit()

            asset_repo = AssetRepository(session_factory)
            library_repo = LibraryRepository(session_factory)
            yield asset_repo, library_repo, lib_path
        finally:
            if prev_db is not None:
                os.environ["DATABASE_URL"] = prev_db
            else:
                os.environ.pop("DATABASE_URL", None)
            clear_app_db_caches()
            _reset_session_factory_for_tests()


def test_clip_api_asset_detail_works(clip_api_postgres):
    """Sanity: asset detail returns 200 with our fixture overrides."""
    asset_repo, library_repo, _ = clip_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/402")
        assert res.status_code == 200
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_clip_api_video_source_exists_302_redirect(clip_api_postgres, tmp_path):
    """GET /api/asset/{id}/clip?ts=5 for video with source file returns 302 redirect."""
    asset_repo, library_repo, lib_path = clip_api_postgres
    asset = asset_repo.get_asset_by_id(402)
    assert asset is not None, "Asset 402 should exist"
    source_video = Path(lib_path) / asset.rel_path
    assert source_video.exists(), f"Source video should exist at {source_video}"

    # Pre-create clip file so we skip extraction (faster, no FFmpeg race)
    clip_dir = tmp_path / "video_clips" / "clip-lib" / "402"
    clip_dir.mkdir(parents=True, exist_ok=True)
    (clip_dir / "clip_5.mp4").write_bytes(b"fake-mp4")

    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo

    with patch("src.api.main.get_config") as mock_config:
        mock_config.return_value = type("Config", (), {"data_dir": str(tmp_path)})()
        with patch("src.api.main.resolve_path", return_value=source_video):
            try:
                client = TestClient(app, follow_redirects=False)
                res = client.get("/api/asset/402/clip", params={"ts": 5.0})
                assert res.status_code == 302, (
                    f"Expected 302, got {res.status_code}. "
                    f"detail={res.json() if 'application/json' in res.headers.get('content-type', '') else res.text}"
                )
                assert res.headers["location"] == "/media/video_clips/clip-lib/402/clip_5.mp4"
            finally:
                app.dependency_overrides.pop(_get_asset_repo, None)
                app.dependency_overrides.pop(_get_library_repo, None)


def test_clip_api_non_video_returns_400(clip_api_postgres):
    """GET /api/asset/{id}/clip for image asset returns 400."""
    asset_repo, library_repo, _ = clip_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/401/clip", params={"ts": 1.0})
        assert res.status_code == 400
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_clip_api_missing_asset_returns_404(clip_api_postgres):
    """GET /api/asset/{id}/clip for non-existent asset returns 404."""
    asset_repo, library_repo, _ = clip_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/999999/clip", params={"ts": 5.0})
        assert res.status_code == 404
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_clip_api_source_file_missing_returns_404(clip_api_postgres):
    """GET /api/asset/{id}/clip when source file does not exist returns 404."""
    asset_repo, library_repo, _ = clip_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo
    try:
        client = TestClient(app)
        res = client.get("/api/asset/403/clip", params={"ts": 5.0})
        assert res.status_code == 404
    finally:
        app.dependency_overrides.pop(_get_asset_repo, None)
        app.dependency_overrides.pop(_get_library_repo, None)


def test_clip_api_extraction_failure_returns_500(clip_api_postgres, tmp_path):
    """When extract_clip fails, endpoint returns 500."""
    asset_repo, library_repo, _ = clip_api_postgres
    app.dependency_overrides[_get_asset_repo] = lambda: asset_repo
    app.dependency_overrides[_get_library_repo] = lambda: library_repo

    with patch("src.api.main.get_config") as mock_config:
        mock_config.return_value = type("Config", (), {"data_dir": str(tmp_path)})()
        with patch("src.api.main.extract_clip", new_callable=AsyncMock, return_value=False):
            try:
                client = TestClient(app)
                res = client.get("/api/asset/402/clip", params={"ts": 5.0})
                assert res.status_code == 500
            finally:
                app.dependency_overrides.pop(_get_asset_repo, None)
                app.dependency_overrides.pop(_get_library_repo, None)
