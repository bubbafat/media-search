"""Tests for SearchRepository full-text search (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.search_repo import SearchRepository


def _create_tables_and_seed(engine, session_factory):
    """Create all tables and seed schema_version. Return (lib_repo, asset_repo, search_repo)."""
    SQLModel.metadata.create_all(engine)
    session = session_factory()
    try:
        existing = session.get(SystemMetadata, "schema_version")
        if existing is None:
            session.add(SystemMetadata(key="schema_version", value="1"))
            session.commit()
    finally:
        session.close()
    return (
        LibraryRepository(session_factory),
        AssetRepository(session_factory),
        SearchRepository(session_factory),
    )


def test_search_assets_vibe_returns_matching_asset(engine, _session_factory):
    """search_assets with query_string (vibe search) returns assets whose visual_analysis matches."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "search-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Search Lib",
                absolute_path="/tmp/search-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "vibe.jpg", AssetType.image, 1000.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = :lib AND rel_path = 'vibe.jpg'"
            ),
            {"va": '{"description": "man in blue shirt", "tags": ["shirt"], "ocr_text": "none"}', "lib": slug},
        )
        session.commit()
    finally:
        session.close()

    assets = search_repo.search_assets(query_string="blue shirt", limit=50)
    assert len(assets) == 1
    assert assets[0].rel_path == "vibe.jpg"
    assert assets[0].library_id == slug


def test_search_assets_ocr_returns_matching_asset(engine, _session_factory):
    """search_assets with ocr_query returns assets whose ocr_text matches."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "ocr-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="OCR Lib",
                absolute_path="/tmp/ocr-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "menu.jpg", AssetType.image, 1000.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = :lib AND rel_path = 'menu.jpg'"
            ),
            {"va": '{"description": "a photo", "tags": [], "ocr_text": "hamburger and fries"}', "lib": slug},
        )
        session.commit()
    finally:
        session.close()

    assets = search_repo.search_assets(ocr_query="hamburger", limit=50)
    assert len(assets) == 1
    assert assets[0].rel_path == "menu.jpg"


def test_search_assets_library_slug_filters(engine, _session_factory):
    """search_assets with library_slug returns only assets from that library."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        for s, name in [("lib-a", "Lib A"), ("lib-b", "Lib B")]:
            session.add(
                Library(
                    slug=s,
                    name=name,
                    absolute_path=f"/tmp/{s}",
                    is_active=True,
                    sampling_limit=100,
                )
            )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("lib-a", "only.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset("lib-b", "other.jpg", AssetType.image, 1001.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) WHERE library_id = 'lib-a' AND rel_path = 'only.jpg'"
            ),
            {"va": '{"description": "unique word xyz", "tags": [], "ocr_text": ""}'},
        )
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) WHERE library_id = 'lib-b' AND rel_path = 'other.jpg'"
            ),
            {"va": '{"description": "unique word xyz", "tags": [], "ocr_text": ""}'},
        )
        session.commit()
    finally:
        session.close()

    assets = search_repo.search_assets(query_string="xyz", library_slug="lib-a", limit=50)
    assert len(assets) == 1
    assert assets[0].library_id == "lib-a" and assets[0].rel_path == "only.jpg"


def test_search_assets_null_visual_analysis_excluded(engine, _session_factory):
    """search_assets does not return assets with null visual_analysis when using FTS."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "null-va-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Null VA Lib",
                absolute_path="/tmp/null-va",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "no-analysis.jpg", AssetType.image, 1000.0, 1024)
    # visual_analysis remains NULL

    assets = search_repo.search_assets(query_string="anything", limit=50)
    assert len(assets) == 0
