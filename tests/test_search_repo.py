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

    results = search_repo.search_assets(query_string="blue shirt", limit=50)
    assert len(results) == 1
    asset, rank = results[0]
    assert asset.rel_path == "vibe.jpg"
    assert asset.library_id == slug
    assert isinstance(rank, float) and rank > 0


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

    results = search_repo.search_assets(ocr_query="hamburger", limit=50)
    assert len(results) == 1
    asset, rank = results[0]
    assert asset.rel_path == "menu.jpg"
    assert isinstance(rank, float) and rank > 0


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

    results = search_repo.search_assets(query_string="xyz", library_slug="lib-a", limit=50)
    assert len(results) == 1
    asset, _ = results[0]
    assert asset.library_id == "lib-a" and asset.rel_path == "only.jpg"


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

    results = search_repo.search_assets(query_string="anything", limit=50)
    assert len(results) == 0


def test_search_assets_ordered_by_rank_descending(engine, _session_factory):
    """search_assets with query_string returns results ordered by relevance (rank desc)."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "rank-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Rank Lib",
                absolute_path="/tmp/rank-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "strong.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset(slug, "weak.jpg", AssetType.image, 1001.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = :lib AND rel_path = 'strong.jpg'"
            ),
            {"va": '{"description": "man in blue shirt", "tags": ["shirt", "blue"], "ocr_text": ""}', "lib": slug},
        )
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = :lib AND rel_path = 'weak.jpg'"
            ),
            {"va": '{"description": "blue sky and far away a shirt", "tags": [], "ocr_text": ""}', "lib": slug},
        )
        session.commit()
    finally:
        session.close()

    results = search_repo.search_assets(
        query_string="blue shirt", library_slug=slug, limit=50
    )
    assert len(results) == 2
    first_asset, first_rank = results[0]
    second_asset, second_rank = results[1]
    assert first_rank >= second_rank
    assert first_asset.rel_path == "strong.jpg"
    assert second_asset.rel_path == "weak.jpg"
