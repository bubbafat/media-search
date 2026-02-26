"""Tests for SearchRepository full-text search (testcontainers Postgres)."""

import pytest
from sqlalchemy import text
from sqlmodel import SQLModel

from src.models.entities import AssetType, Library, SystemMetadata
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.search_repo import SearchRepository

pytestmark = [pytest.mark.slow]


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
    item = results[0]
    assert item.asset.rel_path == "vibe.jpg"
    assert item.asset.library_id == slug
    assert isinstance(item.final_rank, float) and item.final_rank > 0
    assert item.match_ratio == 1.0
    assert item.best_scene_ts is None


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
    item = results[0]
    assert item.asset.rel_path == "menu.jpg"
    assert isinstance(item.final_rank, float) and item.final_rank > 0
    assert item.match_ratio == 1.0
    assert item.best_scene_ts is None


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

    results = search_repo.search_assets(query_string="xyz", library_slugs=["lib-a"], limit=50)
    assert len(results) == 1
    item = results[0]
    assert item.asset.library_id == "lib-a" and item.asset.rel_path == "only.jpg"


def test_search_assets_library_slugs_multiple(engine, _session_factory):
    """search_assets with library_slugs returns only assets from those libraries."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        for s, name in [("lib-m1", "Lib M1"), ("lib-m2", "Lib M2"), ("lib-m3", "Lib M3")]:
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

    asset_repo.upsert_asset("lib-m1", "a.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset("lib-m2", "b.jpg", AssetType.image, 1001.0, 1024)
    asset_repo.upsert_asset("lib-m3", "c.jpg", AssetType.image, 1002.0, 1024)
    session = _session_factory()
    try:
        for lib, path, va in [
            ("lib-m1", "a.jpg", '{"description": "unique xyz", "tags": [], "ocr_text": ""}'),
            ("lib-m2", "b.jpg", '{"description": "unique xyz", "tags": [], "ocr_text": ""}'),
            ("lib-m3", "c.jpg", '{"description": "unique xyz", "tags": [], "ocr_text": ""}'),
        ]:
            session.execute(
                text(
                    "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                    "WHERE library_id = :lib AND rel_path = :path"
                ),
                {"va": va, "lib": lib, "path": path},
            )
        session.commit()
    finally:
        session.close()

    results = search_repo.search_assets(
        query_string="xyz", library_slugs=["lib-m1", "lib-m2"], limit=50
    )
    assert len(results) == 2
    slugs = {r.asset.library_id for r in results}
    assert slugs == {"lib-m1", "lib-m2"}
    assert "lib-m3" not in slugs


def test_search_assets_asset_types_image_only(engine, _session_factory):
    """search_assets with asset_types=['image'] returns only images."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "type-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Type Lib",
                absolute_path=f"/tmp/{slug}",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "img.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset(slug, "vid.mp4", AssetType.video, 1001.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = :lib AND rel_path = 'img.jpg'"
            ),
            {"va": '{"description": "shared term", "tags": [], "ocr_text": ""}', "lib": slug},
        )
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :lib AND rel_path = 'vid.mp4'"),
            {"lib": slug},
        ).fetchone()
        assert row is not None
        asset_id = row[0]
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:aid, 0.0, 5.0, 'shared term', CAST(:meta AS jsonb), 0.0, '', 'phash')"
            ),
            {"aid": asset_id, "meta": '{"moondream": {"description": "shared term", "tags": [], "ocr_text": null}}'},
        )
        session.commit()
    finally:
        session.close()

    results = search_repo.search_assets(
        query_string="shared term", asset_types=["image"], limit=50
    )
    assert len(results) == 1
    assert results[0].asset.type == AssetType.image
    assert results[0].asset.rel_path == "img.jpg"


def test_search_assets_asset_types_video_only(engine, _session_factory):
    """search_assets with asset_types=['video'] returns only videos."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "video-type-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Video Type Lib",
                absolute_path=f"/tmp/{slug}",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "img.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset(slug, "vid.mp4", AssetType.video, 1001.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = :lib AND rel_path = 'img.jpg'"
            ),
            {"va": '{"description": "shared term", "tags": [], "ocr_text": ""}', "lib": slug},
        )
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :lib AND rel_path = 'vid.mp4'"),
            {"lib": slug},
        ).fetchone()
        assert row is not None
        asset_id = row[0]
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:aid, 0.0, 5.0, 'shared term', CAST(:meta AS jsonb), 0.0, '', 'phash')"
            ),
            {"aid": asset_id, "meta": '{"moondream": {"description": "shared term", "tags": [], "ocr_text": null}}'},
        )
        session.commit()
    finally:
        session.close()

    results = search_repo.search_assets(
        query_string="shared term",
        library_slugs=[slug],
        asset_types=["video"],
        limit=50,
    )
    assert len(results) == 1
    assert results[0].asset.type == AssetType.video
    assert results[0].asset.rel_path == "vid.mp4"


def test_search_assets_library_slugs_and_asset_types_combined(engine, _session_factory):
    """search_assets with library_slugs + asset_types returns correct subset."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    session = _session_factory()
    try:
        for s in ["lib-x", "lib-y"]:
            session.add(
                Library(
                    slug=s,
                    name=f"Lib {s}",
                    absolute_path=f"/tmp/{s}",
                    is_active=True,
                    sampling_limit=100,
                )
            )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset("lib-x", "x-img.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset("lib-x", "x-vid.mp4", AssetType.video, 1001.0, 1024)
    asset_repo.upsert_asset("lib-y", "y-img.jpg", AssetType.image, 1002.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = 'lib-x' AND rel_path = 'x-img.jpg'"
            ),
            {"va": '{"description": "match term", "tags": [], "ocr_text": ""}'},
        )
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = 'lib-x' AND rel_path = 'x-vid.mp4'"
            ),
            {"va": '{"description": "match term", "tags": [], "ocr_text": ""}'},
        )
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) "
                "WHERE library_id = 'lib-y' AND rel_path = 'y-img.jpg'"
            ),
            {"va": '{"description": "match term", "tags": [], "ocr_text": ""}'},
        )
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = 'lib-x' AND rel_path = 'x-vid.mp4'"),
        ).fetchone()
        assert row is not None
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:aid, 0.0, 5.0, 'match term', CAST(:meta AS jsonb), 0.0, '', 'phash')"
            ),
            {"aid": row[0], "meta": '{"moondream": {"description": "match term", "tags": [], "ocr_text": null}}'},
        )
        session.commit()
    finally:
        session.close()

    results = search_repo.search_assets(
        query_string="match term",
        library_slugs=["lib-x"],
        asset_types=["image"],
        limit=50,
    )
    assert len(results) == 1
    assert results[0].asset.library_id == "lib-x" and results[0].asset.rel_path == "x-img.jpg"


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
        query_string="blue shirt", library_slugs=[slug], limit=50
    )
    assert len(results) == 2
    first_item, second_item = results[0], results[1]
    assert first_item.final_rank >= second_item.final_rank
    assert first_item.asset.rel_path == "strong.jpg"
    assert second_item.asset.rel_path == "weak.jpg"


def test_search_assets_video_scenes_density_ranking(engine, _session_factory):
    """search_assets returns video assets from video_scenes with best_scene_ts and match_ratio."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "video-search-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Video Search Lib",
                absolute_path="/tmp/video-search-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "clip.mp4", AssetType.video, 1000.0, 1024)
    session = _session_factory()
    try:
        row = session.execute(
            text("SELECT id FROM asset WHERE library_id = :lib AND rel_path = 'clip.mp4'"),
            {"lib": slug},
        ).fetchone()
        assert row is not None
        asset_id = row[0]
        # Two scenes: one matching "sunset beach", one not
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:aid, 0.0, 5.0, 'sunset', CAST(:meta1 AS jsonb), 0.0, '', 'phash')"
            ),
            {
                "aid": asset_id,
                "meta1": '{"moondream": {"description": "sunset beach", "tags": [], "ocr_text": null}}',
            },
        )
        session.execute(
            text(
                "INSERT INTO video_scenes (asset_id, start_ts, end_ts, description, metadata, sharpness_score, rep_frame_path, keep_reason) "
                "VALUES (:aid, 5.0, 10.0, 'indoor', CAST(:meta2 AS jsonb), 0.0, '', 'phash')"
            ),
            {
                "aid": asset_id,
                "meta2": '{"moondream": {"description": "person indoors", "tags": [], "ocr_text": null}}',
            },
        )
        session.commit()
    finally:
        session.close()

    results = search_repo.search_assets(query_string="sunset beach", library_slugs=[slug], limit=50)
    assert len(results) == 1
    item = results[0]
    assert item.asset.type == AssetType.video
    assert item.asset.rel_path == "clip.mp4"
    assert item.best_scene_ts is not None
    assert 0 < item.match_ratio <= 1.0
    assert item.final_rank > 0


def test_search_assets_tag_only_returns_assets_with_tag(engine, _session_factory):
    """search_assets with only tag returns assets that have that tag (image or video scene)."""
    lib_repo, asset_repo, search_repo = _create_tables_and_seed(engine, _session_factory)
    slug = "tag-lib"
    session = _session_factory()
    try:
        session.add(
            Library(
                slug=slug,
                name="Tag Lib",
                absolute_path="/tmp/tag-lib",
                is_active=True,
                sampling_limit=100,
            )
        )
        session.commit()
    finally:
        session.close()

    asset_repo.upsert_asset(slug, "beach.jpg", AssetType.image, 1000.0, 1024)
    asset_repo.upsert_asset(slug, "mountain.jpg", AssetType.image, 1001.0, 1024)
    session = _session_factory()
    try:
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) WHERE library_id = :lib AND rel_path = 'beach.jpg'"
            ),
            {"va": '{"description": "beach", "tags": ["beach", "sun"], "ocr_text": ""}', "lib": slug},
        )
        session.execute(
            text(
                "UPDATE asset SET visual_analysis = CAST(:va AS jsonb) WHERE library_id = :lib AND rel_path = 'mountain.jpg'"
            ),
            {"va": '{"description": "mountain", "tags": ["mountain"], "ocr_text": ""}', "lib": slug},
        )
        session.commit()
    finally:
        session.close()

    results_beach = search_repo.search_assets(tag="beach", limit=50)
    assert len(results_beach) == 1
    assert results_beach[0].asset.rel_path == "beach.jpg"

    results_mountain = search_repo.search_assets(tag="mountain", limit=50)
    assert len(results_mountain) == 1
    assert results_mountain[0].asset.rel_path == "mountain.jpg"

    results_none = search_repo.search_assets(tag="nonexistent", limit=50)
    assert len(results_none) == 0
