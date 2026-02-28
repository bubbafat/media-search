"""Mission Control API: dashboard and dependencies."""

import errno
import os
from functools import lru_cache
from pathlib import Path
from typing import Callable, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import get_config
from src.core.io_utils import file_non_empty
from src.core.path_resolver import resolve_path
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.search_repo import SearchRepository
from src.repository.project_repo import ProjectRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.ui_repo import UIRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.video.clip_extractor import extract_clip


@lru_cache(maxsize=1)
def _get_session_factory() -> Callable[[], Session]:
    from sqlalchemy import create_engine

    cfg = get_config()
    engine = create_engine(cfg.database_url, pool_pre_ping=True)
    return sessionmaker(engine, autocommit=False, autoflush=False, expire_on_commit=False)


@lru_cache(maxsize=1)
def _get_system_metadata_repo() -> SystemMetadataRepository:
    return SystemMetadataRepository(_get_session_factory())


@lru_cache(maxsize=1)
def _get_ui_repo() -> UIRepository:
    smr = _get_system_metadata_repo()
    return UIRepository(_get_session_factory(), smr.get_schema_version)


app = FastAPI(title="MediaSearch Mission Control")

templates_dir = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

# Built Tailwind + DaisyUI CSS (npm run build:css from project root)
_static_dir = Path(__file__).resolve().parent.parent.parent / "static"
if _static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

_data_dir = Path(get_config().data_dir).resolve()
if _data_dir == Path("/") or _data_dir == Path.cwd().resolve():
    raise RuntimeError(
        "Unsafe data_dir configuration. Cannot mount root or current working directory."
    )

app.mount(
    "/media",
    StaticFiles(directory=str(_data_dir), check_dir=False),
    name="media",
)


@lru_cache(maxsize=1)
def _get_search_repo() -> SearchRepository:
    return SearchRepository(_get_session_factory())


@lru_cache(maxsize=1)
def _get_asset_repo() -> AssetRepository:
    return AssetRepository(_get_session_factory())


@lru_cache(maxsize=1)
def _get_video_scene_repo() -> VideoSceneRepository:
    return VideoSceneRepository(_get_session_factory())


@lru_cache(maxsize=1)
def _get_library_repo() -> LibraryRepository:
    return LibraryRepository(_get_session_factory())


@lru_cache(maxsize=1)
def _get_project_repo() -> ProjectRepository:
    return ProjectRepository(_get_session_factory())

NO_THUMB_STATUSES = frozenset({"pending", "processing", "failed", "poisoned"})


class SearchResultOut(BaseModel):
    asset_id: int
    type: Literal["image", "video"]
    thumbnail_url: str | None = None  # null when pending, processing, failed, or poisoned
    preview_url: str | None = None
    video_preview_url: str | None = None
    status: str | None = None
    error_message: str | None = None
    final_rank: float
    match_ratio: float  # percentage (0.0 to 100.0)
    best_scene_ts: str | None = None  # formatted MM:SS
    best_scene_ts_seconds: float | None = None  # raw seconds for deep-linking
    library_slug: str
    library_name: str
    filename: str


class AssetDetailOut(BaseModel):
    description: str | None = None
    tags: list[str] = []
    ocr_text: str | None = None
    library_slug: str = ""
    filename: str = ""


class ProjectOut(BaseModel):
    id: int
    name: str
    created_at: str
    export_path: str | None = None


class ProjectCreateIn(BaseModel):
    name: str
    export_path: str | None = None


class ProjectAssetIn(BaseModel):
    asset_id: int


def _format_mmss(seconds: float) -> str:
    total = max(int(seconds), 0)
    mm = total // 60
    ss = total % 60
    return f"{mm:02d}:{ss:02d}"


@app.get("/api/search", response_model=list[SearchResultOut])
def api_search(
    q: str | None = Query(default=None, description="Semantic (vibe) full-text search query"),
    ocr: str | None = Query(default=None, description="OCR-only full-text search query"),
    library: list[str] | None = Query(default=None, description="Filter to these library slugs (repeatable)"),
    type_: list[Literal["image", "video"]] | None = Query(default=None, alias="type", description="Filter to asset types"),
    tag: str | None = Query(default=None, description="Filter by tag (tag-only search when q/ocr omitted)"),
    limit: int = Query(default=50, ge=1, le=500),
    search_repo: SearchRepository = Depends(_get_search_repo),
    ui_repo: UIRepository = Depends(_get_ui_repo),
) -> JSONResponse:
    results = search_repo.search_assets(
        query_string=q,
        ocr_query=ocr,
        library_slugs=library,
        asset_types=type_,
        tag=tag,
        limit=limit,
    )

    library_ids = list({r.asset.library_id for r in results})
    names = ui_repo.get_library_names(library_ids)

    out: list[SearchResultOut] = []
    for r in results:
        asset = r.asset
        if asset.id is None:
            continue
        shard = asset.id % 1000
        thumb = (
            None
            if asset.status.value in NO_THUMB_STATUSES
            else f"/media/{asset.library_id}/thumbnails/{shard}/{asset.id}.jpg"
        )
        preview = (
            f"/media/{r.best_scene_rep_frame_path.lstrip('/')}"
            if asset.type.value == "video" and r.best_scene_rep_frame_path
            else None
        )
        video_preview = (
            f"/media/{asset.video_preview_path.lstrip('/')}"
            if asset.video_preview_path is not None
            else None
        )
        best_ts_seconds = r.best_scene_ts if r.best_scene_ts is not None else None
        best_ts = _format_mmss(best_ts_seconds) if best_ts_seconds is not None else None
        lib_slug = asset.library_id
        lib_name = names.get(lib_slug, lib_slug)
        filename = os.path.basename(asset.rel_path)
        out.append(
            SearchResultOut(
                asset_id=asset.id,
                type=asset.type.value,
                thumbnail_url=thumb,
                preview_url=preview,
                video_preview_url=video_preview,
                status=asset.status.value,
                error_message=asset.error_message,
                final_rank=r.final_rank,
                match_ratio=round(r.match_ratio * 100.0, 1),
                best_scene_ts=best_ts,
                best_scene_ts_seconds=best_ts_seconds,
                library_slug=lib_slug,
                library_name=lib_name,
                filename=filename,
            )
        )
    is_incomplete = ui_repo.any_libraries_analyzing(library)
    return JSONResponse(
        content=[item.model_dump(mode="json") for item in out],
        headers={"X-Search-Incomplete": "true" if is_incomplete else "false"},
    )


class ProjectExportOut(BaseModel):
    export_path: str


class LibraryOut(BaseModel):
    slug: str
    name: str
    is_analyzing: bool


class LibraryAssetOut(BaseModel):
    """Same shape as SearchResultOut for reuse in the results grid."""

    asset_id: int
    type: Literal["image", "video"]
    thumbnail_url: str | None = None  # null when pending, processing, failed, or poisoned
    preview_url: str | None = None
    video_preview_url: str | None = None
    status: str | None = None
    error_message: str | None = None
    match_ratio: float = 100.0  # neutral value for library browse
    best_scene_ts: str | None = None
    best_scene_ts_seconds: float | None = None
    library_slug: str
    library_name: str
    filename: str


class LibraryAssetsOut(BaseModel):
    items: list[LibraryAssetOut]
    has_more: bool


@app.get("/api/library-assets", response_model=LibraryAssetsOut)
def api_library_assets(
    library: str = Query(..., description="Library slug (required)"),
    sort: Literal["name", "date", "size", "added", "type"] = Query(
        default="date", description="Sort by field"
    ),
    order: Literal["asc", "desc"] = Query(default="desc", description="Sort direction"),
    type_: list[Literal["image", "video"]] | None = Query(
        default=None, alias="type", description="Filter to asset types"
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    asset_repo: AssetRepository = Depends(_get_asset_repo),
    library_repo: LibraryRepository = Depends(_get_library_repo),
    video_scene_repo: VideoSceneRepository = Depends(_get_video_scene_repo),
) -> LibraryAssetsOut:
    """Return paginated assets for a library. Used by Library Browser with infinite scroll."""
    assets = asset_repo.list_assets_for_library(
        library_slug=library,
        limit=limit + 1,
        offset=offset,
        sort_by=sort,
        order=order,
        asset_types=type_,
    )
    has_more = len(assets) > limit
    page = list(assets[:limit])
    libs = library_repo.list_libraries(include_deleted=False)
    lib_map = {l.slug: l.name for l in libs}
    lib_name = lib_map.get(library, library)

    video_asset_ids = [a.id for a in page if a.id is not None and a.type.value == "video"]
    first_rep_paths = video_scene_repo.get_first_scene_rep_frame_paths(video_asset_ids)

    out: list[LibraryAssetOut] = []
    for asset in page:
        if asset.id is None:
            continue
        shard = asset.id % 1000
        thumb = (
            None
            if asset.status.value in NO_THUMB_STATUSES
            else f"/media/{asset.library_id}/thumbnails/{shard}/{asset.id}.jpg"
        )
        preview = (
            f"/media/{first_rep_paths[asset.id].lstrip('/')}"
            if asset.type.value == "video" and asset.id in first_rep_paths
            else None
        )
        video_preview = (
            f"/media/{asset.video_preview_path.lstrip('/')}"
            if asset.video_preview_path is not None
            else None
        )
        filename = os.path.basename(asset.rel_path)
        out.append(
            LibraryAssetOut(
                asset_id=asset.id,
                type=asset.type.value,
                thumbnail_url=thumb,
                preview_url=preview,
                video_preview_url=video_preview,
                status=asset.status.value,
                error_message=asset.error_message,
                match_ratio=100.0,
                best_scene_ts=None,
                best_scene_ts_seconds=None,
                library_slug=asset.library_id,
                library_name=lib_name,
                filename=filename,
            )
        )
    return LibraryAssetsOut(items=out, has_more=has_more)


@app.get("/api/libraries", response_model=list[LibraryOut])
def api_libraries(
    ui_repo: UIRepository = Depends(_get_ui_repo),
) -> list[LibraryOut]:
    """Return non-deleted libraries for filter dropdown, with is_analyzing status."""
    libs = ui_repo.list_libraries_with_status()
    return [LibraryOut(slug=lib.slug, name=lib.name, is_analyzing=lib.is_analyzing) for lib in libs]


@app.get("/api/projects", response_model=list[ProjectOut])
def api_projects(
    project_repo: ProjectRepository = Depends(_get_project_repo),
) -> list[ProjectOut]:
    """List all projects (Project Bins) for export workflows."""
    projects = project_repo.list_projects()
    return [
        ProjectOut(
            id=p.id or 0,
            name=p.name,
            created_at=p.created_at.isoformat(),
            export_path=p.export_path,
        )
        for p in projects
    ]


@app.post("/api/projects", response_model=ProjectOut, status_code=201)
def api_create_project(
    body: ProjectCreateIn,
    project_repo: ProjectRepository = Depends(_get_project_repo),
) -> ProjectOut:
    """Create a new project (Project Bin)."""
    if not body.name or not body.name.strip():
        raise HTTPException(status_code=400, detail="Project name must be non-empty")
    project = project_repo.create_project(body.name.strip(), body.export_path)
    return ProjectOut(
        id=project.id or 0,
        name=project.name,
        created_at=project.created_at.isoformat(),
        export_path=project.export_path,
    )


@app.post("/api/projects/{project_id}/assets", status_code=204)
def api_add_asset_to_project(
    project_id: int,
    body: ProjectAssetIn,
    project_repo: ProjectRepository = Depends(_get_project_repo),
    asset_repo: AssetRepository = Depends(_get_asset_repo),
) -> Response:
    """Add an asset to a project bin."""
    project = project_repo.get_project(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    asset = asset_repo.get_asset_by_id(body.asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    project_repo.add_asset_to_project(project_id, body.asset_id)
    return Response(status_code=204)


@app.post("/api/projects/{project_id}/export", response_model=ProjectExportOut)
def api_export_project(
    project_id: int,
    project_repo: ProjectRepository = Depends(_get_project_repo),
) -> ProjectExportOut:
    """
    Export all assets in the project into a timestamped folder under EXPORT_ROOT_PATH using hard links.
    """
    cfg = get_config()
    export_root = cfg.export_root_path
    if not export_root:
        raise HTTPException(
            status_code=400,
            detail=(
                "EXPORT_ROOT_PATH is not configured; set it to a writable directory on the "
                "same volume as your media."
            ),
        )

    project = project_repo.get_project(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    from datetime import datetime, timezone

    try:
        os.makedirs(export_root, exist_ok=True)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create export root directory: {exc}",
        ) from exc

    safe_name = "".join(ch.lower() if ch.isalnum() else "-" for ch in project.name).strip("-")
    if not safe_name:
        safe_name = f"project-{project.id or project_id}"

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_dir = Path(export_root)
    export_dir = base_dir / f"{safe_name}_{timestamp}"

    suffix = 1
    while export_dir.exists():
        suffix += 1
        export_dir = base_dir / f"{safe_name}_{timestamp}_{suffix}"

    try:
        export_dir.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create export directory: {exc}",
        ) from exc

    src_paths = project_repo.get_project_assets(project_id)

    used_names: set[str] = set()
    for src in src_paths:
        src_path = Path(src)
        filename = src_path.name
        if not filename:
            continue

        dest_name = filename
        counter = 2
        stem = src_path.stem
        suffix_ext = src_path.suffix
        while dest_name in used_names:
            if suffix_ext:
                dest_name = f"{stem}_{counter}{suffix_ext}"
            else:
                dest_name = f"{stem}_{counter}"
            counter += 1
        used_names.add(dest_name)

        dest_path = export_dir / dest_name
        try:
            os.link(src_path, dest_path)
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Export root must be on the same physical volume as the source media "
                        "for hard links (cross-device link error)."
                    ),
                ) from exc
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create hard link for {src_path}: {exc}",
            ) from exc

    return ProjectExportOut(export_path=str(export_dir))

@app.get("/api/asset/{asset_id}/clip")
async def api_asset_clip(
    asset_id: int,
    ts: float = Query(..., description="Timestamp in seconds"),
    asset_repo: AssetRepository = Depends(_get_asset_repo),
    library_repo: LibraryRepository = Depends(_get_library_repo),
) -> RedirectResponse:
    """Lazy-load a 10-second web-safe MP4 clip for video search-hit verification."""
    asset = asset_repo.get_asset_by_id(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    if asset.type.value != "video":
        raise HTTPException(status_code=400, detail="Clip endpoint is for video assets only")
    absolute_path = library_repo.get_absolute_path(asset.library_id)
    if absolute_path is None:
        raise HTTPException(status_code=404, detail="Library not found")
    try:
        source_path = resolve_path(asset.library_id, asset.rel_path)
    except (ValueError, FileNotFoundError):
        raise HTTPException(status_code=404, detail="Source file not found")

    data_dir = Path(get_config().data_dir)
    dest_path = data_dir / "video_clips" / asset.library_id / str(asset_id) / f"clip_{int(ts * 1000)}.mp4"

    if not file_non_empty(dest_path):
        ok = await extract_clip(source_path, dest_path, ts)
        if not ok:
            raise HTTPException(status_code=500, detail="Clip extraction failed")

    return RedirectResponse(
        url=f"/media/video_clips/{asset.library_id}/{asset_id}/clip_{int(ts * 1000)}.mp4",
        status_code=302,
    )


@app.get("/api/asset/{asset_id}", response_model=AssetDetailOut)
def api_asset_detail(
    asset_id: int,
    best_scene_ts: float | None = Query(default=None, description="For videos: scene start_ts (seconds) for scene-level metadata"),
    asset_repo: AssetRepository = Depends(_get_asset_repo),
    video_scene_repo: VideoSceneRepository = Depends(_get_video_scene_repo),
) -> AssetDetailOut:
    """Return description, tags, and ocr_text for the asset (from visual_analysis or video scene metadata)."""
    asset = asset_repo.get_asset_by_id(asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    lib_slug = asset.library_id
    filename = os.path.basename(asset.rel_path)

    # Video with best_scene_ts: use scene metadata
    if asset.type.value == "video" and best_scene_ts is not None:
        scene_meta = video_scene_repo.get_scene_metadata_at_timestamp(asset_id, best_scene_ts)
        if scene_meta is not None:
            return AssetDetailOut(
                description=scene_meta.get("description"),
                tags=scene_meta.get("tags") or [],
                ocr_text=scene_meta.get("ocr_text"),
                library_slug=lib_slug,
                filename=filename,
            )

    # Image or video without scene ts: use asset visual_analysis
    va = asset.visual_analysis or {}
    return AssetDetailOut(
        description=va.get("description"),
        tags=va.get("tags") or [],
        ocr_text=va.get("ocr_text"),
        library_slug=lib_slug,
        filename=filename,
    )


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    ui_repo: UIRepository = Depends(_get_ui_repo),
    tag: str | None = Query(default=None, description="Initial tag filter (runs tag search on load)"),
    library: list[str] | None = Query(default=None, description="Initial library filter (repeatable)"),
    type_: list[str] | None = Query(default=None, alias="type", description="Initial media type filter"),
) -> HTMLResponse:
    """Render Mission Control dashboard: system version, DB status, fleet, stats."""
    health = ui_repo.get_system_health()
    fleet = ui_repo.get_worker_fleet()
    stats = ui_repo.get_library_stats()
    initial_libraries = library or []
    initial_types = type_ or []
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "schema_version": health.schema_version,
            "db_status": health.db_status,
            "fleet": fleet,
            "total_assets": stats.total_assets,
            "pending_assets": stats.pending_assets,
            "initial_tag": tag or "",
            "initial_libraries": initial_libraries,
            "initial_types": initial_types,
        },
    )


@app.get("/library", response_class=HTMLResponse)
def library_page(
    request: Request,
    ui_repo: UIRepository = Depends(_get_ui_repo),
    library: str | None = Query(default=None, description="Initial library slug"),
    sort: str = Query(default="date", description="Sort field"),
    order: str = Query(default="desc", description="Sort order"),
    type_: list[str] | None = Query(default=None, alias="type", description="Initial media type filter"),
) -> HTMLResponse:
    """Render Library browser: select library, browse assets with infinite scroll."""
    health = ui_repo.get_system_health()
    fleet = ui_repo.get_worker_fleet()
    return templates.TemplateResponse(
        request,
        "library.html",
        {
            "schema_version": health.schema_version,
            "db_status": health.db_status,
            "fleet": fleet,
            "initial_library": library or "",
            "initial_sort": sort,
            "initial_order": order,
            "initial_types": type_ or [],
        },
    )


@app.get("/library/{library_slug}", response_class=HTMLResponse)
def library_page_slug(
    request: Request,
    library_slug: str,
    ui_repo: UIRepository = Depends(_get_ui_repo),
    sort: str = Query(default="date", description="Sort field"),
    order: str = Query(default="desc", description="Sort order"),
    type_: list[str] | None = Query(default=None, alias="type", description="Initial media type filter"),
) -> HTMLResponse:
    """Render Library browser with library pre-selected."""
    health = ui_repo.get_system_health()
    fleet = ui_repo.get_worker_fleet()
    return templates.TemplateResponse(
        request,
        "library.html",
        {
            "schema_version": health.schema_version,
            "db_status": health.db_status,
            "fleet": fleet,
            "initial_library": library_slug,
            "initial_sort": sort,
            "initial_order": order,
            "initial_types": type_ or [],
        },
    )


@app.get("/dashboard/tag/{tag:path}", response_class=HTMLResponse)
def dashboard_tag(
    request: Request,
    tag: str,
    ui_repo: UIRepository = Depends(_get_ui_repo),
    library: list[str] | None = Query(default=None, description="Initial library filter (repeatable)"),
    type_: list[str] | None = Query(default=None, alias="type", description="Initial media type filter"),
) -> HTMLResponse:
    """Render dashboard with tag filter applied (same page as /dashboard?tag=...)."""
    health = ui_repo.get_system_health()
    fleet = ui_repo.get_worker_fleet()
    stats = ui_repo.get_library_stats()
    initial_libraries = library or []
    initial_types = type_ or []
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "schema_version": health.schema_version,
            "db_status": health.db_status,
            "fleet": fleet,
            "total_assets": stats.total_assets,
            "pending_assets": stats.pending_assets,
            "initial_tag": tag,
            "initial_libraries": initial_libraries,
            "initial_types": initial_types,
        },
    )
