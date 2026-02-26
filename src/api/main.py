"""Mission Control API: dashboard and dependencies."""

from functools import lru_cache
from pathlib import Path
from typing import Callable, Literal

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import get_config
from src.repository.search_repo import SearchRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.ui_repo import UIRepository


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

app.mount(
    "/media",
    StaticFiles(directory=str(Path(get_config().data_dir)), check_dir=False),
    name="media",
)


@lru_cache(maxsize=1)
def _get_search_repo() -> SearchRepository:
    return SearchRepository(_get_session_factory())


class SearchResultOut(BaseModel):
    asset_id: int
    type: Literal["image", "video"]
    thumbnail_url: str
    preview_url: str | None = None
    final_rank: float
    match_ratio: float  # percentage (0.0 to 100.0)
    best_scene_ts: str | None = None  # formatted MM:SS
    best_scene_ts_seconds: float | None = None  # raw seconds for deep-linking


def _format_mmss(seconds: float) -> str:
    total = max(int(seconds), 0)
    mm = total // 60
    ss = total % 60
    return f"{mm:02d}:{ss:02d}"


@app.get("/api/search", response_model=list[SearchResultOut])
def api_search(
    q: str | None = Query(default=None, description="Semantic (vibe) full-text search query"),
    ocr: str | None = Query(default=None, description="OCR-only full-text search query"),
    library_slug: str | None = Query(default=None, description="Optional library slug filter"),
    limit: int = Query(default=50, ge=1, le=500),
    search_repo: SearchRepository = Depends(_get_search_repo),
) -> list[SearchResultOut]:
    results = search_repo.search_assets(
        query_string=q,
        ocr_query=ocr,
        library_slug=library_slug,
        limit=limit,
    )

    out: list[SearchResultOut] = []
    for r in results:
        asset = r.asset
        if asset.id is None:
            continue
        shard = asset.id % 1000
        thumb = f"/media/{asset.library_id}/thumbnails/{shard}/{asset.id}.jpg"
        preview = (
            f"/media/{asset.preview_path.lstrip('/')}"
            if asset.preview_path is not None
            else None
        )
        best_ts_seconds = r.best_scene_ts if r.best_scene_ts is not None else None
        best_ts = _format_mmss(best_ts_seconds) if best_ts_seconds is not None else None
        out.append(
            SearchResultOut(
                asset_id=asset.id,
                type=asset.type.value,
                thumbnail_url=thumb,
                preview_url=preview,
                final_rank=r.final_rank,
                match_ratio=round(r.match_ratio * 100.0, 1),
                best_scene_ts=best_ts,
                best_scene_ts_seconds=best_ts_seconds,
            )
        )
    return out


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    ui_repo: UIRepository = Depends(_get_ui_repo),
) -> HTMLResponse:
    """Render Mission Control dashboard: system version, DB status, fleet, stats."""
    health = ui_repo.get_system_health()
    fleet = ui_repo.get_worker_fleet()
    stats = ui_repo.get_library_stats()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "schema_version": health.schema_version,
            "db_status": health.db_status,
            "fleet": fleet,
            "total_assets": stats.total_assets,
            "pending_assets": stats.pending_assets,
        },
    )
