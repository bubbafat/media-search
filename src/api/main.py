"""Mission Control API: dashboard and dependencies."""

from functools import lru_cache
from pathlib import Path
from typing import Callable

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import get_config
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
