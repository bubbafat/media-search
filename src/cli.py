"""Typer Admin CLI: library management and one-shot scan."""

import json
import logging
import os
import socket
import sys
import uuid
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

from src.core.config import get_config
from src.core.storage import rawpy_available
from src.models.entities import AssetStatus, AssetType, ScanStatus, WorkerState
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.search_repo import SearchRepository
from src.repository.system_metadata_repo import (
    ALLOW_MOCK_DEFAULT_ENV,
    SystemMetadataRepository,
)
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.ai_worker import AIWorker
from src.workers.proxy_worker import ImageProxyWorker
from src.workers.video_proxy_worker import VideoProxyWorker
from src.workers.video_worker import VideoWorker
from src.workers.scanner import ScannerWorker

app = typer.Typer(no_args_is_help=True)
library_app = typer.Typer(help="Add, remove, restore, and list libraries.")
app.add_typer(library_app, name="library")
trash_app = typer.Typer(help="Manage soft-deleted libraries.")
app.add_typer(trash_app, name="trash")
asset_app = typer.Typer(help="Manage individual assets.")
app.add_typer(asset_app, name="asset")
ai_app = typer.Typer(help="Manage AI models and workers.")
app.add_typer(ai_app, name="ai")
ai_default_app = typer.Typer(help="Get or set the system default AI model.")
ai_app.add_typer(ai_default_app, name="default")
repair_app = typer.Typer(help="Repair database consistency (e.g. orphaned assets).")
app.add_typer(repair_app, name="repair")


def _get_session_factory():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    cfg = get_config()
    engine = create_engine(cfg.database_url, pool_pre_ping=True)
    return sessionmaker(engine, autocommit=False, autoflush=False, expire_on_commit=False)


@library_app.command("add")
def library_add(
    name: str = typer.Argument(..., help="Display name for the library"),
    path: str = typer.Argument(..., help="Absolute or relative path to the library root"),
) -> None:
    """Add a new library. Slug is generated from the name."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    absolute_path = str(Path(path).resolve())
    try:
        slug = lib_repo.add(name, absolute_path)
        typer.echo(f"Added library '{name}' with slug '{slug}'.")
    except ValueError as e:
        typer.secho(str(e), fg=typer.colors.RED)
        raise typer.Exit(1)


@library_app.command("remove")
def library_remove(
    library_slug: str = typer.Argument(..., help="Library slug to soft-delete"),
) -> None:
    """Soft-delete a library (moves to trash)."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    lib_repo.soft_delete(library_slug)
    typer.echo("Library moved to trash.")


@library_app.command("restore")
def library_restore(
    library_slug: str = typer.Argument(..., help="Library slug to restore from trash"),
) -> None:
    """Restore a soft-deleted library."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    lib_repo.restore(library_slug)
    typer.echo("Library restored.")


@trash_app.command("list")
def trash_list() -> None:
    """List libraries in the trash (Slug | Name | Deleted At)."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    libraries = lib_repo.list_trashed()
    if not libraries:
        typer.echo("No trashed libraries.")
        return
    table = Table(title=None)
    table.add_column("Slug")
    table.add_column("Name")
    table.add_column("Deleted At")
    for lib in libraries:
        deleted = str(lib.deleted_at) if lib.deleted_at else ""
        table.add_row(lib.slug, lib.name, deleted)
    console = Console()
    console.print(table)


@trash_app.command("empty")
def trash_empty(
    library_slug: str = typer.Argument(..., help="Library slug to permanently delete"),
    force: bool = typer.Option(False, "--force", help="Skip confirmation"),
) -> None:
    """Permanently delete a trashed library and all its assets. Cannot be undone."""
    if not force:
        typer.confirm(
            "Are you sure you want to permanently delete this library and ALL its assets? This cannot be undone.",
            abort=True,
        )
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    try:
        lib_repo.hard_delete(library_slug)
        typer.secho(f"Permanently deleted library '{library_slug}'.", fg=typer.colors.GREEN)
    except ValueError as e:
        typer.secho(str(e), fg=typer.colors.RED)
        raise typer.Exit(1)


@trash_app.command("empty-all")
def trash_empty_all(
    force: bool = typer.Option(False, "--force", help="Skip confirmation"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print progress (Emptying 1/N: slug)."),
) -> None:
    """Permanently delete all trashed libraries and their assets. Cannot be undone."""
    if not force:
        typer.confirm("Permanently delete ALL trashed libraries?", abort=True)
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    trashed = lib_repo.list_trashed()
    n = len(trashed)
    for i, lib in enumerate(trashed, 1):
        if verbose:
            typer.echo(f"Emptying {i}/{n}: {lib.slug}")
        lib_repo.hard_delete(lib.slug)
    typer.secho(f"Permanently deleted {n} library(ies).", fg=typer.colors.GREEN)


@repair_app.command("orphan-assets")
def repair_orphan_assets(
    dry_run: bool = typer.Option(False, "--dry-run", help="Only report orphaned assets; do not delete"),
    force: bool = typer.Option(False, "--force", help="Skip confirmation when deleting"),
) -> None:
    """Find and remove assets whose library no longer exists (e.g. after a failed or partial trash empty)."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    slugs = lib_repo.get_orphaned_library_slugs()
    if not slugs:
        typer.echo("No orphaned assets found.")
        return
    if dry_run:
        typer.secho(f"Found {len(slugs)} orphaned library slug(s) with assets:", fg=typer.colors.YELLOW)
        for slug in slugs:
            count = lib_repo.get_orphaned_asset_count_for_library(slug)
            typer.echo(f"  {slug}: {count} asset(s)")
        typer.echo("Run without --dry-run to delete these assets (and their video_scenes, video_active_state, videoframe rows).")
        return
    if not force:
        total = sum(lib_repo.get_orphaned_asset_count_for_library(s) for s in slugs)
        typer.confirm(
            f"Permanently delete {total} orphaned asset(s) across {len(slugs)} missing library(ies)? This cannot be undone.",
            abort=True,
        )
    total_deleted = 0
    for slug in slugs:
        n = lib_repo.delete_orphaned_assets_for_library(slug)
        total_deleted += n
        typer.echo(f"Removed {n} asset(s) for missing library '{slug}'.")
    typer.secho(f"Repair complete: removed {total_deleted} orphaned asset(s).", fg=typer.colors.GREEN)


@library_app.command("list")
def library_list(
    include_deleted: bool = typer.Option(
        False,
        "--include-deleted",
        help="Include soft-deleted libraries",
    ),
) -> None:
    """List libraries in a table (slug, name, path, deleted_at)."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    libraries = lib_repo.list_libraries(include_deleted=include_deleted)
    if not libraries:
        typer.echo("No libraries.")
        return
    # Format table: slug | name | absolute_path | deleted_at
    slug_w = max(4, max(len(l.slug) for l in libraries))
    name_w = max(4, max(len(l.name) for l in libraries))
    path_w = max(4, min(60, max(len(l.absolute_path) for l in libraries)))
    head_slug = "slug".ljust(slug_w)
    head_name = "name".ljust(name_w)
    head_path = "path".ljust(path_w)
    head_del = "deleted_at"
    typer.echo(f"{head_slug}  {head_name}  {head_path}  {head_del}")
    typer.echo("-" * (slug_w + name_w + path_w + len(head_del) + 6))
    for lib in libraries:
        deleted = str(lib.deleted_at) if lib.deleted_at else ""
        path_short = (lib.absolute_path[: path_w - 3] + "...") if len(lib.absolute_path) > path_w else lib.absolute_path
        typer.echo(f"{lib.slug.ljust(slug_w)}  {lib.name.ljust(name_w)}  {path_short.ljust(path_w)}  {deleted}")


@library_app.command("reindex-videos")
def library_reindex_videos(
    library_slug: str = typer.Argument(..., help="Library slug"),
) -> None:
    """Clear video index and set all video assets to pending so the Video worker will re-process them."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)
    scene_repo = VideoSceneRepository(session_factory)

    lib = lib_repo.get_by_slug(library_slug)
    if lib is None:
        typer.echo(f"Library not found or deleted: '{library_slug}'.", err=True)
        raise typer.Exit(1)

    asset_ids = asset_repo.get_video_asset_ids_by_library(library_slug)
    for aid in asset_ids:
        scene_repo.clear_index_for_asset(aid)
        asset_repo.set_preview_path(aid, None)
        asset_repo.update_asset_status(aid, AssetStatus.pending)
    typer.echo(f"{len(asset_ids)} video asset(s) set to pending. Run 'ai video --library {library_slug}' to re-process.")


@asset_app.command("list")
def asset_list(
    library_slug: str = typer.Argument(..., help="Library slug to list assets for"),
    limit: int = typer.Option(50, "--limit", help="Maximum number of assets to show"),
    status: str | None = typer.Option(None, "--status", help="Filter by status: pending, processing, proxied, extracting, analyzing, analyzed_light, completed, failed, poisoned"),
) -> None:
    """List discovered assets for a library."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)

    lib = lib_repo.get_by_slug(library_slug)
    if lib is None:
        typer.echo(f"Library not found or deleted: '{library_slug}'.", err=True)
        raise typer.Exit(1)

    status_enum: AssetStatus | None = None
    if status is not None:
        try:
            status_enum = AssetStatus(status)
        except ValueError:
            typer.echo(f"Invalid status: '{status}'.", err=True)
            raise typer.Exit(1)

    assets = asset_repo.get_assets_by_library(lib.slug, limit=limit, status=status_enum)
    total = asset_repo.count_assets_by_library(lib.slug, status=status_enum)

    table = Table(title=None)
    table.add_column("ID", style="dim")
    table.add_column("Rel Path")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Size (KB)")
    for a in assets:
        id_str = str(a.id) if a.id is not None else ""
        size_kb = round(a.size / 1024) if a.size else 0
        table.add_row(id_str, a.rel_path, a.type.value, a.status.value, str(size_kb))
    console = Console()
    console.print(table)
    typer.echo(f"Showing {len(assets)} of {total} assets for library '{library_slug}'.")


@asset_app.command("show")
def asset_show(
    library_slug: str = typer.Argument(..., help="Library slug"),
    rel_path: str = typer.Argument(..., help="Relative path of the asset within the library"),
    metadata: bool = typer.Option(False, "--metadata", help="Dump full asset record as JSON (including visual_analysis)"),
) -> None:
    """Show one asset: minimal summary by default, or full metadata with --metadata."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)

    lib = lib_repo.get_by_slug(library_slug)
    if lib is None:
        typer.echo(f"Library not found or deleted: '{library_slug}'.", err=True)
        raise typer.Exit(1)

    asset = asset_repo.get_asset(library_slug, rel_path)
    if asset is None:
        typer.echo("Asset not found.", err=True)
        raise typer.Exit(1)

    if metadata:
        payload = {
            "id": asset.id,
            "library_id": asset.library_id,
            "rel_path": asset.rel_path,
            "type": asset.type.value,
            "mtime": asset.mtime,
            "size": asset.size,
            "status": asset.status.value,
            "tags_model_id": asset.tags_model_id,
            "analysis_model_id": asset.analysis_model_id,
            "worker_id": asset.worker_id,
            "lease_expires_at": asset.lease_expires_at.isoformat() if asset.lease_expires_at else None,
            "retry_count": asset.retry_count,
            "error_message": asset.error_message,
            "visual_analysis": asset.visual_analysis,
        }
        typer.echo(json.dumps(payload, indent=2))
    else:
        size_kb = round(asset.size / 1024) if asset.size else 0
        typer.echo(f"id: {asset.id}")
        typer.echo(f"library_id: {asset.library_id}")
        typer.echo(f"rel_path: {asset.rel_path}")
        typer.echo(f"type: {asset.type.value}")
        typer.echo(f"status: {asset.status.value}")
        typer.echo(f"size: {size_kb} KB")


@asset_app.command("scenes")
def asset_scenes(
    library_slug: str = typer.Argument(..., help="Library slug"),
    rel_path: str = typer.Argument(..., help="Relative path of the video asset within the library"),
    metadata: bool = typer.Option(False, "--metadata", help="Output full scene records as JSON including metadata (e.g. moondream description/tags)."),
) -> None:
    """List video scenes for a video asset: summary table by default, or full JSON with --metadata."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)
    scene_repo = VideoSceneRepository(session_factory)

    lib = lib_repo.get_by_slug(library_slug)
    if lib is None:
        typer.echo(f"Library not found or deleted: '{library_slug}'.", err=True)
        raise typer.Exit(1)

    asset = asset_repo.get_asset(library_slug, rel_path)
    if asset is None:
        typer.echo("Asset not found.", err=True)
        raise typer.Exit(1)

    if asset.type != AssetType.video:
        typer.echo("Scenes are only available for video assets.", err=True)
        raise typer.Exit(1)

    assert asset.id is not None
    scenes = scene_repo.list_scenes(asset.id)

    if metadata:
        payload = [
            {
                "id": s.id,
                "start_ts": s.start_ts,
                "end_ts": s.end_ts,
                "description": s.description,
                "metadata": s.metadata,
                "sharpness_score": s.sharpness_score,
                "rep_frame_path": s.rep_frame_path,
                "keep_reason": s.keep_reason,
            }
            for s in scenes
        ]
        typer.echo(json.dumps(payload, indent=2))
    else:
        if not scenes:
            typer.echo("No scenes indexed for this asset.")
            return
        table = Table(title="Video scenes")
        table.add_column("#", style="dim")
        table.add_column("Start (s)")
        table.add_column("End (s)")
        table.add_column("Keep reason")
        table.add_column("Description")
        for i, s in enumerate(scenes, start=1):
            desc = (s.description or "").replace("\n", " ").strip()
            if len(desc) > 60:
                desc = desc[:57] + "..."
            table.add_row(str(i), f"{s.start_ts:.2f}", f"{s.end_ts:.2f}", s.keep_reason, desc or "—")
        console = Console()
        console.print(table)


@asset_app.command("reindex")
def asset_reindex(
    library_slug: str = typer.Argument(..., help="Library slug"),
    rel_path: str = typer.Argument(..., help="Relative path of the video asset within the library"),
) -> None:
    """Clear video index and set asset to pending so the Video worker will re-process it."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)
    scene_repo = VideoSceneRepository(session_factory)

    lib = lib_repo.get_by_slug(library_slug)
    if lib is None:
        typer.echo(f"Library not found or deleted: '{library_slug}'.", err=True)
        raise typer.Exit(1)

    asset = asset_repo.get_asset(library_slug, rel_path)
    if asset is None:
        typer.echo("Asset not found.", err=True)
        raise typer.Exit(1)

    if asset.type != AssetType.video:
        typer.echo("Reindex is only available for video assets.", err=True)
        raise typer.Exit(1)

    assert asset.id is not None
    scene_repo.clear_index_for_asset(asset.id)
    asset_repo.set_preview_path(asset.id, None)
    asset_repo.update_asset_status(asset.id, AssetStatus.pending)
    typer.echo("Video index cleared and asset set to pending. Run 'ai video' (or 'ai video --library <slug>') to re-process.")


@app.command("search")
def search(
    query: str = typer.Argument(None, help="Search query (optional). If omitted, no results are returned. E.g. 'man in blue shirt'."),
    ocr: bool = typer.Option(False, "--ocr", help="Search only within extracted OCR text"),
    library: list[str] = typer.Option([], "--library", help="Filter to these library slugs (repeatable)"),
    type_filter: list[str] = typer.Option([], "--type", help="Filter to asset types: image, video (repeatable)"),
    limit: int = typer.Option(50, "--limit", help="Maximum number of results"),
) -> None:
    """Search assets by full-text query on visual analysis (vibe or OCR)."""
    session_factory = _get_session_factory()
    search_repo = SearchRepository(session_factory)
    query_string = query if not ocr else None
    ocr_query = query if ocr else None
    library_slugs = library if library else None
    asset_types: list[str] | None = None
    if type_filter:
        valid = {"image", "video"}
        asset_types = [t.lower() for t in type_filter if t.lower() in valid]
        if not asset_types:
            typer.secho("--type must be 'image' or 'video'.", err=True)
            raise typer.Exit(1)
    results = search_repo.search_assets(
        query_string=query_string,
        ocr_query=ocr_query,
        library_slugs=library_slugs,
        asset_types=asset_types or None,
        limit=limit,
    )
    if not results:
        typer.secho("No matching assets found.", fg=typer.colors.YELLOW)
        return
    max_rank = max(r.final_rank for r in results) if results else 0

    def _confidence_cell(rank: float) -> Text:
        if max_rank <= 0:
            return Text("—", style="dim")
        pct = round(100 * rank / max_rank)
        s = f"{pct}%"
        if pct > 80:
            return Text(s, style="green")
        if pct > 50:
            return Text(s, style="yellow")
        return Text(s, style="red")

    def _best_ts_cell(best_scene_ts: float | None) -> str:
        if best_scene_ts is None:
            return "N/A"
        m = int(best_scene_ts) // 60
        s = int(best_scene_ts) % 60
        return f"{m}:{s:02d}"

    table = Table(title=None)
    table.add_column("Library")
    table.add_column("Relative Path")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Best Timestamp")
    table.add_column("Match Density")
    table.add_column("Confidence")
    for item in results:
        table.add_row(
            item.asset.library_id,
            item.asset.rel_path,
            item.asset.type.value,
            item.asset.status.value,
            _best_ts_cell(item.best_scene_ts),
            f"{item.match_ratio * 100:.1f}%",
            _confidence_cell(item.final_rank),
        )
    console = Console()
    console.print(table)


@app.command()
def scan(
    library_slug: str = typer.Argument(..., help="Library slug to scan once"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable DEBUG logging to stdout"),
) -> None:
    """Run a one-shot scan for the given library. Does not start the worker loop."""
    if verbose:
        root = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    lib = lib_repo.get_by_slug(library_slug)
    if lib is None:
        typer.echo(
            f"Library not found or deleted: '{library_slug}'. Use 'library list' to see valid slugs.",
            err=True,
        )
        raise typer.Exit(1)

    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)

    asset_repo.set_library_scan_status(library_slug, ScanStatus.full_scan_requested)

    worker_id = f"cli-scan-{library_slug}"
    worker_repo.register_worker(worker_id, WorkerState.idle)
    scanner = ScannerWorker(
        worker_id,
        worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        progress_interval=100 if verbose else None,
    )
    scanner.process_task(library_slug=library_slug)
    stats = scanner.get_heartbeat_stats()
    typer.echo("Scan complete. Heartbeat stats:")
    if stats:
        for k, v in stats.items():
            typer.echo(f"  {k}: {v}")
    else:
        typer.echo("  (no stats)")


@app.command("proxy")
def proxy(
    heartbeat: float = typer.Option(15.0, "--heartbeat", help="Heartbeat interval in seconds."),
    worker_name: str | None = typer.Option(None, "--worker-name", help="Force a specific worker ID. Defaults to auto-generated."),
    library_slug: str | None = typer.Option(None, "--library", help="Limit to this library slug only."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print progress (each asset and N/total)."),
    repair: bool = typer.Option(False, "--repair", help="Check for missing proxy/thumbnail files and set those assets to pending so they are regenerated."),
    once: bool = typer.Option(False, "--once", help="Process one batch then exit (no work = exit immediately)."),
    ignore_previews: bool = typer.Option(
        False,
        "--ignore-previews",
        help="When set, always perform full RAW decoding instead of using embedded/fast-path RAW previews.",
    ),
) -> None:
    """Start the image proxy worker: claims pending image assets, generates thumbnails and WebP proxies."""
    worker_id = (
        worker_name
        if worker_name is not None
        else f"proxy-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting Image Proxy Worker: {worker_id}")

    session_factory = _get_session_factory()
    cfg = get_config()
    if library_slug is not None:
        lib_repo = LibraryRepository(session_factory)
        lib = lib_repo.get_by_slug(library_slug)
        if lib is None:
            typer.echo(
                f"Library not found or deleted: '{library_slug}'. Use 'library list' to see valid slugs.",
                err=True,
            )
            raise typer.Exit(1)

    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)

    if verbose:
        root = logging.getLogger("src.workers")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    initial_pending = asset_repo.count_pending_proxyable(library_slug) if verbose else None

    use_previews = cfg.use_raw_previews and not ignore_previews

    if use_previews and not rawpy_available():
        typer.secho(
            "Warning: rawpy is not available. RAW files will use libvips fallback (higher memory use). "
            "Install rawpy and LibRaw for optimal RAW handling.",
            err=True,
            fg=typer.colors.YELLOW,
        )

    worker = ImageProxyWorker(
        worker_id=worker_id,
        repository=worker_repo,
        heartbeat_interval_seconds=heartbeat,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        library_slug=library_slug,
        verbose=verbose,
        initial_pending_count=initial_pending,
        repair=repair,
        use_previews=use_previews,
    )
    try:
        worker.run(once=once)
    except KeyboardInterrupt:
        typer.secho(f"Worker {worker_id} shutting down...")


@app.command("video-proxy")
def video_proxy(
    heartbeat: float = typer.Option(15.0, "--heartbeat", help="Heartbeat interval in seconds."),
    worker_name: str | None = typer.Option(None, "--worker-name", help="Force a specific worker ID. Defaults to auto-generated."),
    library_slug: str | None = typer.Option(None, "--library", help="Limit to this library slug only."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print progress (each asset and N/total)."),
    repair: bool = typer.Option(False, "--repair", help="Check for missing video thumbnail files and set those assets to pending."),
    once: bool = typer.Option(False, "--once", help="Process one batch then exit (no work = exit immediately)."),
) -> None:
    """Start the video proxy worker: claims pending video assets, 720p pipeline (thumbnail, head-clip, scene indexing)."""
    worker_id = (
        worker_name
        if worker_name is not None
        else f"video-proxy-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting Video Proxy Worker: {worker_id}")

    session_factory = _get_session_factory()
    if library_slug is not None:
        lib_repo = LibraryRepository(session_factory)
        lib = lib_repo.get_by_slug(library_slug)
        if lib is None:
            typer.echo(
                f"Library not found or deleted: '{library_slug}'. Use 'library list' to see valid slugs.",
                err=True,
            )
            raise typer.Exit(1)

    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)
    scene_repo = VideoSceneRepository(session_factory)

    if verbose:
        root = logging.getLogger("src.workers")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    initial_pending = asset_repo.count_pending_proxyable(library_slug) if verbose else None

    worker = VideoProxyWorker(
        worker_id=worker_id,
        repository=worker_repo,
        heartbeat_interval_seconds=heartbeat,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        scene_repo=scene_repo,
        library_slug=library_slug,
        verbose=verbose,
        initial_pending_count=initial_pending,
        repair=repair,
    )
    try:
        worker.run(once=once)
    except KeyboardInterrupt:
        typer.secho(f"Worker {worker_id} shutting down...")


def _resolve_effective_default_model_id(
    system_metadata_repo: SystemMetadataRepository,
    library_repo: LibraryRepository,
    library_slug: str | None,
) -> int | None:
    """Resolve effective default: COALESCE(library.target_tagger_id, system_default_id)."""
    system_default_id = system_metadata_repo.get_default_ai_model_id()
    if library_slug is not None:
        lib = library_repo.get_by_slug(library_slug)
        if lib is None:
            return None
        return lib.target_tagger_id if lib.target_tagger_id is not None else system_default_id
    return system_default_id


def _aimodel_name_is_mock(name: str) -> bool:
    """True if this aimodel name should be treated as mock (forbidden as default unless override)."""
    return name in ("mock", "mock-analyzer")


@ai_default_app.command("set")
def ai_default_set(
    name: str = typer.Argument(..., help="Model name (e.g. moondream2)."),
    version: str | None = typer.Argument(None, help="Model version; if omitted, latest by id for that name."),
) -> None:
    """Set the system default AI model. Resolved by name and optional version. Rejects 'mock' unless MEDIASEARCH_ALLOW_MOCK_DEFAULT=1."""
    session_factory = _get_session_factory()
    system_metadata_repo = SystemMetadataRepository(session_factory)
    model = system_metadata_repo.get_ai_model_by_name_version(name, version)
    if model is None:
        typer.secho(
            f"No AI model found for name '{name}'" + (f" version '{version}'" if version else "") + ".",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(1)
    assert model.id is not None
    try:
        system_metadata_repo.set_default_ai_model_id(model.id)
        typer.secho(f"Default AI model set to '{model.name}' version '{model.version}' (id={model.id}).", fg=typer.colors.GREEN)
    except ValueError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(1)


@ai_default_app.command("show")
def ai_default_show() -> None:
    """Show the current system default AI model (id, name, version) or a message if unset."""
    session_factory = _get_session_factory()
    system_metadata_repo = SystemMetadataRepository(session_factory)
    model_id = system_metadata_repo.get_default_ai_model_id()
    if model_id is None:
        typer.echo("No default AI model set. Use 'ai default set <name> [version]' to set one.")
        return
    model = system_metadata_repo.get_ai_model_by_id(model_id)
    if model is None:
        typer.echo(f"Default AI model id {model_id} is set but the model row was not found (may have been removed).")
        return
    typer.echo(f"Default AI model: id={model.id} name={model.name} version={model.version}")


@ai_app.command("start")
def ai_start(
    heartbeat: float = typer.Option(15.0, "--heartbeat", help="Heartbeat interval in seconds."),
    worker_name: str | None = typer.Option(None, "--worker-name", help="Force a specific worker ID. Defaults to auto-generated."),
    library_slug: str | None = typer.Option(None, "--library", help="Limit to this library slug only."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print progress for each completed asset."),
    analyzer: str | None = typer.Option(None, "--analyzer", help="AI model to use (e.g. mock, moondream2). If omitted, uses library or system default."),
    repair: bool = typer.Option(False, "--repair", help="Before the main loop, set assets that need re-analysis (effective model changed) to proxied."),
    once: bool = typer.Option(False, "--once", help="Process one batch then exit (no work = exit immediately)."),
    batch: int = typer.Option(1, "--batch", help="Number of assets to claim and process in parallel per task."),
    mode: str = typer.Option("full", "--mode", help="Processing tier: 'light' (fast tags/desc) or 'full' (OCR)."),
) -> None:
    """Start the AI worker: claims proxied assets, runs vision analysis, marks completed."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)

    if library_slug is not None:
        lib = lib_repo.get_by_slug(library_slug)
        if lib is None:
            typer.echo(
                f"Library not found or deleted: '{library_slug}'. Use 'library list' to see valid slugs.",
                err=True,
            )
            raise typer.Exit(1)

    resolved_analyzer: str
    system_default_model_id: int | None = system_metadata_repo.get_default_ai_model_id()

    if analyzer is not None:
        resolved_analyzer = analyzer
    else:
        effective_id = _resolve_effective_default_model_id(system_metadata_repo, lib_repo, library_slug)
        if effective_id is None:
            typer.secho(
                "No default AI model. Set one with 'ai default set <name> [version]' or use --analyzer.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(1)
        model = system_metadata_repo.get_ai_model_by_id(effective_id)
        if model is None:
            typer.secho(f"Default AI model id {effective_id} not found.", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        if _aimodel_name_is_mock(model.name) and os.environ.get(ALLOW_MOCK_DEFAULT_ENV, "").strip() != "1":
            typer.secho(
                f"Cannot use 'mock' as default. Set {ALLOW_MOCK_DEFAULT_ENV}=1 only in tests if needed.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(1)
        resolved_analyzer = model.name
        if resolved_analyzer == "mock-analyzer":
            resolved_analyzer = "mock"

    worker_id = (
        worker_name
        if worker_name is not None
        else f"ai-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting AI Worker: {worker_id} (analyzer: {resolved_analyzer})")

    if verbose:
        root = logging.getLogger("src.workers")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    worker = AIWorker(
        worker_id=worker_id,
        repository=worker_repo,
        heartbeat_interval_seconds=heartbeat,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        library_slug=library_slug,
        verbose=verbose,
        analyzer_name=resolved_analyzer,
        system_default_model_id=system_default_model_id,
        repair=repair,
        library_repo=lib_repo if repair else None,
        batch_size=batch,
        mode=mode,
    )
    try:
        worker.run(once=once)
    except KeyboardInterrupt:
        typer.secho(f"Worker {worker_id} shutting down...")


@ai_app.command("video")
def ai_video(
    heartbeat: float = typer.Option(15.0, "--heartbeat", help="Heartbeat interval in seconds."),
    worker_name: str | None = typer.Option(None, "--worker-name", help="Force a specific worker ID. Defaults to auto-generated."),
    library_slug: str | None = typer.Option(None, "--library", help="Limit to this library slug only."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print progress for each completed asset."),
    analyzer: str | None = typer.Option(None, "--analyzer", help="AI model to use (e.g. mock, moondream2). If omitted, uses library or system default."),
    once: bool = typer.Option(False, "--once", help="Process one batch then exit (no work = exit immediately)."),
    mode: str = typer.Option("full", "--mode", help="Processing tier: 'light' (fast tags/desc) or 'full' (OCR)."),
) -> None:
    """Start the Video worker: claims proxied video assets, runs vision on scene rep frames, marks completed."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)
    scene_repo = VideoSceneRepository(session_factory)

    if library_slug is not None:
        lib = lib_repo.get_by_slug(library_slug)
        if lib is None:
            typer.echo(
                f"Library not found or deleted: '{library_slug}'. Use 'library list' to see valid slugs.",
                err=True,
            )
            raise typer.Exit(1)

    resolved_analyzer: str
    system_default_model_id: int | None = system_metadata_repo.get_default_ai_model_id()

    if analyzer is not None:
        resolved_analyzer = analyzer
    else:
        effective_id = _resolve_effective_default_model_id(system_metadata_repo, lib_repo, library_slug)
        if effective_id is None:
            typer.secho(
                "No default AI model. Set one with 'ai default set <name> [version]' or use --analyzer.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(1)
        model = system_metadata_repo.get_ai_model_by_id(effective_id)
        if model is None:
            typer.secho(f"Default AI model id {effective_id} not found.", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        if _aimodel_name_is_mock(model.name) and os.environ.get(ALLOW_MOCK_DEFAULT_ENV, "").strip() != "1":
            typer.secho(
                f"Cannot use 'mock' as default. Set {ALLOW_MOCK_DEFAULT_ENV}=1 only in tests if needed.",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(1)
        resolved_analyzer = model.name
        if resolved_analyzer == "mock-analyzer":
            resolved_analyzer = "mock"

    worker_id = (
        worker_name
        if worker_name is not None
        else f"video-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting Video Worker: {worker_id} (analyzer: {resolved_analyzer})")

    # Always show progress (which video, each scene, completion)
    root = logging.getLogger("src.workers")
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    worker = VideoWorker(
        worker_id=worker_id,
        repository=worker_repo,
        heartbeat_interval_seconds=heartbeat,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        scene_repo=scene_repo,
        library_slug=library_slug,
        verbose=verbose,
        analyzer_name=resolved_analyzer,
        system_default_model_id=system_default_model_id,
        mode=mode,
    )
    try:
        worker.run(once=once)
    except KeyboardInterrupt:
        typer.secho(f"Worker {worker_id} shutting down...")


@ai_app.command("list")
def ai_list() -> None:
    """List all registered AI models (ID, Name, Version)."""
    session_factory = _get_session_factory()
    system_metadata_repo = SystemMetadataRepository(session_factory)
    models = system_metadata_repo.get_all_ai_models()
    if not models:
        typer.echo("No AI models registered.")
        return
    table = Table(title=None)
    table.add_column("ID", style="dim")
    table.add_column("Name")
    table.add_column("Version")
    for m in models:
        id_str = str(m.id) if m.id is not None else ""
        table.add_row(id_str, m.name, m.version)
    console = Console()
    console.print(table)


@ai_app.command("add")
def ai_add(
    name: str = typer.Argument(..., help="Model name"),
    version: str = typer.Argument(..., help="Model version"),
) -> None:
    """Register an AI model by name and version."""
    session_factory = _get_session_factory()
    system_metadata_repo = SystemMetadataRepository(session_factory)
    model = system_metadata_repo.add_ai_model(name, version)
    typer.echo(f"Added AI model '{model.name}' version '{model.version}' (id={model.id}).")


@ai_app.command("remove")
def ai_remove(
    name: str = typer.Argument(..., help="Model name to remove (all versions)."),
    force: bool = typer.Option(False, "--force", help="Skip confirmation"),
) -> None:
    """Remove an AI model by name. Fails if any asset references it."""
    if not force:
        typer.confirm(
            f"Remove AI model '{name}' (all versions)? This will fail if any asset references it.",
            abort=True,
        )
    session_factory = _get_session_factory()
    system_metadata_repo = SystemMetadataRepository(session_factory)
    try:
        removed = system_metadata_repo.remove_ai_model(name)
        if removed:
            typer.secho(f"Removed AI model '{name}'.", fg=typer.colors.GREEN)
        else:
            typer.echo(f"No AI model named '{name}' found.")
    except ValueError as e:
        typer.secho(str(e), fg=typer.colors.RED)
        raise typer.Exit(1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
