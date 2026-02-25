"""Typer Admin CLI: library management and one-shot scan."""

import json
import logging
import socket
import sys
import uuid
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.text import Text

from src.core.config import get_config
from src.models.entities import AssetStatus, ScanStatus, WorkerState
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.search_repo import SearchRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.ai_worker import AIWorker
from src.workers.proxy_worker import ProxyWorker
from src.workers.scanner import ScannerWorker

app = typer.Typer(no_args_is_help=True)
library_app = typer.Typer(help="Add, remove, restore, and list libraries.")
app.add_typer(library_app, name="library")
trash_app = typer.Typer(help="Manage soft-deleted libraries.")
app.add_typer(trash_app, name="trash")
asset_app = typer.Typer(help="Manage individual assets.")
app.add_typer(asset_app, name="asset")
ai_app = typer.Typer(help="Manage AI models and workers")
app.add_typer(ai_app, name="ai")


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
    slug: str = typer.Argument(..., help="Library slug to soft-delete"),
) -> None:
    """Soft-delete a library (moves to trash)."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    lib_repo.soft_delete(slug)
    typer.echo("Library moved to trash.")


@library_app.command("restore")
def library_restore(
    slug: str = typer.Argument(..., help="Library slug to restore from trash"),
) -> None:
    """Restore a soft-deleted library."""
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    lib_repo.restore(slug)
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
    slug: str = typer.Argument(..., help="Library slug to permanently delete"),
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
        lib_repo.hard_delete(slug)
        typer.secho(f"Permanently deleted library '{slug}'.", fg=typer.colors.GREEN)
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


@asset_app.command("list")
def asset_list(
    library_slug: str = typer.Argument(..., help="Library slug to list assets for"),
    limit: int = typer.Option(50, "--limit", help="Maximum number of assets to show"),
    status: str | None = typer.Option(None, "--status", help="Filter by status (e.g. pending, completed)"),
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


@app.command("search")
def search(
    query: str = typer.Argument(None, help="Global search (e.g., 'man in blue shirt')"),
    ocr: bool = typer.Option(False, "--ocr", help="Search only within extracted OCR text"),
    library: str | None = typer.Option(None, "--library", help="Filter by library slug"),
    limit: int = typer.Option(50, "--limit", help="Maximum number of results"),
) -> None:
    """Search assets by full-text query on visual analysis (vibe or OCR)."""
    session_factory = _get_session_factory()
    search_repo = SearchRepository(session_factory)
    query_string = query if not ocr else None
    ocr_query = query if ocr else None
    results = search_repo.search_assets(
        query_string=query_string,
        ocr_query=ocr_query,
        library_slug=library,
        limit=limit,
    )
    if not results:
        typer.secho("No matching assets found.", fg=typer.colors.YELLOW)
        return
    ranks = [r for _, r in results]
    max_rank = max(ranks) if results else 0

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

    table = Table(title=None)
    table.add_column("Library")
    table.add_column("Relative Path")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Confidence")
    for a, rank in results:
        table.add_row(
            a.library_id,
            a.rel_path,
            a.type.value,
            a.status.value,
            _confidence_cell(rank),
        )
    console = Console()
    console.print(table)


@app.command()
def scan(
    slug: str = typer.Argument(..., help="Library slug to scan once"),
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
    lib = lib_repo.get_by_slug(slug)
    if lib is None:
        typer.echo(
            f"Library not found or deleted: '{slug}'. Use 'library list' to see valid slugs.",
            err=True,
        )
        raise typer.Exit(1)

    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)

    asset_repo.set_library_scan_status(slug, ScanStatus.full_scan_requested)

    worker_id = f"cli-scan-{slug}"
    worker_repo.register_worker(worker_id, WorkerState.idle)
    scanner = ScannerWorker(
        worker_id,
        worker_repo,
        heartbeat_interval_seconds=15.0,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        progress_interval=100 if verbose else None,
    )
    scanner.process_task(library_slug=slug)
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
) -> None:
    """Start the proxy worker: claims pending assets, generates thumbnails and proxies."""
    worker_id = (
        worker_name
        if worker_name is not None
        else f"proxy-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting Proxy Worker: {worker_id}")

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

    if verbose:
        root = logging.getLogger("src.workers")
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    initial_pending = asset_repo.count_pending(library_slug) if verbose else None

    worker = ProxyWorker(
        worker_id=worker_id,
        repository=worker_repo,
        heartbeat_interval_seconds=heartbeat,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
        library_slug=library_slug,
        verbose=verbose,
        initial_pending_count=initial_pending,
        repair=repair,
    )
    try:
        worker.run()
    except KeyboardInterrupt:
        typer.secho(f"Worker {worker_id} shutting down...")


@ai_app.command("start")
def ai_start(
    heartbeat: float = typer.Option(15.0, "--heartbeat", help="Heartbeat interval in seconds."),
    worker_name: str | None = typer.Option(None, "--worker-name", help="Force a specific worker ID. Defaults to auto-generated."),
    library_slug: str | None = typer.Option(None, "--library", help="Limit to this library slug only."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print progress for each completed asset."),
    analyzer: str = typer.Option("mock", "--analyzer", help="Which AI model to use (mock or moondream2)."),
) -> None:
    """Start the AI worker: claims proxied assets, runs vision analysis, marks completed."""
    worker_id = (
        worker_name
        if worker_name is not None
        else f"ai-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting AI Worker: {worker_id} (analyzer: {analyzer})")

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
        analyzer_name=analyzer,
    )
    try:
        worker.run()
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
