"""Typer Admin CLI: library management and one-shot scan."""

import logging
import socket
import sys
import uuid
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from src.core.config import get_config
from src.models.entities import AssetStatus, ScanStatus, WorkerState
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.proxy_worker import ProxyWorker
from src.workers.scanner import ScannerWorker

app = typer.Typer(no_args_is_help=True)
library_app = typer.Typer(help="Add, remove, restore, and list libraries.")
app.add_typer(library_app, name="library")
trash_app = typer.Typer(help="Manage soft-deleted libraries.")
app.add_typer(trash_app, name="trash")
asset_app = typer.Typer(help="Manage individual assets.")
app.add_typer(asset_app, name="asset")


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
) -> None:
    """Permanently delete all trashed libraries and their assets. Cannot be undone."""
    if not force:
        typer.confirm("Permanently delete ALL trashed libraries?", abort=True)
    session_factory = _get_session_factory()
    lib_repo = LibraryRepository(session_factory)
    count = lib_repo.hard_delete_all_trashed()
    typer.secho(f"Permanently deleted {count} library(ies).", fg=typer.colors.GREEN)


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
    typer.echo(f"Showing {len(assets)} assets for library '{library_slug}'.")


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
) -> None:
    """Start the proxy worker: claims pending assets, generates thumbnails and proxies."""
    worker_id = (
        worker_name
        if worker_name is not None
        else f"proxy-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    typer.secho(f"Starting Proxy Worker: {worker_id}")

    session_factory = _get_session_factory()
    asset_repo = AssetRepository(session_factory)
    worker_repo = WorkerRepository(session_factory)
    system_metadata_repo = SystemMetadataRepository(session_factory)

    worker = ProxyWorker(
        worker_id=worker_id,
        repository=worker_repo,
        heartbeat_interval_seconds=heartbeat,
        asset_repo=asset_repo,
        system_metadata_repo=system_metadata_repo,
    )
    try:
        worker.run()
    except KeyboardInterrupt:
        typer.secho(f"Worker {worker_id} shutting down...")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
