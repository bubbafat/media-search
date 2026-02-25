"""Typer Admin CLI: library management and one-shot scan."""

import logging
import sys
from pathlib import Path

import typer

from src.core.config import get_config
from src.models.entities import ScanStatus, WorkerState
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.scanner import ScannerWorker

app = typer.Typer(no_args_is_help=True)
library_app = typer.Typer()
app.add_typer(library_app, name="library")


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
        typer.echo(str(e), err=True)
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
