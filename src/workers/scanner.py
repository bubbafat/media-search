"""Scanner worker: discovers files under library roots and upserts assets. Inherits BaseWorker."""

import logging
import os
from pathlib import Path
from typing import Callable

from src.core.path_resolver import get_library_root
from src.models.entities import AssetType, ScanStatus, WorkerState
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker

# Supported extensions: video and image only
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
SUPPORTED_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS

STATS_INTERVAL = 1_000
MTIME_DECIMALS = 3

_log = logging.getLogger(__name__)


def _asset_type_for_path(path: Path) -> AssetType:
    ext = path.suffix.lower()
    if ext in VIDEO_EXTENSIONS:
        return AssetType.video
    return AssetType.image


def _scan_dir(
    current_dir: Path,
    library_root: Path,
    library_id: str,
    asset_repo: AssetRepository,
    worker_id: str,
    worker_repo: WorkerRepository,
    should_stop: Callable[[], bool],
) -> int:
    """Recursively walk current_dir with os.scandir; rel_path is always relative to library_root. Returns file count."""
    count = 0
    try:
        entries = list(os.scandir(current_dir))
    except (PermissionError, OSError) as e:
        _log.error("Scanner: %s", e, exc_info=True)
        return 0

    for entry in entries:
        if should_stop():
            return count
        try:
            if entry.is_dir(follow_symlinks=False):
                count += _scan_dir(
                    Path(entry.path),
                    library_root,
                    library_id,
                    asset_repo,
                    worker_id,
                    worker_repo,
                    should_stop,
                )
            elif entry.is_file(follow_symlinks=False):
                path = Path(entry.path)
                if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                    continue
                try:
                    stat = entry.stat(follow_symlinks=False)
                except (PermissionError, OSError) as e:
                    _log.warning("Scanner stat error for %s: %s", entry.path, e)
                    continue
                rel_path = path.relative_to(library_root).as_posix()
                mtime = round(stat.st_mtime, MTIME_DECIMALS)
                size = stat.st_size
                atype = _asset_type_for_path(path)
                asset_repo.upsert_asset(library_id, rel_path, atype, mtime, size)
                count += 1
                if count % STATS_INTERVAL == 0:
                    worker_repo.update_heartbeat(worker_id, stats={"files_processed": count})
                    _log.info("Scanner: files_processed=%s", count)
                    if should_stop():
                        return count
        except (PermissionError, OSError) as e:
            _log.warning("Scanner error at %s: %s", getattr(entry, "path", current_dir), e)
    return count


class ScannerWorker(BaseWorker):
    """
    Worker that claims libraries with full_scan_requested or fast_scan_requested, walks their roots with os.scandir,
    and upserts assets. Respects pause/shutdown and logs filesystem errors to FlightLogger.
    """

    def __init__(
        self,
        worker_id: str,
        repository: WorkerRepository,
        heartbeat_interval_seconds: float = 15.0,
        *,
        asset_repo: AssetRepository,
        system_metadata_repo: SystemMetadataRepository,
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
        )
        self._asset_repo = asset_repo
        self._last_files_processed: int = 0

    def get_heartbeat_stats(self) -> dict | None:
        return {"files_processed": self._last_files_processed}

    def process_task(self, library_slug: str | None = None) -> None:
        library = self._asset_repo.claim_library_for_scanning(slug=library_slug)
        if library is None:
            _log.info("No libraries require scanning")
            return

        def should_stop():
            return self.should_exit or self._state == WorkerState.paused

        if library.absolute_path:
            root = Path(library.absolute_path).resolve()
        else:
            try:
                root = get_library_root(library.slug)
            except ValueError as e:
                _log.warning("Scanner: %s; resetting library to idle", e)
                self._asset_repo.set_library_scan_status(library.slug, ScanStatus.idle)
                return
            except OSError as e:
                _log.warning("Scanner: %s; resetting library to idle", e)
                self._asset_repo.set_library_scan_status(library.slug, ScanStatus.idle)
                return

        if not root.exists():
            _log.warning("Scanner: library root does not exist %s; resetting library to idle", root)
            self._asset_repo.set_library_scan_status(library.slug, ScanStatus.idle)
            return

        self._set_state(WorkerState.processing, persist=True)
        try:
            self._last_files_processed = _scan_dir(
                root,
                root,
                library.slug,
                self._asset_repo,
                self.worker_id,
                self._repo,
                should_stop,
            )
        finally:
            self._asset_repo.set_library_scan_status(library.slug, ScanStatus.idle)
            self._set_state(WorkerState.idle, persist=True)
