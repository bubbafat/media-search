"""Maintenance service: prune stale workers, reclaim expired leases, cleanup temp files."""

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from src.repository.asset_repo import AssetRepository
from src.repository.worker_repo import WorkerRepository

if TYPE_CHECKING:
    from src.repository.library_repo import LibraryRepository
    from src.repository.video_scene_repo import VideoSceneRepository

_log = logging.getLogger(__name__)

MAX_TEMP_AGE_SECONDS = 4 * 3600  # 4 hours
DEFAULT_CLEANUP_DATA_MIN_AGE_SECONDS = 15 * 60  # 15 minutes


class MaintenanceService:
    """Central service for janitor tasks (prune workers, reclaim leases, cleanup tmp)."""

    def __init__(
        self,
        asset_repo: AssetRepository,
        worker_repo: WorkerRepository,
        data_dir: Path | str,
        *,
        library_repo: "LibraryRepository",
        video_scene_repo: "VideoSceneRepository",
        hostname: str = "",
    ) -> None:
        self._asset_repo = asset_repo
        self._worker_repo = worker_repo
        self._data_dir = Path(data_dir)
        self._library_repo = library_repo
        self._video_scene_repo = video_scene_repo
        self._hostname = hostname

    def run_all(self, *, library_slug: str | None = None) -> None:
        """Execute all maintenance tasks in order. When library_slug is set, temp cleanup and reclaim are filtered to that library."""
        self.prune_stale_workers()
        self.reclaim_stale_leases(library_slug=library_slug)
        self.cleanup_temp_dir(library_slug=library_slug)

    def prune_stale_workers(self, max_age_hours: int = 24) -> int:
        """Delete worker_status rows older than max_age_hours. Returns count deleted."""
        return self._worker_repo.prune_stale_workers(max_age_hours=max_age_hours)

    def reclaim_stale_leases(self, *, library_slug: str | None = None) -> int:
        """Reset assets stuck in processing with expired leases. When library_slug is set, only reclaim assets in that library. Returns count updated."""
        return self._asset_repo.reclaim_stale_leases(library_slug=library_slug)

    def preview_temp_cleanup(
        self,
        max_age_seconds: int = MAX_TEMP_AGE_SECONDS,
        *,
        library_slug: str | None = None,
    ) -> tuple[int, int]:
        """
        Preview files in data_dir/tmp that would be deleted.
        When library_slug is set, only considers data_dir/tmp/library_slug/.
        Returns (file_count, total_bytes). Does not modify anything.
        """
        tmp_dir = self._data_dir / "tmp"
        if library_slug is not None:
            tmp_dir = tmp_dir / library_slug
        if not tmp_dir.is_dir():
            return (0, 0)
        cutoff = time.time() - max_age_seconds
        file_count = 0
        total_bytes = 0
        try:
            for entry in tmp_dir.rglob("*"):
                if entry.is_symlink():
                    continue
                if entry.is_file():
                    try:
                        st = entry.stat()
                        if st.st_mtime < cutoff:
                            file_count += 1
                            total_bytes += st.st_size
                    except (PermissionError, OSError):
                        pass
        except (PermissionError, OSError):
            pass
        return (file_count, total_bytes)

    def cleanup_temp_dir(
        self,
        max_age_seconds: int = MAX_TEMP_AGE_SECONDS,
        *,
        library_slug: str | None = None,
    ) -> int:
        """Delete files in data_dir/tmp older than max_age_seconds. When library_slug is set, only cleans data_dir/tmp/library_slug/. Returns count deleted."""
        if self._hostname and self._worker_repo.has_active_local_transcodes(self._hostname):
            _log.info(
                "Active local transcode detected. Skipping 'tmp' directory cleanup for safety."
            )
            return 0
        tmp_dir = self._data_dir / "tmp"
        if library_slug is not None:
            tmp_dir = tmp_dir / library_slug
        if not tmp_dir.is_dir():
            return 0
        cutoff = time.time() - max_age_seconds
        deleted = 0
        dirs_to_prune: list[Path] = []
        try:
            for entry in tmp_dir.rglob("*"):
                if entry.is_symlink():
                    continue
                if entry.is_file():
                    try:
                        if entry.stat().st_mtime < cutoff:
                            entry.unlink()
                            deleted += 1
                    except (PermissionError, OSError) as e:
                        _log.warning("Could not delete %s: %s", entry, e)
                elif entry.is_dir():
                    dirs_to_prune.append(entry)
            for d in sorted(dirs_to_prune, key=lambda p: len(p.parts), reverse=True):
                try:
                    if d.exists() and not any(d.iterdir()):
                        d.rmdir()
                except (PermissionError, OSError) as e:
                    _log.warning("Could not remove empty dir %s: %s", d, e)
        except (PermissionError, OSError) as e:
            _log.warning("Error during tmp cleanup: %s", e)
        return deleted

    def preview_data_dir_cleanup(
        self, min_file_age_seconds: int = DEFAULT_CLEANUP_DATA_MIN_AGE_SECONDS
    ) -> tuple[int, int]:
        """
        Preview orphaned files under data_dir that would be deleted.
        Returns (file_count, total_bytes). Does not modify anything.
        """
        non_deleted_libs = self._library_repo.list_libraries(include_deleted=False)
        non_deleted_slugs = {lib.slug for lib in non_deleted_libs}
        expected: set[str] = set()
        offset = 0
        limit = 500
        while True:
            batch = self._asset_repo.get_asset_ids_expecting_proxy(
                library_slug=None, limit=limit, offset=offset
            )
            if not batch:
                break
            for asset_id, lib_slug, _ in batch:
                shard = asset_id % 1000
                expected.add(f"{lib_slug}/thumbnails/{shard}/{asset_id}.jpg")
                expected.add(f"{lib_slug}/proxies/{shard}/{asset_id}.webp")
            offset += limit
        expected.update(
            self._asset_repo.get_all_video_preview_paths_excluding_trash()
        )
        expected.update(
            self._video_scene_repo.get_all_rep_frame_paths_excluding_trash()
        )

        cutoff = time.time() - min_file_age_seconds

        def _preview_walk(base: Path, exp: set[str]) -> tuple[int, int]:
            file_count = 0
            total_bytes = 0
            try:
                for entry in base.rglob("*"):
                    if entry.is_symlink():
                        continue
                    if entry.is_file():
                        try:
                            rel = entry.relative_to(self._data_dir).as_posix()
                            st = entry.stat()
                            if rel not in exp and st.st_mtime < cutoff:
                                file_count += 1
                                total_bytes += st.st_size
                        except (PermissionError, OSError):
                            pass
            except (PermissionError, OSError):
                pass
            return (file_count, total_bytes)

        total_files = 0
        total_bytes = 0
        for lib_slug in non_deleted_slugs:
            for sub in ("thumbnails", "proxies"):
                d = self._data_dir / lib_slug / sub
                if d.is_dir():
                    fc, tb = _preview_walk(d, expected)
                    total_files += fc
                    total_bytes += tb
        video_clips = self._data_dir / "video_clips"
        if video_clips.is_dir():
            for lib_slug in non_deleted_slugs:
                d = video_clips / lib_slug
                if d.is_dir():
                    fc, tb = _preview_walk(d, expected)
                    total_files += fc
                    total_bytes += tb
        video_scenes = self._data_dir / "video_scenes"
        if video_scenes.is_dir():
            for lib_slug in non_deleted_slugs:
                d = video_scenes / lib_slug
                if d.is_dir():
                    fc, tb = _preview_walk(d, expected)
                    total_files += fc
                    total_bytes += tb
        return (total_files, total_bytes)

    def cleanup_data_dir(
        self, min_file_age_seconds: int = DEFAULT_CLEANUP_DATA_MIN_AGE_SECONDS
    ) -> int:
        """
        Remove orphaned files under data_dir (no corresponding DB entry).
        Excludes trashed libraries. Only deletes files older than min_file_age_seconds.
        Returns count deleted.
        """
        non_deleted_libs = self._library_repo.list_libraries(include_deleted=False)
        non_deleted_slugs = {lib.slug for lib in non_deleted_libs}
        expected: set[str] = set()

        # Paginate get_asset_ids_expecting_proxy for thumbnails and proxies
        offset = 0
        limit = 500
        while True:
            batch = self._asset_repo.get_asset_ids_expecting_proxy(
                library_slug=None, limit=limit, offset=offset
            )
            if not batch:
                break
            for asset_id, lib_slug, _ in batch:
                shard = asset_id % 1000
                expected.add(f"{lib_slug}/thumbnails/{shard}/{asset_id}.jpg")
                expected.add(f"{lib_slug}/proxies/{shard}/{asset_id}.webp")
            offset += limit

        # Video preview paths and scene rep frames
        expected.update(
            self._asset_repo.get_all_video_preview_paths_excluding_trash()
        )
        expected.update(
            self._video_scene_repo.get_all_rep_frame_paths_excluding_trash()
        )

        cutoff = time.time() - min_file_age_seconds
        deleted = 0
        dirs_to_prune: list[Path] = []

        def walk_dir(base: Path) -> None:
            nonlocal deleted
            try:
                for entry in base.rglob("*"):
                    if entry.is_symlink():
                        continue
                    if entry.is_file():
                        try:
                            rel = entry.relative_to(self._data_dir).as_posix()
                            if rel not in expected and entry.stat().st_mtime < cutoff:
                                entry.unlink()
                                deleted += 1
                        except (PermissionError, OSError) as e:
                            _log.warning("Could not delete %s: %s", entry, e)
                    elif entry.is_dir():
                        dirs_to_prune.append(entry)
            except (PermissionError, OSError) as e:
                _log.warning("Error walking %s: %s", base, e)

        for lib_slug in non_deleted_slugs:
            for sub in ("thumbnails", "proxies"):
                d = self._data_dir / lib_slug / sub
                if d.is_dir():
                    walk_dir(d)

        video_clips = self._data_dir / "video_clips"
        if video_clips.is_dir():
            for lib_slug in non_deleted_slugs:
                d = video_clips / lib_slug
                if d.is_dir():
                    walk_dir(d)

        video_scenes = self._data_dir / "video_scenes"
        if video_scenes.is_dir():
            for lib_slug in non_deleted_slugs:
                d = video_scenes / lib_slug
                if d.is_dir():
                    walk_dir(d)

        for d in sorted(dirs_to_prune, key=lambda p: len(p.parts), reverse=True):
            try:
                if d.exists() and not any(d.iterdir()):
                    d.rmdir()
            except (PermissionError, OSError) as e:
                _log.warning("Could not remove empty dir %s: %s", d, e)

        return deleted
