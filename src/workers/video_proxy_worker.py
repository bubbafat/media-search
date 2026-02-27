"""Video proxy worker: claims pending video assets, 720p pipeline (thumbnail, head-clip, scene indexing), updates to proxied."""

import logging
import tempfile
from pathlib import Path

from src.core.config import get_config
from src.core.file_extensions import VIDEO_EXTENSIONS_LIST
from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.video.clip_extractor import (
    extract_head_clip_copy,
    extract_video_frame,
    transcode_to_720p_h264,
)
from src.video.indexing import run_video_scene_indexing
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class VideoProxyWorker(BaseWorker):
    """
    Worker that claims pending video assets, runs the 720p disposable pipeline:
    transcode to temp 720p H.264 → thumbnail from temp → head-clip (stream copy) →
    scene indexing (pHash, rep frames, no vision) → cleanup temp → set proxied.
    """

    def __init__(
        self,
        worker_id: str,
        repository: WorkerRepository,
        heartbeat_interval_seconds: float = 15.0,
        *,
        asset_repo: AssetRepository,
        system_metadata_repo: SystemMetadataRepository,
        scene_repo: VideoSceneRepository,
        library_slug: str | None = None,
        verbose: bool = False,
        initial_pending_count: int | None = None,
        repair: bool = False,
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
        )
        self.asset_repo = asset_repo
        self.scene_repo = scene_repo
        self.storage = LocalMediaStore()
        self._library_slug = library_slug
        self._verbose = verbose
        self._initial_pending = initial_pending_count
        self._processed_count = 0
        self._repair = repair

    def _head_clip_path(self, library_slug: str, asset_id: int) -> Path:
        """Path to head_clip.mp4 for this asset (under data_dir)."""
        data_dir = Path(get_config().data_dir)
        return data_dir / "video_clips" / library_slug / str(asset_id) / "head_clip.mp4"

    def _run_repair_pass(self) -> None:
        """Find video assets that should have thumbnail and head-clip but are missing; set status to pending."""
        batch_size = 500
        offset = 0
        total_checked = 0
        total_reset = 0
        while True:
            batch = self.asset_repo.get_asset_ids_expecting_proxy(
                library_slug=self._library_slug,
                limit=batch_size,
                offset=offset,
            )
            if not batch:
                break
            for asset_id, library_slug, type_str in batch:
                if type_str != "video":
                    continue
                missing = False
                if not self.storage.thumbnail_exists(library_slug, asset_id):
                    missing = True
                if not self._head_clip_path(library_slug, asset_id).exists():
                    missing = True
                if missing:
                    self.asset_repo.update_asset_status(asset_id, AssetStatus.pending)
                    total_reset += 1
                total_checked += 1
            offset += len(batch)
            if len(batch) < batch_size:
                break
        if self._verbose or total_reset:
            _log.info(
                "Repair: checked %s videos, reset %s to pending",
                total_checked,
                total_reset,
            )

    def run(self, once: bool = False) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_repair_pass()
            if self._verbose:
                self._initial_pending = self.asset_repo.count_pending_proxyable(self._library_slug)
        super().run(once=once)

    def process_task(self) -> bool:
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.pending,
            VIDEO_EXTENSIONS_LIST,
            library_slug=self._library_slug,
        )
        if asset is None:
            return False
        assert asset.id is not None
        assert asset.library is not None
        library_slug = asset.library.slug
        source_path = Path(asset.library.absolute_path) / asset.rel_path
        data_dir = Path(get_config().data_dir)
        tmp_dir = data_dir / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        temp_fd = tempfile.NamedTemporaryFile(
            suffix=".mp4", dir=str(tmp_dir), delete=False
        )
        temp_path = Path(temp_fd.name)
        temp_fd.close()
        try:
            if not transcode_to_720p_h264(source_path, temp_path):
                raise RuntimeError("720p transcode failed")
            thumb_path = self.storage.get_thumbnail_write_path(library_slug, asset.id)
            if not extract_video_frame(temp_path, thumb_path, 0.0):
                raise RuntimeError("FFmpeg frame extraction failed")
            head_clip_path = self._head_clip_path(library_slug, asset.id)
            if not extract_head_clip_copy(temp_path, head_clip_path, duration=10.0):
                raise RuntimeError("Head-clip copy failed")
            run_video_scene_indexing(
                asset.id,
                temp_path,
                library_slug,
                self.scene_repo,
                vision_analyzer=None,
                check_interrupt=lambda: self.should_exit,
            )
            self.asset_repo.set_video_preview_path(
                asset.id, f"video_clips/{library_slug}/{asset.id}/head_clip.mp4"
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.proxied)
            self._processed_count += 1
            if self._verbose:
                total = self._initial_pending if self._initial_pending is not None else "?"
                _log.info(
                    "Proxied video %s (%s) %s/%s",
                    asset.id,
                    asset.rel_path,
                    self._processed_count,
                    total,
                )
        except InterruptedError:
            self.asset_repo.update_asset_status(asset.id, AssetStatus.pending)
            return False
        except Exception as e:
            _log.error(
                "Video proxy worker failed for asset %s (%s): %s",
                asset.id,
                source_path,
                e,
                exc_info=True,
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, str(e))
        finally:
            temp_path.unlink(missing_ok=True)
        return True
