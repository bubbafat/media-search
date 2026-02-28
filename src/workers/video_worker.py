"""Video worker: claims proxied video assets, runs vision-only pass on existing scene rep frames."""

import logging
from pathlib import Path

from src.ai.factory import get_vision_analyzer
from src.core.config import get_config
from src.core.file_extensions import VIDEO_EXTENSIONS_LIST
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.video.indexing import run_vision_on_scenes
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class VideoWorker(BaseWorker):
    """
    Worker that claims proxied video assets, runs vision analysis on existing scene
    rep frames (persisted by VideoProxyWorker from the 720p pipeline), and marks
    assets completed. Does not re-read source video or generate head-clip (already
    set by VideoProxyWorker). Supports lease renewal and graceful shutdown.
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
        analyzer_name: str = "mock",
        system_default_model_id: int | None = None,
        mode: str = "full",
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
        )
        self.asset_repo = asset_repo
        self._scene_repo = scene_repo
        self.analyzer = get_vision_analyzer(analyzer_name)
        self.db_model_id = self._system_metadata_repo.get_or_create_ai_model(
            self.analyzer.get_model_card()
        )
        self._library_slug = library_slug
        self._verbose = verbose
        self._system_default_model_id = system_default_model_id
        self._mode = mode

    def run(self, once: bool = False) -> None:
        """Run the normal worker loop. Pass once=True to exit when no work is available."""
        super().run(once=once)

    def process_task(self) -> bool:
        claim_status = (
            AssetStatus.proxied if self._mode == "light" else AssetStatus.analyzed_light
        )
        claim_kwargs: dict = {"library_slug": self._library_slug}
        if self._system_default_model_id is not None:
            claim_kwargs["target_model_id"] = self.db_model_id
            claim_kwargs["system_default_model_id"] = self._system_default_model_id
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            claim_status,
            VIDEO_EXTENSIONS_LIST,
            lease_seconds=300,
            **claim_kwargs,
        )
        if asset is None:
            return False
        assert asset.id is not None
        assert asset.library is not None

        def _check_interrupt() -> bool:
            return self.should_exit

        _log.info("Processing video (vision-only): %s", asset.rel_path)
        try:
            run_vision_on_scenes(
                asset.id,
                asset.library.slug,
                self._scene_repo,
                self.analyzer,
                mode=self._mode,
                check_interrupt=_check_interrupt,
                renew_lease=lambda: self.asset_repo.renew_asset_lease(asset.id, 300),
            )
            if asset.video_preview_path is None or asset.video_preview_path == "":
                data_dir = Path(get_config().data_dir)
                clip_path = data_dir / "video_clips" / asset.library.slug / str(asset.id) / "head_clip.mp4"
                if clip_path.exists():
                    self.asset_repo.set_video_preview_path(
                        asset.id, f"video_clips/{asset.library.slug}/{asset.id}/head_clip.mp4"
                    )
            if self._mode == "light":
                self.asset_repo.mark_analyzed_light(asset.id, self.db_model_id)
            else:
                self.asset_repo.mark_completed(asset.id, self.db_model_id)
            _log.info(
                "Completed: %s (%s/%s)",
                asset.id,
                asset.library.slug,
                asset.rel_path,
            )
            return True
        except InterruptedError:
            reset_status = (
                AssetStatus.proxied if self._mode == "light" else AssetStatus.analyzed_light
            )
            self.asset_repo.update_asset_status(asset.id, reset_status)
            return False
        except Exception as e:
            _log.error(
                "Video worker failed for asset %s (%s): %s",
                asset.id,
                asset.rel_path,
                e,
                exc_info=True,
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, str(e))
            return True
