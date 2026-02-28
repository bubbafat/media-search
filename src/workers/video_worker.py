"""Video worker: claims proxied video assets, runs vision-only pass on existing scene rep frames."""

import logging
from pathlib import Path

from src.ai.factory import get_vision_analyzer
from src.core.config import get_config
from src.core.file_extensions import VIDEO_EXTENSIONS_LIST
from src.core.io_utils import file_non_empty
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.video.indexing import needs_ocr, run_vision_on_scenes
from src.workers.base import BaseWorker
from src.workers.constants import MAX_RETRY_COUNT_BEFORE_POISON

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
        self._global_mode = library_slug is None
        self._verbose = verbose
        self._system_default_model_id = system_default_model_id
        self._mode = mode

    def run(self, once: bool = False) -> None:
        """Run the normal worker loop. Pass once=True to exit when no work is available."""
        super().run(once=once)

    def process_task(self) -> bool:
        if self._library_slug is None and not self._global_mode:
            raise RuntimeError("Worker scope is ambiguous: library_slug is None but global_mode is False.")
        claim_status = (
            AssetStatus.proxied if self._mode == "light" else AssetStatus.analyzed_light
        )
        claim_kwargs: dict = {"library_slug": self._library_slug, "global_scope": self._global_mode}
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
            asset = self.asset_repo.claim_asset_by_status(
                self.worker_id,
                AssetStatus.failed,
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

        active_count = self._repo.get_active_local_worker_count(
            self.hostname, self.worker_id
        )
        should_flush_memory = active_count > 0

        _log.info("Processing video (vision-only): %s", asset.rel_path)
        try:
            run_vision_on_scenes(
                asset.id,
                asset.library.slug,
                self._scene_repo,
                self.analyzer,
                effective_model_id=self.db_model_id,
                mode=self._mode,
                asset_analysis_model_id=asset.analysis_model_id,
                asset_tags_model_id=asset.tags_model_id,
                check_interrupt=_check_interrupt,
                renew_lease=lambda: self.asset_repo.renew_asset_lease(
                    asset.id, 300, worker_id=self.worker_id
                ),
                should_flush_memory=should_flush_memory,
            )
            # Safety check: before mark_completed, ensure all scenes have description and OCR
            if self._mode == "full":
                _renew = lambda: self.asset_repo.renew_asset_lease(
                    asset.id, 300, worker_id=self.worker_id
                )
                for _ in range(3):
                    scenes = self._scene_repo.list_scenes(asset.id)
                    missing_desc = [s for s in scenes if s.description is None]
                    missing_ocr = [s for s in scenes if s.description and needs_ocr(s)]
                    if not missing_desc and not missing_ocr:
                        break
                    if missing_desc:
                        run_vision_on_scenes(
                            asset.id,
                            asset.library.slug,
                            self._scene_repo,
                            self.analyzer,
                            effective_model_id=self.db_model_id,
                            mode="light",
                            asset_analysis_model_id=asset.analysis_model_id,
                            asset_tags_model_id=asset.tags_model_id,
                            check_interrupt=_check_interrupt,
                            renew_lease=_renew,
                            should_flush_memory=should_flush_memory,
                        )
                    elif missing_ocr:
                        run_vision_on_scenes(
                            asset.id,
                            asset.library.slug,
                            self._scene_repo,
                            self.analyzer,
                            effective_model_id=self.db_model_id,
                            mode="full",
                            asset_analysis_model_id=asset.analysis_model_id,
                            asset_tags_model_id=asset.tags_model_id,
                            check_interrupt=_check_interrupt,
                            renew_lease=_renew,
                            should_flush_memory=should_flush_memory,
                        )
            if asset.video_preview_path is None or asset.video_preview_path == "":
                data_dir = Path(get_config().data_dir)
                clip_path = data_dir / "video_clips" / asset.library.slug / str(asset.id) / "head_clip.mp4"
                if file_non_empty(clip_path):
                    self.asset_repo.set_video_preview_path(
                        asset.id, f"video_clips/{asset.library.slug}/{asset.id}/head_clip.mp4"
                    )
            if self._mode == "light":
                self.asset_repo.mark_analyzed_light(
                    asset.id, self.db_model_id, owned_by=self.worker_id
                )
            else:
                self.asset_repo.mark_completed(
                    asset.id, self.db_model_id, owned_by=self.worker_id
                )
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
            self.asset_repo.update_asset_status(
                asset.id, reset_status, owned_by=self.worker_id
            )
            return False
        except Exception as e:
            _log.error(
                "Video worker failed for asset %s (%s): %s",
                asset.id,
                asset.rel_path,
                e,
                exc_info=True,
            )
            msg = str(e)
            if asset.retry_count > MAX_RETRY_COUNT_BEFORE_POISON:
                self.asset_repo.update_asset_status(
                    asset.id, AssetStatus.poisoned, msg, owned_by=self.worker_id
                )
            else:
                self.asset_repo.update_asset_status(
                    asset.id, AssetStatus.failed, msg, owned_by=self.worker_id
                )
            return True
