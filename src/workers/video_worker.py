"""Video worker: claims pending video assets, runs scene indexing with lease renewal and fast-interrupts."""

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
from src.video.clip_extractor import extract_video_clip
from src.video.indexing import run_video_scene_indexing
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class VideoWorker(BaseWorker):
    """
    Worker that claims proxied video assets, runs scene indexing (pHash + temporal ceiling,
    best-frame selection, optional vision analysis), and marks assets completed or poisoned.
    Renews the asset lease after each closed scene and aborts cleanly on shutdown (InterruptedError).
    When system_default_model_id is set, only claims assets whose library's effective target
    model equals the worker's model.
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

    def run(self) -> None:
        """Run the normal worker loop."""
        super().run()

    def process_task(self) -> bool:
        claim_kwargs: dict = {"library_slug": self._library_slug}
        if self._system_default_model_id is not None:
            claim_kwargs["target_model_id"] = self.db_model_id
            claim_kwargs["system_default_model_id"] = self._system_default_model_id
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.proxied,
            VIDEO_EXTENSIONS_LIST,
            lease_seconds=300,
            **claim_kwargs,
        )
        if asset is None:
            return False
        assert asset.id is not None
        assert asset.library is not None
        source_path = Path(asset.library.absolute_path) / asset.rel_path

        def _renew() -> None:
            self.asset_repo.renew_asset_lease(asset.id, 300)

        def _check_interrupt() -> bool:
            return self.should_exit

        def _on_scene_saved(rep_path: Path, start_ts: float, end_ts: float) -> None:
            _log.info(
                "Scene %.1f-%.1fs -> %s",
                start_ts,
                end_ts,
                rep_path,
            )

        _log.info("Processing video: %s", source_path)
        try:
            run_video_scene_indexing(
                asset.id,
                source_path,
                asset.library.slug,
                self._scene_repo,
                vision_analyzer=self.analyzer,
                on_scene_closed=_renew,
                on_scene_saved=_on_scene_saved,
                check_interrupt=_check_interrupt,
            )
            data_dir = Path(get_config().data_dir)
            clip_path = data_dir / "video_clips" / asset.library.slug / str(asset.id) / "head_clip.mp4"
            clip_path.parent.mkdir(parents=True, exist_ok=True)
            if extract_video_clip(
                source_path, clip_path, start_ts=0.0, duration=10.0, context_seconds=0
            ):
                self.asset_repo.set_video_preview_path(
                    asset.id, f"video_clips/{asset.library.slug}/{asset.id}/head_clip.mp4"
                )
            self.asset_repo.mark_completed(asset.id, self.db_model_id)
            _log.info(
                "Completed: %s (%s/%s)",
                asset.id,
                asset.library.slug,
                asset.rel_path,
            )
            return True
        except InterruptedError:
            self.asset_repo.update_asset_status(asset.id, AssetStatus.proxied)
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
