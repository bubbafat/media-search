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
from src.video.indexing import run_video_scene_indexing
from src.video.preview import build_preview_webp
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class VideoWorker(BaseWorker):
    """
    Worker that claims pending video assets, runs scene indexing (pHash + temporal ceiling,
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
        repair: bool = False,
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
        self._repair = repair

    def _run_preview_repair_pass(self) -> None:
        """Build missing preview.webp from existing scene images (no reindex)."""
        data_dir = Path(get_config().data_dir)
        batch_size = 500
        offset = 0
        total_checked = 0
        total_built = 0
        while True:
            batch = self._scene_repo.get_asset_ids_with_scenes(
                library_slug=self._library_slug,
                limit=batch_size,
                offset=offset,
            )
            if not batch:
                break
            for asset_id, library_slug in batch:
                preview_path = data_dir / "video_scenes" / library_slug / str(asset_id) / "preview.webp"
                if preview_path.exists():
                    total_checked += 1
                    continue
                try:
                    built = build_preview_webp(asset_id, library_slug, self._scene_repo, data_dir)
                    if built is not None:
                        total_built += 1
                        relative = f"video_scenes/{library_slug}/{asset_id}/preview.webp"
                        self.asset_repo.set_preview_path(asset_id, relative)
                        _log.info("Preview: %s", built.resolve())
                except Exception as e:
                    _log.warning(
                        "Preview repair failed for asset %s (%s): %s",
                        asset_id,
                        library_slug,
                        e,
                        exc_info=True,
                    )
                total_checked += 1
            offset += len(batch)
            if len(batch) < batch_size:
                break
        if self._verbose or total_built:
            _log.info("Repair: checked %s, built %s", total_checked, total_built)

    def run(self) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_preview_repair_pass()
        super().run()

    def process_task(self) -> bool:
        claim_kwargs: dict = {"library_slug": self._library_slug}
        if self._system_default_model_id is not None:
            claim_kwargs["target_model_id"] = self.db_model_id
            claim_kwargs["system_default_model_id"] = self._system_default_model_id
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.pending,
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
            preview_path = run_video_scene_indexing(
                asset.id,
                source_path,
                asset.library.slug,
                self._scene_repo,
                vision_analyzer=self.analyzer,
                on_scene_closed=_renew,
                on_scene_saved=_on_scene_saved,
                check_interrupt=_check_interrupt,
            )
            self.asset_repo.mark_completed(asset.id, self.db_model_id)
            _log.info(
                "Completed: %s (%s/%s)",
                asset.id,
                asset.library.slug,
                asset.rel_path,
            )
            if preview_path is not None:
                relative = f"video_scenes/{asset.library.slug}/{asset.id}/preview.webp"
                self.asset_repo.set_preview_path(asset.id, relative)
                _log.info("Preview: %s", preview_path.resolve())
            return True
        except InterruptedError:
            self.asset_repo.update_asset_status(asset.id, AssetStatus.pending)
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
