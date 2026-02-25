"""AI worker: claims proxied assets, runs vision analysis, updates to completed or poisoned."""

import logging
import time

from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.ai.factory import get_vision_analyzer
from src.workers.base import BaseWorker

SUPPORTED_EXTS = [".jpg", ".jpeg", ".png", ".webp", ".bmp"]

_log = logging.getLogger(__name__)


class AIWorker(BaseWorker):
    """
    Worker that claims proxied assets, runs vision analysis on local proxies,
    saves visual analysis, and marks assets completed (or poisoned on error).
    """

    def __init__(
        self,
        worker_id: str,
        repository: WorkerRepository,
        heartbeat_interval_seconds: float = 15.0,
        *,
        asset_repo: AssetRepository,
        system_metadata_repo: SystemMetadataRepository,
        library_slug: str | None = None,
        verbose: bool = False,
        analyzer_name: str = "mock",
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
        )
        self.asset_repo = asset_repo
        self.storage = LocalMediaStore()
        self.analyzer = get_vision_analyzer(analyzer_name)
        self.db_model_id = self._system_metadata_repo.get_or_create_ai_model(
            self.analyzer.get_model_card()
        )
        self._library_slug = library_slug
        self._verbose = verbose

    def process_task(self) -> bool:
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.proxied,
            SUPPORTED_EXTS,
            library_slug=self._library_slug,
        )
        if asset is None:
            return False
        assert asset.id is not None
        try:
            started = time.perf_counter()
            proxy_path = self.storage.get_proxy_path(asset.library.slug, asset.id)
            results = self.analyzer.analyze_image(proxy_path)
            self._system_metadata_repo.save_visual_analysis(asset.id, results)
            self.asset_repo.mark_completed(asset.id, self.db_model_id)
            elapsed = time.perf_counter() - started
            if self._verbose:
                _log.info(
                    "Completed asset %s (%s/%s) in %.1fs",
                    asset.id,
                    asset.library.slug,
                    asset.rel_path,
                    elapsed,
                )
        except Exception as e:
            _log.error(
                "AI worker failed for asset %s (%s): %s",
                asset.id,
                asset.rel_path,
                e,
                exc_info=True,
            )
            self.asset_repo.update_asset_status(
                asset.id, AssetStatus.poisoned, str(e)
            )
        return True
