"""AI worker: claims proxied assets, runs vision analysis, updates to completed or poisoned."""

import logging
import time

from src.ai.factory import get_vision_analyzer
from src.core.file_extensions import IMAGE_EXTENSIONS_LIST
from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.library_repo import LibraryRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class AIWorker(BaseWorker):
    """
    Worker that claims proxied assets, runs vision analysis on local proxies,
    saves visual analysis, and marks assets completed (or poisoned on error).
    When system_default_model_id is set, only claims assets whose library's
    effective target model (COALESCE(library.target_tagger_id, system_default)) equals
    the worker's model.
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
        system_default_model_id: int | None = None,
        repair: bool = False,
        library_repo: LibraryRepository | None = None,
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
        self._system_default_model_id = system_default_model_id
        self._repair = repair
        self._library_repo = library_repo

    def _run_repair_pass(self) -> None:
        """
        Find assets that were analyzed with a different model than the library's
        effective target; set their status to proxied so they are re-claimed and re-analyzed.
        """
        if self._library_repo is None:
            _log.warning("Repair requested but no library_repo; skipping repair pass.")
            return
        system_default_id = self._system_default_model_id
        if system_default_id is None:
            _log.warning("Repair requested but system default model unset; skipping repair pass.")
            return
        libraries: list[tuple[str, int]] = []
        if self._library_slug is not None:
            lib = self._library_repo.get_by_slug(self._library_slug)
            if lib is None:
                return
            effective = lib.target_tagger_id if lib.target_tagger_id is not None else system_default_id
            libraries.append((lib.slug, effective))
        else:
            for lib in self._library_repo.list_libraries(include_deleted=False):
                effective = lib.target_tagger_id if lib.target_tagger_id is not None else system_default_id
                libraries.append((lib.slug, effective))
        batch_size = 500
        total_reset = 0
        for library_slug, effective_id in libraries:
            offset = 0
            while True:
                batch = self.asset_repo.get_asset_ids_expecting_reanalysis(
                    effective_target_model_id=effective_id,
                    library_slug=library_slug,
                    limit=batch_size,
                    offset=offset,
                )
                if not batch:
                    break
                for asset_id, _ in batch:
                    self.asset_repo.update_asset_status(asset_id, AssetStatus.proxied)
                    total_reset += 1
                offset += len(batch)
                if len(batch) < batch_size:
                    break
        if self._verbose or total_reset:
            _log.info("AI repair: set %s assets to proxied for re-analysis", total_reset)

    def run(self) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_repair_pass()
        super().run()

    def process_task(self) -> bool:
        claim_kwargs: dict = {"library_slug": self._library_slug}
        if self._system_default_model_id is not None:
            claim_kwargs["target_model_id"] = self.db_model_id
            claim_kwargs["system_default_model_id"] = self._system_default_model_id
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.proxied,
            IMAGE_EXTENSIONS_LIST,
            **claim_kwargs,
        )
        if asset is None:
            return False
        assert asset.id is not None
        card = self.analyzer.get_model_card()
        try:
            started = time.perf_counter()
            proxy_path = self.storage.get_proxy_path(asset.library.slug, asset.id)
            results = self.analyzer.analyze_image(proxy_path)
            self._system_metadata_repo.save_visual_analysis(
                asset.id,
                results,
                model_name=card.name,
                model_version=card.version,
            )
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
