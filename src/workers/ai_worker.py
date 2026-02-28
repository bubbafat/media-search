"""AI worker: claims proxied assets, runs vision analysis, updates to completed or poisoned."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ai.factory import get_vision_analyzer
from src.core.file_extensions import IMAGE_EXTENSIONS_LIST
from src.core.storage import LocalMediaStore
from src.models.entities import Asset, AssetStatus
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
        batch_size: int = 1,
        mode: str = "full",
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
        self._global_mode = library_slug is None
        self._verbose = verbose
        self._system_default_model_id = system_default_model_id
        self._repair = repair
        self._library_repo = library_repo
        self._batch_size = max(1, batch_size)
        self._mode = mode

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

    def run(self, once: bool = False) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_repair_pass()
        super().run(once=once)

    def process_task(self) -> bool:
        if self._library_slug is None and not self._global_mode:
            raise RuntimeError("Worker scope is ambiguous: library_slug is None but global_mode is False.")
        claim_status = (
            AssetStatus.proxied if self._mode == "light" else AssetStatus.analyzed_light
        )
        claim_kwargs: dict = {
            "library_slug": self._library_slug,
            "global_scope": self._global_mode,
            "limit": self._batch_size,
        }
        if self._system_default_model_id is not None:
            claim_kwargs["target_model_id"] = self.db_model_id
            claim_kwargs["system_default_model_id"] = self._system_default_model_id
        assets = self.asset_repo.claim_assets_by_status(
            self.worker_id,
            claim_status,
            IMAGE_EXTENSIONS_LIST,
            **claim_kwargs,
        )
        if not assets:
            return False
        card = self.analyzer.get_model_card()
        mode = self._mode

        def _process_one(asset: Asset) -> tuple[int, str, str, Exception | None]:
            """Run analyze_image for one asset. Returns (asset_id, slug, rel_path, error or None)."""
            assert asset.id is not None
            try:
                proxy_path = self.storage.get_proxy_path(asset.library.slug, asset.id)
                active_count = self._repo.get_active_local_worker_count(
                    self.hostname, self.worker_id
                )
                should_flush_memory = active_count > 0
                results = self.analyzer.analyze_image(
                    proxy_path, mode=mode, should_flush_memory=should_flush_memory
                )
                if mode == "light":
                    self._system_metadata_repo.save_visual_analysis(
                        asset.id,
                        results,
                        model_name=card.name,
                        model_version=card.version,
                    )
                    self.asset_repo.mark_analyzed_light(
                        asset.id, self.db_model_id, owned_by=self.worker_id
                    )
                else:
                    self._system_metadata_repo.merge_ocr_into_visual_analysis(
                        asset.id, results.ocr_text
                    )
                    self.asset_repo.mark_completed(
                        asset.id, self.db_model_id, owned_by=self.worker_id
                    )
                return (asset.id, asset.library.slug, asset.rel_path, None)
            except Exception as e:
                return (asset.id, asset.library.slug, asset.rel_path, e)

        started = time.perf_counter()
        completed = 0
        failed = 0
        with ThreadPoolExecutor(max_workers=len(assets)) as executor:
            futures = {executor.submit(_process_one, a): a for a in assets}
            for future in as_completed(futures):
                asset_id, slug, rel_path, err = future.result()
                if err is None:
                    completed += 1
                    if self._verbose:
                        _log.info("Completed asset %s (%s/%s)", asset_id, slug, rel_path)
                else:
                    failed += 1
                    _log.error(
                        "AI worker failed for asset %s (%s): %s",
                        asset_id,
                        rel_path,
                        err,
                        exc_info=True,
                    )
                    self.asset_repo.update_asset_status(
                        asset_id, AssetStatus.poisoned, str(err), owned_by=self.worker_id
                    )
        elapsed = time.perf_counter() - started
        if self._verbose and completed:
            _log.info(
                "Batch: %s completed, %s failed in %.1fs",
                completed,
                failed,
                elapsed,
            )
        return True
