"""Proxy worker: claims pending assets, generates thumbnails and proxies on local SSD, updates to proxied."""

import logging
from pathlib import Path

from src.core.file_extensions import IMAGE_EXTENSIONS_LIST
from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class ProxyWorker(BaseWorker):
    """
    Worker that claims pending assets, loads source images, writes thumbnail and proxy
    to the local sharded store, and updates status to proxied (or poisoned on error).
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
        self.storage = LocalMediaStore()
        self._library_slug = library_slug
        self._verbose = verbose
        self._initial_pending = initial_pending_count
        self._processed_count = 0
        self._repair = repair

    def _run_repair_pass(self) -> None:
        """Find assets that should have proxy/thumbnail but are missing; set status to pending."""
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
            for asset_id, library_slug in batch:
                if not self.storage.proxy_and_thumbnail_exist(library_slug, asset_id):
                    self.asset_repo.update_asset_status(asset_id, AssetStatus.pending)
                    total_reset += 1
                total_checked += 1
            offset += len(batch)
            if len(batch) < batch_size:
                break
        if self._verbose or total_reset:
            _log.info(
                "Repair: checked %s, reset %s to pending",
                total_checked,
                total_reset,
            )

    def run(self) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_repair_pass()
            if self._verbose:
                self._initial_pending = self.asset_repo.count_pending_proxyable(self._library_slug)
        super().run()

    def process_task(self) -> bool:
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.pending,
            IMAGE_EXTENSIONS_LIST,
            library_slug=self._library_slug,
        )
        if asset is None:
            return False
        assert asset.id is not None
        source_path = Path(asset.library.absolute_path) / asset.rel_path
        try:
            image = self.storage.load_source_image(source_path)
            self.storage.save_thumbnail(asset.library.slug, asset.id, image)
            self.storage.save_proxy(asset.library.slug, asset.id, image)
            self.asset_repo.update_asset_status(asset.id, AssetStatus.proxied)
            self._processed_count += 1
            if self._verbose:
                total = self._initial_pending if self._initial_pending is not None else "?"
                _log.info(
                    "Proxied asset %s (%s/%s) %s/%s",
                    asset.id,
                    asset.library.slug,
                    asset.rel_path,
                    self._processed_count,
                    total,
                )
        except Exception as e:
            _log.error(
                "Proxy worker failed for asset %s (%s): %s",
                asset.id,
                source_path,
                e,
                exc_info=True,
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, str(e))
        return True
