"""Proxy worker: claims pending assets, generates thumbnails and proxies on local SSD, updates to proxied."""

import logging
from pathlib import Path

from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker

SUPPORTED_EXTS = [".jpg", ".jpeg", ".png", ".webp", ".bmp"]

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
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
        )
        self.asset_repo = asset_repo
        self.storage = LocalMediaStore()

    def process_task(self) -> None:
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id, AssetStatus.pending, SUPPORTED_EXTS
        )
        if asset is None:
            return
        assert asset.id is not None
        source_path = Path(asset.library.absolute_path) / asset.rel_path
        try:
            image = self.storage.load_source_image(source_path)
            self.storage.save_thumbnail(asset.library.slug, asset.id, image)
            self.storage.save_proxy(asset.library.slug, asset.id, image)
            self.asset_repo.update_asset_status(asset.id, AssetStatus.proxied)
        except Exception as e:
            _log.error(
                "Proxy worker failed for asset %s (%s): %s",
                asset.id,
                source_path,
                e,
                exc_info=True,
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, str(e))
