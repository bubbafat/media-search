"""Proxy worker: claims pending image assets, generates thumbnails and WebP proxies on local SSD, updates to proxied."""

import logging

from src.core.file_extensions import IMAGE_EXTENSIONS_LIST
from src.core.path_resolver import resolve_path
from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker
from src.workers.constants import MAX_RETRY_COUNT_BEFORE_POISON

_log = logging.getLogger(__name__)


class ImageProxyWorker(BaseWorker):
    """
    Worker that claims pending image assets only, writes thumbnails (JPEG) and proxies (WebP)
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
        use_previews: bool = True,
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
        self._global_mode = library_slug is None
        self._verbose = verbose
        self._initial_pending = initial_pending_count
        self._processed_count = 0
        self._repair = repair
        self._use_previews = use_previews

    def _run_repair_pass(self) -> None:
        """Find image assets that should have proxy/thumbnail but are missing; set status to pending."""
        batch_size = 500
        offset = 0
        total_checked = 0
        total_reset = 0
        while True:
            batch = self.asset_repo.get_asset_ids_expecting_proxy(
                library_slug=self._library_slug,
                limit=batch_size,
                offset=offset,
                global_scope=self._global_mode,
            )
            if not batch:
                break
            for asset_id, library_slug, type_str in batch:
                if type_str != "image":
                    continue
                if not self.storage.proxy_and_thumbnail_exist(library_slug, asset_id):
                    self.asset_repo.update_asset_status(asset_id, AssetStatus.pending)
                    total_reset += 1
                total_checked += 1
            offset += len(batch)
            if len(batch) < batch_size:
                break
        if self._verbose or total_reset or total_checked:
            _log.info(
                "Repair: checked %s image assets (proxied/completed), reset %s to pending",
                total_checked,
                total_reset,
            )

    def run(self, once: bool = False) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_repair_pass()
            if self._verbose:
                self._initial_pending = self.asset_repo.count_pending_proxyable(
                    self._library_slug, global_scope=self._global_mode
                )
        super().run(once=once)

    def process_task(self) -> bool:
        if self._library_slug is None and not self._global_mode:
            raise RuntimeError("Worker scope is ambiguous: library_slug is None but global_mode is False.")
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.pending,
            IMAGE_EXTENSIONS_LIST,
            library_slug=self._library_slug,
            global_scope=self._global_mode,
        )
        if asset is None:
            asset = self.asset_repo.claim_asset_by_status(
                self.worker_id,
                AssetStatus.failed,
                IMAGE_EXTENSIONS_LIST,
                library_slug=self._library_slug,
                global_scope=self._global_mode,
            )
        if asset is None:
            return False
        assert asset.id is not None
        try:
            source_path = resolve_path(asset.library.slug, asset.rel_path)
        except (ValueError, FileNotFoundError) as e:
            _log.error(
                "Image proxy worker path resolution failed for asset %s (%s): %s",
                asset.id,
                asset.rel_path,
                e,
            )
            self.asset_repo.update_asset_status(
                asset.id, AssetStatus.poisoned, str(e), owned_by=self.worker_id
            )
            return True
        try:
            # Cascade: generate proxy first, then thumbnail from that proxy image,
            # using a pyvips-first pipeline with shrink-on-load where available.
            self.storage.generate_proxy_and_thumbnail_from_source(
                asset.library.slug,
                asset.id,
                source_path,
                use_previews=self._use_previews,
            )
            if not self.asset_repo.update_asset_status(
                asset.id, AssetStatus.proxied, owned_by=self.worker_id
            ):
                _log.info(
                    "Asset %s was evicted (scanner reset or lease reclaimed); skipping completion update",
                    asset.id,
                )
                return True
            self._processed_count += 1
            if self._verbose:
                total = self._initial_pending if self._initial_pending is not None else "?"
                _log.info(
                    "Proxied asset %s (%s) %s/%s",
                    asset.id,
                    asset.rel_path,
                    self._processed_count,
                    total,
                )
        except Exception as e:
            _log.error(
                "Image proxy worker failed for asset %s (%s): %s",
                asset.id,
                source_path,
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


# Backward compatibility: old name referred to the combined image+video worker.
ProxyWorker = ImageProxyWorker
