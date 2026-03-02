from __future__ import annotations

"""MetadataWorker: EXIF extraction and metadata enrichment."""

import logging
from pathlib import Path
from typing import Literal

from src.metadata import exif_adapter
from src.metadata.normalization import normalize_media_metadata
from src.models.entities import Asset
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)


class MetadataWorker(BaseWorker):
    """
    Worker that enriches assets with EXIF and derived media metadata.

    Phase "exif": runs EXIF extraction and normalization.
    Phase "sharpness": reserved for future sharpness scoring (not yet implemented).
    """

    def __init__(
        self,
        worker_id: str,
        repository: WorkerRepository,
        heartbeat_interval_seconds: float = 15.0,
        *,
        asset_repo: AssetRepository,
        system_metadata_repo: SystemMetadataRepository,
        phase: Literal["exif", "sharpness"],
        batch_size: int,
        library_slug: str | None = None,
        idle_poll_interval_seconds: float = 5.0,
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
            idle_poll_interval_seconds=idle_poll_interval_seconds,
        )
        self._asset_repo = asset_repo
        self._phase: Literal["exif", "sharpness"] = phase
        self._batch_size = batch_size
        self._library_slug = library_slug

    def process_task(self) -> bool:
        if self._phase == "exif":
            return self._process_exif_batch()
        if self._phase == "sharpness":
            return self._process_sharpness_batch()
        raise RuntimeError(f"Unsupported metadata worker phase: {self._phase}")

    def _process_sharpness_batch(self) -> bool:
        """Placeholder for sharpness scoring phase (A3 workstream)."""
        raise NotImplementedError("Sharpness phase is not yet implemented.")

    def _process_exif_batch(self) -> bool:
        """
        Claim a batch of assets for EXIF extraction and process them one by one.

        The claim operation is a short transaction that sets metadata_status to
        'processing' and immediately releases row locks. EXIF extraction and
        normalization are performed outside any transaction; completion is then
        written in a second short transaction.
        """
        asset_ids = self._asset_repo.claim_assets_for_exif_metadata(
            self._batch_size,
            library_slug=self._library_slug,
        )
        if not asset_ids:
            return False

        for asset_id in asset_ids:
            try:
                asset = self._asset_repo.get_asset_with_library_by_id(asset_id)
                if asset is None:
                    _log.warning("MetadataWorker: asset %s no longer exists; skipping", asset_id)
                    continue
                lib_root = Path(asset.library.absolute_path)
                path = lib_root / asset.rel_path

                raw_exif = exif_adapter.read_metadata(path)
                # Ensure we pass an Asset instance with the fields normalize_media_metadata expects.
                assert isinstance(asset, Asset)
                media_metadata = normalize_media_metadata(raw_exif, asset=asset)
                self._asset_repo.write_exif_metadata(asset_id, raw_exif, media_metadata)
            except Exception as e:  # noqa: BLE001
                # Leave the asset in 'processing' for the recovery CLI to reset.
                try:
                    rel = asset.rel_path if "asset" in locals() and asset is not None else "?"
                    _log.error(
                        "MetadataWorker EXIF phase failed for asset %s (%s): %s",
                        asset_id,
                        rel,
                        e,
                        exc_info=True,
                    )
                except Exception:
                    _log.error(
                        "MetadataWorker EXIF phase failed for asset %s: %s",
                        asset_id,
                        e,
                        exc_info=True,
                    )
                continue

        return True

