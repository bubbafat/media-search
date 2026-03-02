from __future__ import annotations

"""MetadataWorker: EXIF extraction and sharpness/face metadata enrichment."""

import logging
from pathlib import Path
from typing import Literal

import cv2

from src.core.storage import LocalMediaStore
from src.metadata import exif_adapter
from src.metadata.face_detection import detect_faces
from src.metadata.normalization import normalize_media_metadata
from src.metadata.sharpness import compute_sharpness_from_array
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
    Phase "sharpness": runs sharpness scoring and face detection on thumbnails.
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
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
        )
        self._asset_repo = asset_repo
        self._phase: Literal["exif", "sharpness"] = phase
        self._batch_size = batch_size
        self._library_slug = library_slug
        self._storage = LocalMediaStore()

    def process_task(self) -> bool:
        if self._phase == "exif":
            return self._process_exif_batch()
        if self._phase == "sharpness":
            return self._process_sharpness_batch()
        raise RuntimeError(f"Unsupported metadata worker phase: {self._phase}")

    def _process_sharpness_batch(self) -> bool:
        """
        Claim a batch of assets for sharpness/face metadata, read thumbnails,
        compute sharpness and face count, then write results. Missing thumbnails
        are reset to exif_done.
        """
        asset_ids = self._asset_repo.claim_assets_for_sharpness_metadata(
            self._batch_size,
            library_slug=self._library_slug,
        )
        if not asset_ids:
            return False

        for asset_id in asset_ids:
            try:
                asset = self._asset_repo.get_asset_with_library_by_id(asset_id)
                if asset is None:
                    _log.warning(
                        "MetadataWorker sharpness: asset %s no longer exists; skipping",
                        asset_id,
                    )
                    continue
                library_id = asset.library_id
                if not self._storage.thumbnail_exists(library_id, asset_id):
                    self._asset_repo.reset_sharpness_processing_to_exif_done(asset_id)
                    _log.warning(
                        "MetadataWorker sharpness: thumbnail missing for asset_id=%s library_id=%s",
                        asset_id,
                        library_id,
                    )
                    continue
                thumb_path = self._storage._get_shard_path(
                    library_id, asset_id, "thumbnails", create_dirs=False
                )
                img_bgr = cv2.imread(str(thumb_path))
                if img_bgr is None:
                    self._asset_repo.reset_sharpness_processing_to_exif_done(asset_id)
                    _log.warning(
                        "MetadataWorker sharpness: could not read thumbnail for asset_id=%s",
                        asset_id,
                    )
                    continue
                sharpness_score = compute_sharpness_from_array(img_bgr)
                has_face, face_count = detect_faces(img_bgr)
                self._asset_repo.write_sharpness_metadata(
                    asset_id, has_face, face_count, sharpness_score
                )
                _log.info(
                    "Sharpness: processed asset %s (%s)",
                    asset_id,
                    asset.rel_path,
                )
            except Exception as e:  # noqa: BLE001
                _log.error(
                    "MetadataWorker sharpness phase failed for asset %s: %s",
                    asset_id,
                    e,
                    exc_info=True,
                )
                continue

        return True

    def _process_exif_batch(self) -> bool:
        """
        Claim a batch of assets for EXIF extraction and process them one by one.

        The claim operation is a short transaction that sets metadata_status to
        'exif_processing' and immediately releases row locks. EXIF extraction and
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
                _log.info(
                    "EXIF: processed asset %s (%s)",
                    asset_id,
                    asset.rel_path,
                )
            except Exception as e:  # noqa: BLE001
                # Leave the asset in 'exif_processing' for the recovery CLI to reset.
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

