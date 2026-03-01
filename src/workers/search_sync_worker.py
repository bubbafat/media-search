"""Search sync worker: streams completed assets from PostgreSQL into Quickwit.

Append-only. Documents are never updated or deleted by this worker.
Index name per library is resolved from library_model_policy. If no policy
exists for a library, one is created and the index is created in Quickwit.

First-run behavior (no policy exists):
  - Generate a default index name: media_scenes_{library_slug}_{unix_timestamp}
  - upsert a new LibraryModelPolicy with active_index_name set to that name
  - create the Quickwit index from the schema file
  - Do NOT call begin_shadow_indexing — that is only for subsequent model
    upgrades, not for initial indexing. Calling it on first run would set
    locked=True on a new policy, which would cause the API to behave
    incorrectly in Stage 7.

Progress is tracked via system_metadata key 'search_sync_last_asset_id'.
"""
import logging
import time

from src.models.entities import Asset, AssetStatus, AssetType, LibraryModelPolicy
from src.repository.asset_repo import AssetRepository
from src.repository.library_model_policy_repo import LibraryModelPolicyRepository
from src.repository.quickwit_search_repo import QuickwitSearchRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)

BATCH_SIZE = 100
PROGRESS_KEY = "search_sync_last_asset_id"
SCHEMA_PATH = "quickwit/media_scenes_schema.json"


class SearchSyncWorker(BaseWorker):
    """Syncs completed assets and scenes from PostgreSQL to Quickwit."""

    def __init__(
        self,
        worker_id: str,
        repository: WorkerRepository,
        *,
        asset_repo: AssetRepository,
        scene_repo: VideoSceneRepository,
        policy_repo: LibraryModelPolicyRepository,
        quickwit_base_url: str,
        system_metadata_repo: SystemMetadataRepository,
        library_slug: str | None = None,
        heartbeat_interval_seconds: float = 15.0,
        idle_poll_interval_seconds: float = 30.0,
    ) -> None:
        super().__init__(
            worker_id,
            repository,
            heartbeat_interval_seconds,
            system_metadata_repo=system_metadata_repo,
            idle_poll_interval_seconds=idle_poll_interval_seconds,
        )
        self._asset_repo = asset_repo
        self._scene_repo = scene_repo
        self._policy_repo = policy_repo
        self._quickwit_base_url = quickwit_base_url
        self._library_slug = library_slug
        # QuickwitSearchRepository is instantiated with a placeholder
        # active_index_name. The actual index name is resolved per-asset
        # from library_model_policy at runtime.
        self._qw = QuickwitSearchRepository(
            base_url=quickwit_base_url,
            active_index_name="",
        )

    def process_task(self) -> bool:
        """Sync one batch of completed assets to Quickwit.

        Returns True if any documents were written, False if no work remained.
        """
        # 1. Read progress cursor
        raw = self._system_metadata_repo.get_value(PROGRESS_KEY)
        last_asset_id = int(raw) if raw else None

        if self._library_slug:
            _log.debug("[search-sync] Library: %s", self._library_slug)
        _log.debug("[search-sync] Last synced asset id: %s", last_asset_id or "none")

        # 2. Fetch batch
        assets = self._asset_repo.list_completed_assets_after(
            last_asset_id=last_asset_id,
            limit=BATCH_SIZE,
            library_slug=self._library_slug,
        )
        if not assets:
            _log.debug("[search-sync] No more assets. Sync complete.")
            return False

        min_id = min(a.id for a in assets)
        max_id = max(a.id for a in assets)
        _log.debug(
            "[search-sync] Fetched batch of %d assets (ids %s–%s)",
            len(assets),
            min_id,
            max_id,
        )

        # 3. Process each asset
        docs_written = 0
        for asset in assets:
            try:
                index_name = self._resolve_index(asset)
                written = self._index_asset(asset, index_name)
                docs_written += written
                if asset.type == AssetType.image:
                    _log.debug(
                        "[search-sync] Asset %s (image) → wrote 1 document to %s",
                        asset.id,
                        index_name,
                    )
                elif written > 0:
                    _log.debug(
                        "[search-sync] Asset %s (video) → %d scenes → wrote %d documents",
                        asset.id,
                        written,
                        written,
                    )
                else:
                    _log.debug(
                        "[search-sync] Asset %s (video) → 0 scenes → skipped",
                        asset.id,
                    )
            except Exception:
                _log.exception(
                    "Failed to index asset %s — skipping", asset.id
                )

        # 4. Advance cursor to the highest asset id in the batch
        self._system_metadata_repo.set_value(PROGRESS_KEY, str(max_id))

        _log.info(
            "[search-sync] Batch complete: %d assets, %d documents written, cursor → %s",
            len(assets),
            docs_written,
            max_id,
        )
        return True

    def get_heartbeat_stats(self) -> dict:
        raw = self._system_metadata_repo.get_value(PROGRESS_KEY)
        return {"last_synced_asset_id": raw or "none"}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_index(self, asset: Asset) -> str:
        """Return the Quickwit index name to write to for this asset's library.

        If no policy exists, creates one (upsert + create_index).
        Does NOT call begin_shadow_indexing on first run — that is only
        for model upgrade flows.
        """
        policy = self._policy_repo.get(asset.library_id)
        if policy is not None:
            return policy.active_index_name

        # First run for this library — create the index and policy
        index_name = f"media_scenes_{asset.library_id}_{int(time.time())}"
        _log.info(
            "No policy for library '%s' — creating index '%s'",
            asset.library_id,
            index_name,
        )
        self._qw.create_index(index_name, SCHEMA_PATH)
        self._policy_repo.upsert(
            LibraryModelPolicy(
                library_slug=asset.library_id,
                active_index_name=index_name,
                shadow_index_name=None,
                previous_index_name=None,
                locked=False,
                locked_since=None,
                promotion_progress=0.0,
            )
        )
        return index_name

    def _index_asset(self, asset: Asset, index_name: str) -> int:
        """Write Quickwit documents for one asset. Returns count written."""
        if asset.type == AssetType.video:
            return self._index_video(asset, index_name)
        return self._index_image(asset, index_name)

    def _index_video(self, asset: Asset, index_name: str) -> int:
        """Write one document per scene for a video asset."""
        scenes = self._scene_repo.list_scenes(asset.id)
        if not scenes:
            _log.warning(
                "Video asset %s has status=completed but no scenes — skipping",
                asset.id,
            )
            return 0

        now = int(time.time())
        for scene in scenes:
            # Extract AI analysis fields from scene metadata if present
            # list_scenes returns VideoSceneListItem with .metadata (DB column name)
            scene_meta = getattr(scene, "scene_metadata", None) or getattr(
                scene, "metadata", None
            )
            moondream = {}
            if scene_meta and isinstance(scene_meta, dict) and "moondream" in scene_meta:
                moondream = scene_meta["moondream"]

            doc = {
                "id": f"scene_{scene.id}",
                "scene_id": scene.id,
                "asset_id": asset.id,
                "library_slug": asset.library_id,
                "capture_ts": None,
                "country": None,
                "region": None,
                "city": None,
                "camera_make": None,
                "camera_model": None,
                "color_space": None,
                "generation_hint": None,
                "resolution_w": None,
                "resolution_h": None,
                "duration_sec": None,
                "frame_rate": None,
                "scene_start_ts": scene.start_ts,
                "scene_end_ts": scene.end_ts,
                "description": moondream.get("description") or scene.description,
                "ocr_text": moondream.get("ocr_text"),
                "tags": moondream.get("tags") or [],
                "rep_frame_path": scene.rep_frame_path,
                "head_clip_path": asset.video_preview_path,
                "preview_ready": asset.video_preview_path is not None,
                "playable": asset.video_preview_path is not None,
                "searchable": True,
                "offline_ready": False,
                "indexed_at": now,
            }
            self._qw.index_document(index_name, doc)
        return len(scenes)

    def _index_image(self, asset: Asset, index_name: str) -> int:
        """Write one document for an image asset."""
        analysis = asset.visual_analysis or {}
        now = int(time.time())

        doc = {
            "id": f"asset_{asset.id}",
            # scene_id explicitly 0 for images so the image filter
            # (NOT scene_id:[1 TO *]) works correctly in Quickwit.
            "scene_id": 0,
            "asset_id": asset.id,
            "library_slug": asset.library_id,
            "capture_ts": None,
            "country": None,
            "region": None,
            "city": None,
            "camera_make": None,
            "camera_model": None,
            "color_space": None,
            "generation_hint": None,
            "resolution_w": None,
            "resolution_h": None,
            "duration_sec": None,
            "frame_rate": None,
            "scene_start_ts": None,
            "scene_end_ts": None,
            "description": analysis.get("description"),
            "ocr_text": analysis.get("ocr_text"),
            "tags": analysis.get("tags") or [],
            "rep_frame_path": asset.preview_path,
            "head_clip_path": None,
            "preview_ready": asset.preview_path is not None,
            "playable": False,
            "searchable": True,
            "offline_ready": False,
            "indexed_at": now,
        }
        self._qw.index_document(index_name, doc)
        return 1
