"""Video proxy worker: claims pending video assets, 720p pipeline (thumbnail, head-clip, scene indexing), updates to proxied."""

import logging
import tempfile
from pathlib import Path

from src.core.config import get_config
from src.core.file_extensions import VIDEO_EXTENSIONS_LIST
from src.core.storage import LocalMediaStore
from src.models.entities import AssetStatus
from src.repository.asset_repo import AssetRepository
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.video_scene_repo import VideoSceneRepository
from src.repository.worker_repo import WorkerRepository
from src.video.clip_extractor import (
    FFmpegAttempt,
    extract_head_clip_copy_detailed,
    extract_video_frame_detailed,
    probe_video_duration,
    transcode_to_720p_h264_detailed,
)
from src.video.indexing import run_video_scene_indexing
from src.workers.base import BaseWorker

_log = logging.getLogger(__name__)

_MAX_RETRY_COUNT_BEFORE_POISON = 5


class _PermanentVideoProxyError(RuntimeError):
    pass


class _RetryableVideoProxyError(RuntimeError):
    pass


def _format_ffmpeg_attempts(label: str, attempts: list[FFmpegAttempt]) -> str:
    lines: list[str] = [label]
    if not attempts:
        return label
    for i, a in enumerate(attempts, start=1):
        lines.append(f"Attempt {i}: Repro: {a.repro}")
        tail = a.stderr_tail()
        if tail:
            lines.append(f"Attempt {i}: FFmpeg stderr tail:\n{tail}")
    return "\n".join(lines).strip()


def _format_ffmpeg_attempt(label: str, attempt: FFmpegAttempt) -> str:
    tail = attempt.stderr_tail()
    if tail:
        return f"{label}\nRepro: {attempt.repro}\nFFmpeg stderr tail:\n{tail}"
    return f"{label}\nRepro: {attempt.repro}"


class VideoProxyWorker(BaseWorker):
    """
    Worker that claims pending video assets, runs the 720p disposable pipeline:
    transcode to temp 720p H.264 → thumbnail from temp → head-clip (stream copy) →
    scene indexing (pHash, rep frames, no vision) → cleanup temp → set proxied.
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
        self.scene_repo = scene_repo
        self.storage = LocalMediaStore()
        self._library_slug = library_slug
        self._verbose = verbose
        self._initial_pending = initial_pending_count
        self._processed_count = 0
        self._repair = repair
        self._current_asset_id: int | None = None
        self._current_asset_rel_path: str | None = None
        self._current_stage: str | None = None
        self._current_stage_progress: float | None = None

    def _head_clip_path(self, library_slug: str, asset_id: int) -> Path:
        """Path to head_clip.mp4 for this asset (under data_dir)."""
        data_dir = Path(get_config().data_dir)
        return data_dir / "video_clips" / library_slug / str(asset_id) / "head_clip.mp4"

    def _run_repair_pass(self) -> None:
        """Find video assets that should have thumbnail and head-clip but are missing; set status to pending."""
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
            for asset_id, library_slug, type_str in batch:
                if type_str != "video":
                    continue
                missing = False
                if not self.storage.thumbnail_exists(library_slug, asset_id):
                    missing = True
                if not self._head_clip_path(library_slug, asset_id).exists():
                    missing = True
                if missing:
                    self.asset_repo.update_asset_status(asset_id, AssetStatus.pending)
                    total_reset += 1
                total_checked += 1
            offset += len(batch)
            if len(batch) < batch_size:
                break
        if self._verbose or total_reset:
            _log.info(
                "Repair: checked %s videos, reset %s to pending",
                total_checked,
                total_reset,
            )

    def run(self, once: bool = False) -> None:
        """Run repair pass once if --repair, then the normal worker loop."""
        if self._repair:
            self._run_repair_pass()
            if self._verbose:
                self._initial_pending = self.asset_repo.count_pending_proxyable(self._library_slug)
        super().run(once=once)

    def get_heartbeat_stats(self) -> dict[str, object] | None:
        """Include current asset, stage, and progress for observability."""
        if self._current_asset_id is None:
            return None
        stats: dict[str, object] = {
            "current_asset_id": self._current_asset_id,
            "current_asset_rel_path": self._current_asset_rel_path or "",
            "current_stage": self._current_stage or "",
        }
        if self._current_stage_progress is not None:
            stats["current_stage_progress"] = self._current_stage_progress
        return stats

    def process_task(self) -> bool:
        asset = self.asset_repo.claim_asset_by_status(
            self.worker_id,
            AssetStatus.pending,
            VIDEO_EXTENSIONS_LIST,
            library_slug=self._library_slug,
        )
        if asset is None:
            asset = self.asset_repo.claim_asset_by_status(
                self.worker_id,
                AssetStatus.failed,
                VIDEO_EXTENSIONS_LIST,
                library_slug=self._library_slug,
            )
        if asset is None:
            return False
        assert asset.id is not None
        assert asset.library is not None
        self._current_asset_id = asset.id
        self._current_asset_rel_path = asset.rel_path
        self._current_stage = "claimed"
        self._current_stage_progress = None
        _log.info(
            "Starting video proxy pipeline for asset %s (%s)",
            asset.id,
            asset.rel_path,
        )
        library_slug = asset.library.slug
        source_path = Path(asset.library.absolute_path) / asset.rel_path
        data_dir = Path(get_config().data_dir)
        tmp_dir = data_dir / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        temp_fd = tempfile.NamedTemporaryFile(
            suffix=".mp4", dir=str(tmp_dir), delete=False
        )
        temp_path = Path(temp_fd.name)
        temp_fd.close()
        try:
            if asset.retry_count > _MAX_RETRY_COUNT_BEFORE_POISON:
                raise _PermanentVideoProxyError(
                    f"Retry limit exceeded (retry_count={asset.retry_count} > {_MAX_RETRY_COUNT_BEFORE_POISON})"
                )

            self._current_stage = "transcode"
            self._current_stage_progress = 0.0
            _log.info(
                "Starting 720p transcode for asset %s (%s)",
                asset.id,
                source_path,
            )
            duration = probe_video_duration(source_path)

            last_reported_percent: float | None = None

            def _on_progress(p: float) -> None:
                # p is in [0.0, 1.0]
                nonlocal last_reported_percent
                self._current_stage_progress = p
                if last_reported_percent is None or (p - last_reported_percent) >= 0.05:
                    last_reported_percent = p
                    pct = int(p * 100)
                    _log.info(
                        "[asset %s] %s%% complete (720p transcode)",
                        asset.id,
                        pct,
                    )

            transcode_attempts = transcode_to_720p_h264_detailed(
                source_path,
                temp_path,
                duration=duration,
                on_progress=_on_progress if duration is not None else None,
            )
            if not transcode_attempts or not transcode_attempts[-1].ok:
                raise _PermanentVideoProxyError(
                    _format_ffmpeg_attempts("720p transcode failed", transcode_attempts)
                )
            self._current_stage = "thumbnail"
            self._current_stage_progress = None
            _log.info(
                "Extracting thumbnail at t=0.0s for asset %s (%s)",
                asset.id,
                temp_path,
            )
            thumb_path = self.storage.get_thumbnail_write_path(library_slug, asset.id)
            frame_attempt = extract_video_frame_detailed(temp_path, thumb_path, 0.0)
            if not frame_attempt.ok:
                raise _RetryableVideoProxyError(
                    _format_ffmpeg_attempt("FFmpeg frame extraction failed", frame_attempt)
                )
            self._current_stage = "head_clip"
            self._current_stage_progress = None
            _log.info(
                "Extracting 10s head clip for asset %s (%s)",
                asset.id,
                temp_path,
            )
            head_clip_path = self._head_clip_path(library_slug, asset.id)
            head_attempt = extract_head_clip_copy_detailed(
                temp_path, head_clip_path, duration=10.0
            )
            if not head_attempt.ok:
                raise _RetryableVideoProxyError(
                    _format_ffmpeg_attempt("Head-clip copy failed", head_attempt)
                )
            self._current_stage = "scene_indexing"
            self._current_stage_progress = None
            _log.info(
                "Running scene indexing for asset %s (%s)",
                asset.id,
                temp_path,
            )
            run_video_scene_indexing(
                asset.id,
                temp_path,
                library_slug,
                self.scene_repo,
                vision_analyzer=None,
                check_interrupt=lambda: self.should_exit,
            )
            self.asset_repo.set_video_preview_path(
                asset.id, f"video_clips/{library_slug}/{asset.id}/head_clip.mp4"
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.proxied)
            self._processed_count += 1
            self._current_stage = "completed"
            self._current_stage_progress = 1.0
            if self._verbose:
                total = self._initial_pending if self._initial_pending is not None else "?"
                _log.info(
                    "Proxied video %s (%s) %s/%s",
                    asset.id,
                    asset.rel_path,
                    self._processed_count,
                    total,
                )
        except InterruptedError:
            self.asset_repo.update_asset_status(asset.id, AssetStatus.pending)
            return False
        except _PermanentVideoProxyError as e:
            _log.error(
                "Video proxy worker permanent failure for asset %s (%s): %s",
                asset.id,
                source_path,
                e,
            )
            self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, str(e))
        except _RetryableVideoProxyError as e:
            # Retryable: mark failed unless we exceeded retry limit.
            msg = str(e)
            if asset.retry_count > _MAX_RETRY_COUNT_BEFORE_POISON:
                msg = (
                    f"{msg}\n\nRetry limit exceeded (retry_count={asset.retry_count} > {_MAX_RETRY_COUNT_BEFORE_POISON})"
                )
                self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, msg)
            else:
                self.asset_repo.update_asset_status(asset.id, AssetStatus.failed, msg)
        except Exception as e:
            _log.error(
                "Video proxy worker failed for asset %s (%s): %s",
                asset.id,
                source_path,
                e,
                exc_info=True,
            )
            msg = str(e)
            if "No frames produced by decoder" in msg or "ffprobe returned no stream" in msg:
                self.asset_repo.update_asset_status(asset.id, AssetStatus.poisoned, msg)
            elif asset.retry_count > _MAX_RETRY_COUNT_BEFORE_POISON:
                self.asset_repo.update_asset_status(
                    asset.id,
                    AssetStatus.poisoned,
                    f"{msg}\n\nRetry limit exceeded (retry_count={asset.retry_count} > {_MAX_RETRY_COUNT_BEFORE_POISON})",
                )
            else:
                self.asset_repo.update_asset_status(asset.id, AssetStatus.failed, msg)
        finally:
            temp_path.unlink(missing_ok=True)
            self._current_asset_id = None
            self._current_asset_rel_path = None
            self._current_stage = None
            self._current_stage_progress = None
        return True
