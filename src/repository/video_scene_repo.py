"""Video scene and active state repository: resume state, save scene + UPSERT/delete state in one transaction."""

import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class VideoSceneRow:
    """Data for one closed scene (insert into video_scenes)."""

    start_ts: float
    end_ts: float
    description: str | None
    metadata: dict[str, Any] | None
    sharpness_score: float
    rep_frame_path: str
    keep_reason: str  # "phash" | "temporal" | "forced"


@dataclass(frozen=True)
class VideoActiveState:
    """One row of video_active_state (resume state for an asset)."""

    anchor_phash: str
    scene_start_ts: float
    current_best_pts: float
    current_best_sharpness: float


class VideoSceneRepository:
    """
    Database access for video_scenes and video_active_state.

    Uses a single transaction for save_scene_and_update_state: INSERT scene then
    UPSERT or DELETE active_state so there are no orphaned state rows.
    """

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self, write: bool = False) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
            if write:
                session.commit()
        finally:
            session.close()

    def get_max_end_ts(self, asset_id: int) -> float | None:
        """Return max(end_ts) for the asset from video_scenes, or None if no rows."""
        with self._session_scope(write=False) as session:
            row = session.execute(
                text("SELECT max(end_ts) FROM video_scenes WHERE asset_id = :asset_id"),
                {"asset_id": asset_id},
            ).fetchone()
            if row is None or row[0] is None:
                return None
            return float(row[0])

    def get_last_scene_description(self, asset_id: int) -> str | None:
        """Return the description of the most recent scene (max end_ts) for the asset, for deduplication."""
        with self._session_scope(write=False) as session:
            row = session.execute(
                text(
                    "SELECT description FROM video_scenes WHERE asset_id = :asset_id "
                    "ORDER BY end_ts DESC LIMIT 1"
                ),
                {"asset_id": asset_id},
            ).fetchone()
            if row is None or row[0] is None:
                return None
            return str(row[0])

    def upsert_active_state(self, asset_id: int, state: VideoActiveState) -> None:
        """UPSERT a single row into video_active_state (e.g. when closing a scene with no best frame)."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("""
                    INSERT INTO video_active_state (
                        asset_id, anchor_phash, scene_start_ts,
                        current_best_pts, current_best_sharpness
                    )
                    VALUES (
                        :asset_id, :anchor_phash, :scene_start_ts,
                        :current_best_pts, :current_best_sharpness
                    )
                    ON CONFLICT (asset_id) DO UPDATE SET
                        anchor_phash = EXCLUDED.anchor_phash,
                        scene_start_ts = EXCLUDED.scene_start_ts,
                        current_best_pts = EXCLUDED.current_best_pts,
                        current_best_sharpness = EXCLUDED.current_best_sharpness
                """),
                {
                    "asset_id": asset_id,
                    "anchor_phash": state.anchor_phash,
                    "scene_start_ts": state.scene_start_ts,
                    "current_best_pts": state.current_best_pts,
                    "current_best_sharpness": state.current_best_sharpness,
                },
            )

    def delete_active_state(self, asset_id: int) -> None:
        """Remove the video_active_state row for the asset (e.g. EOF with no scene to persist)."""
        with self._session_scope(write=True) as session:
            session.execute(
                text("DELETE FROM video_active_state WHERE asset_id = :asset_id"),
                {"asset_id": asset_id},
            )

    def get_active_state(self, asset_id: int) -> VideoActiveState | None:
        """Load the video_active_state row for the asset, or None."""
        with self._session_scope(write=False) as session:
            row = session.execute(
                text(
                    "SELECT anchor_phash, scene_start_ts, current_best_pts, current_best_sharpness "
                    "FROM video_active_state WHERE asset_id = :asset_id"
                ),
                {"asset_id": asset_id},
            ).fetchone()
            if row is None:
                return None
            return VideoActiveState(
                anchor_phash=row[0],
                scene_start_ts=float(row[1]),
                current_best_pts=float(row[2]),
                current_best_sharpness=float(row[3]),
            )

    def save_scene_and_update_state(
        self,
        asset_id: int,
        scene: VideoSceneRow,
        active_state: VideoActiveState | None,
    ) -> int:
        """
        In one transaction: INSERT into video_scenes; then if active_state is not None,
        UPSERT video_active_state (ON CONFLICT (asset_id) DO UPDATE); else DELETE from
        video_active_state for this asset. Returns the inserted video_scenes.id.
        """
        with self._session_scope(write=True) as session:
            metadata_json = json.dumps(scene.metadata) if scene.metadata is not None else None
            result = session.execute(
                text("""
                    INSERT INTO video_scenes (
                        asset_id, start_ts, end_ts, description, metadata,
                        sharpness_score, rep_frame_path, keep_reason
                    )
                    VALUES (
                        :asset_id, :start_ts, :end_ts, :description, CAST(:metadata AS jsonb),
                        :sharpness_score, :rep_frame_path, :keep_reason
                    )
                    RETURNING id
                """),
                {
                    "asset_id": asset_id,
                    "start_ts": scene.start_ts,
                    "end_ts": scene.end_ts,
                    "description": scene.description,
                    "metadata": metadata_json,
                    "sharpness_score": scene.sharpness_score,
                    "rep_frame_path": scene.rep_frame_path,
                    "keep_reason": scene.keep_reason,
                },
            )
            scene_id = result.scalar_one()

            if active_state is not None:
                session.execute(
                    text("""
                        INSERT INTO video_active_state (
                            asset_id, anchor_phash, scene_start_ts,
                            current_best_pts, current_best_sharpness
                        )
                        VALUES (
                            :asset_id, :anchor_phash, :scene_start_ts,
                            :current_best_pts, :current_best_sharpness
                        )
                        ON CONFLICT (asset_id) DO UPDATE SET
                            anchor_phash = EXCLUDED.anchor_phash,
                            scene_start_ts = EXCLUDED.scene_start_ts,
                            current_best_pts = EXCLUDED.current_best_pts,
                            current_best_sharpness = EXCLUDED.current_best_sharpness
                    """),
                    {
                        "asset_id": asset_id,
                        "anchor_phash": active_state.anchor_phash,
                        "scene_start_ts": active_state.scene_start_ts,
                        "current_best_pts": active_state.current_best_pts,
                        "current_best_sharpness": active_state.current_best_sharpness,
                    },
                )
            else:
                session.execute(
                    text("DELETE FROM video_active_state WHERE asset_id = :asset_id"),
                    {"asset_id": asset_id},
                )

            return scene_id
