"""SQLModel table/entity definitions for MediaSearch v2. Postgres 16+ only (JSONB, TSVector)."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from sqlalchemy import Column, Index, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlmodel import Field, Relationship, SQLModel


# --- Enums (stored as strings in DB) ---


class ScanStatus(str, Enum):
    idle = "idle"
    full_scan_requested = "full_scan_requested"
    fast_scan_requested = "fast_scan_requested"
    scanning = "scanning"


class AssetType(str, Enum):
    image = "image"
    video = "video"


class AssetStatus(str, Enum):
    pending = "pending"
    processing = "processing"
    proxied = "proxied"
    extracting = "extracting"
    analyzing = "analyzing"
    analyzed_light = "analyzed_light"
    completed = "completed"
    failed = "failed"
    poisoned = "poisoned"


class WorkerState(str, Enum):
    idle = "idle"
    processing = "processing"
    paused = "paused"
    offline = "offline"


class WorkerCommand(str, Enum):
    none = "none"
    pause = "pause"
    resume = "resume"
    shutdown = "shutdown"
    forensic_dump = "forensic_dump"


class SceneKeepReason(str, Enum):
    """Why a scene was closed (analytics: e.g. why so many scenes)."""

    phash = "phash"  # Visual drift (Hamming > 51)
    temporal = "temporal"  # 30s ceiling
    forced = "forced"  # EOF


# --- Tables (FK order: AIModel -> Library -> Asset -> VideoFrame; WorkerStatus standalone) ---


class AIModel(SQLModel, table=True):
    __tablename__ = "aimodel"
    __table_args__ = (UniqueConstraint("name", "version", name="uq_aimodel_name_version"),)

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(nullable=False)
    version: str = Field(nullable=False)


class Library(SQLModel, table=True):
    __tablename__ = "library"

    slug: str = Field(primary_key=True)
    name: str = ""
    absolute_path: str = Field(...)
    is_active: bool = True
    scan_status: ScanStatus = Field(default=ScanStatus.idle)
    target_tagger_id: int | None = Field(default=None, foreign_key="aimodel.id")
    sampling_limit: int = 100
    deleted_at: datetime | None = Field(default=None)


class Asset(SQLModel, table=True):
    __tablename__ = "asset"
    __table_args__ = (
        Index("ix_asset_library_rel_path", "library_id", "rel_path", unique=True),
        Index(
            "ix_asset_fts",
            text("to_tsvector('english', visual_analysis)"),
            postgresql_using="gin",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    library_id: str = Field(foreign_key="library.slug")
    rel_path: str = Field(index=True)
    type: AssetType = AssetType.image
    mtime: float = 0.0
    size: int = 0
    status: AssetStatus = Field(default=AssetStatus.pending)
    tags_model_id: int | None = Field(default=None, foreign_key="aimodel.id")
    analysis_model_id: int | None = Field(default=None, foreign_key="aimodel.id")
    worker_id: str | None = Field(default=None)
    lease_expires_at: datetime | None = Field(default=None)
    retry_count: int = 0
    error_message: str | None = Field(default=None)
    visual_analysis: dict[str, Any] | None = Field(default=None, sa_column=Column(JSONB))
    preview_path: str | None = Field(default=None)
    video_preview_path: str | None = Field(default=None)

    library: "Library" = Relationship()


class Project(SQLModel, table=True):
    __tablename__ = "project"

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(nullable=False)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    export_path: str | None = Field(default=None)


class ProjectAsset(SQLModel, table=True):
    __tablename__ = "project_assets"

    project_id: int = Field(foreign_key="project.id", primary_key=True)
    asset_id: int = Field(foreign_key="asset.id", primary_key=True)


class VideoFrame(SQLModel, table=True):
    __tablename__ = "videoframe"

    id: int | None = Field(default=None, primary_key=True)
    asset_id: int = Field(foreign_key="asset.id")
    timestamp_ms: int = 0
    is_keyframe: bool = False
    search_vector: Optional[str] = Field(default=None, sa_column=Column(TSVECTOR))


class VideoScene(SQLModel, table=True):
    """One closed scene: rep frame path, bounds, caption, keep_reason."""

    __tablename__ = "video_scenes"
    __table_args__ = (
        Index("ix_video_scenes_asset_id_end_ts", "asset_id", "end_ts"),
        Index(
            "ix_video_scenes_fts",
            text("to_tsvector('english', metadata)"),
            postgresql_using="gin",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    asset_id: int = Field(foreign_key="asset.id")
    start_ts: float = 0.0
    end_ts: float = 0.0
    description: str | None = Field(default=None)
    # DB column "metadata" (JSONB); Python attr "scene_metadata" to avoid SQLAlchemy reserved name
    scene_metadata: dict[str, Any] | None = Field(default=None, sa_column=Column("metadata", JSONB()))
    sharpness_score: float = 0.0
    rep_frame_path: str = ""
    keep_reason: SceneKeepReason = Field(default=SceneKeepReason.forced)


class VideoActiveState(SQLModel, table=True):
    """One row per asset currently being indexed (resume state)."""

    __tablename__ = "video_active_state"

    asset_id: int = Field(foreign_key="asset.id", primary_key=True)
    anchor_phash: str = ""
    scene_start_ts: float = 0.0
    current_best_pts: float = 0.0
    current_best_sharpness: float = -1.0


class WorkerStatus(SQLModel, table=True):
    __tablename__ = "worker_status"

    worker_id: str = Field(primary_key=True)
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    state: WorkerState = Field(default=WorkerState.offline)
    command: WorkerCommand = Field(default=WorkerCommand.none)
    stats: dict[str, Any] | None = Field(default=None, sa_column=Column(JSONB))


class SystemMetadata(SQLModel, table=True):
    """Key/value store for system-wide settings (e.g. schema_version). Standalone, no FK."""

    __tablename__ = "system_metadata"

    key: str = Field(primary_key=True)
    value: str = ""
