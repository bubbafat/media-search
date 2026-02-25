"""SQLModel table/entity definitions for MediaSearch v2. Postgres 16+ only (JSONB, TSVector)."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from sqlalchemy import Column, Index
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


# --- Tables (FK order: AIModel -> Library -> Asset -> VideoFrame; WorkerStatus standalone) ---


class AIModel(SQLModel, table=True):
    __tablename__ = "aimodel"

    id: int | None = Field(default=None, primary_key=True)
    slug: str = Field(unique=True, index=True)
    version: str = ""


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
    )

    id: int | None = Field(default=None, primary_key=True)
    library_id: str = Field(foreign_key="library.slug")
    rel_path: str = Field(index=True)
    type: AssetType = AssetType.image
    mtime: float = 0.0
    size: int = 0
    status: AssetStatus = Field(default=AssetStatus.pending)
    tags_model_id: int | None = Field(default=None, foreign_key="aimodel.id")
    worker_id: str | None = Field(default=None)
    lease_expires_at: datetime | None = Field(default=None)
    retry_count: int = 0
    error_message: str | None = Field(default=None)

    library: "Library" = Relationship()


class VideoFrame(SQLModel, table=True):
    __tablename__ = "videoframe"

    id: int | None = Field(default=None, primary_key=True)
    asset_id: int = Field(foreign_key="asset.id")
    timestamp_ms: int = 0
    is_keyframe: bool = False
    search_vector: Optional[str] = Field(default=None, sa_column=Column(TSVECTOR))


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
