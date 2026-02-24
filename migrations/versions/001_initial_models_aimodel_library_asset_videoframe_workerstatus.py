"""initial_models_aimodel_library_asset_videoframe_workerstatus

Revision ID: 001
Revises:
Create Date: 2025-02-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # AIModel
    op.create_table(
        "aimodel",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("version", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_aimodel_slug"), "aimodel", ["slug"], unique=True)

    # Library (FK to aimodel)
    op.create_table(
        "library",
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("scan_status", sa.String(), nullable=False),
        sa.Column("target_tagger_id", sa.Integer(), nullable=True),
        sa.Column("sampling_limit", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("slug"),
        sa.ForeignKeyConstraint(["target_tagger_id"], ["aimodel.id"]),
    )

    # Asset (FK to library, aimodel) with composite index
    op.create_table(
        "asset",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("library_id", sa.String(), nullable=False),
        sa.Column("rel_path", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("mtime", sa.Float(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("tags_model_id", sa.Integer(), nullable=True),
        sa.Column("worker_id", sa.String(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["library_id"], ["library.slug"]),
        sa.ForeignKeyConstraint(["tags_model_id"], ["aimodel.id"]),
    )
    op.create_index(op.f("ix_asset_rel_path"), "asset", ["rel_path"], unique=False)
    op.create_index(
        "ix_asset_library_rel_path",
        "asset",
        ["library_id", "rel_path"],
        unique=False,
    )

    # VideoFrame (FK to asset)
    op.create_table(
        "videoframe",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("timestamp_ms", sa.Integer(), nullable=False),
        sa.Column("is_keyframe", sa.Boolean(), nullable=False),
        sa.Column("search_vector", TSVECTOR(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["asset_id"], ["asset.id"]),
    )

    # WorkerStatus
    op.create_table(
        "workerstatus",
        sa.Column("worker_id", sa.String(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("command", sa.String(), nullable=False),
        sa.Column("stats", JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("worker_id"),
    )


def downgrade() -> None:
    op.drop_table("workerstatus")
    op.drop_table("videoframe")
    op.drop_index("ix_asset_library_rel_path", table_name="asset")
    op.drop_index(op.f("ix_asset_rel_path"), table_name="asset")
    op.drop_table("asset")
    op.drop_table("library")
    op.drop_index(op.f("ix_aimodel_slug"), table_name="aimodel")
    op.drop_table("aimodel")
