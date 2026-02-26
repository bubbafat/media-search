"""video_scenes_and_active_state

- Add scene_keep_reason enum, video_scenes and video_active_state tables for video indexing persistence and resume.

Revision ID: 011
Revises: 010
Create Date: 2025-02-25

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCENE_KEEP_REASON_ENUM = "scene_keep_reason"


def upgrade() -> None:
    # PostgreSQL enum for keep_reason (phash, temporal, forced); create_type=True so it is created once here
    op.create_table(
        "video_scenes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("start_ts", sa.Float(), nullable=False),
        sa.Column("end_ts", sa.Float(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("metadata", JSONB(), nullable=True),
        sa.Column("sharpness_score", sa.Float(), nullable=False),
        sa.Column("rep_frame_path", sa.String(), nullable=False),
        sa.Column(
            "keep_reason",
            sa.Enum("phash", "temporal", "forced", name=SCENE_KEEP_REASON_ENUM, create_type=True),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["asset_id"], ["asset.id"]),
    )
    op.create_index(
        "ix_video_scenes_asset_id_end_ts",
        "video_scenes",
        ["asset_id", "end_ts"],
        unique=False,
    )
    op.create_index(op.f("ix_video_scenes_asset_id"), "video_scenes", ["asset_id"], unique=False)

    op.create_table(
        "video_active_state",
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("anchor_phash", sa.String(), nullable=False),
        sa.Column("scene_start_ts", sa.Float(), nullable=False),
        sa.Column("current_best_pts", sa.Float(), nullable=False),
        sa.Column("current_best_sharpness", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("asset_id"),
        sa.ForeignKeyConstraint(["asset_id"], ["asset.id"]),
    )


def downgrade() -> None:
    op.drop_table("video_active_state")
    op.drop_index(op.f("ix_video_scenes_asset_id"), table_name="video_scenes")
    op.drop_index("ix_video_scenes_asset_id_end_ts", table_name="video_scenes")
    op.drop_table("video_scenes")
    sa.Enum(name=SCENE_KEEP_REASON_ENUM).drop(op.get_bind(), checkfirst=True)
