"""asset_video_preview_path

Add video_preview_path column to asset (relative path to 10s head-clip MP4 for hover preview).

Revision ID: 015
Revises: 014
Create Date: 2025-02-26

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "015"
down_revision: Union[str, None] = "014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "asset",
        sa.Column("video_preview_path", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset", "video_preview_path")
