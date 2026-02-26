"""video_scenes_fts_gin_index

Add GIN index on video_scenes.metadata for full-text search.

Revision ID: 013
Revises: 012
Create Date: 2025-02-26

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "013"
down_revision: Union[str, None] = "012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_video_scenes_fts",
        "video_scenes",
        [sa.text("to_tsvector('english', metadata)")],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_video_scenes_fts", table_name="video_scenes")
