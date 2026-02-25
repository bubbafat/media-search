"""asset_fts_gin_index

Add GIN index on asset.visual_analysis for full-text search.

Revision ID: 010
Revises: 009
Create Date: 2025-02-25

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_asset_fts",
        "asset",
        [sa.text("to_tsvector('english', visual_analysis)")],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_asset_fts", table_name="asset")
