"""asset_segmentation_version

Add segmentation_version column to asset (tracks PHASH_THRESHOLD and DEBOUNCE_SEC;
when changed, invalidates scene data and triggers re-segmentation).

Revision ID: 021
Revises: 020
Create Date: 2026-02-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "021"
down_revision: Union[str, None] = "020"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "asset",
        sa.Column("segmentation_version", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset", "segmentation_version")
