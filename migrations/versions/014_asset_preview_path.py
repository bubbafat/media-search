"""asset_preview_path

Add preview_path column to asset (relative path to video animated preview; single source of truth).

Revision ID: 014
Revises: 013
Create Date: 2025-02-26

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "014"
down_revision: Union[str, None] = "013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "asset",
        sa.Column("preview_path", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset", "preview_path")
