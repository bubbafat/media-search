"""asset_proxied_processing_error_message

Add error_message column to asset. Status column is already VARCHAR; new enum values
(processing, proxied) are valid strings and require no schema change.

Revision ID: 008
Revises: 007
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "asset",
        sa.Column("error_message", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset", "error_message")
