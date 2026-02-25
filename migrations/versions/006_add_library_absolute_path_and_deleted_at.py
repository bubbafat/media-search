"""add_library_absolute_path_and_deleted_at

Revision ID: 006
Revises: 005
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "library",
        sa.Column("absolute_path", sa.String(), nullable=True),
    )
    op.add_column(
        "library",
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("library", "deleted_at")
    op.drop_column("library", "absolute_path")
