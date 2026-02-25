"""system_metadata

Revision ID: 003
Revises: 002
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "system_metadata",
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("value", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("key"),
    )
    op.execute(
        sa.text("INSERT INTO system_metadata (key, value) VALUES ('schema_version', '1')")
    )


def downgrade() -> None:
    op.drop_table("system_metadata")
