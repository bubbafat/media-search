"""rename_workerstatus_to_worker_status

Revision ID: 004
Revises: 003
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op


revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("workerstatus", "worker_status")


def downgrade() -> None:
    op.rename_table("worker_status", "workerstatus")
