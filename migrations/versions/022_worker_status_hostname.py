"""worker_status_hostname

Add hostname column to worker_status (enables local-aware memory management).

Revision ID: 022
Revises: 021
Create Date: 2026-02-28

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "022"
down_revision: Union[str, None] = "021"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "worker_status",
        sa.Column("hostname", sa.String(), nullable=False, server_default=""),
    )
    op.create_index(
        op.f("ix_worker_status_hostname"),
        "worker_status",
        ["hostname"],
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_worker_status_hostname"), table_name="worker_status")
    op.drop_column("worker_status", "hostname")
