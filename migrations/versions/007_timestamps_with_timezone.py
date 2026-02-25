"""timestamps_with_timezone

Ensure all timestamp columns use TIMESTAMP WITH TIME ZONE (project rule: never store local time).
Revision ID: 007
Revises: 006
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # asset.lease_expires_at: interpret existing values as UTC
    op.alter_column(
        "asset",
        "lease_expires_at",
        existing_type=sa.DateTime(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=True,
        postgresql_using="lease_expires_at AT TIME ZONE 'UTC'",
    )
    # worker_status.last_seen_at: interpret existing values as UTC
    op.alter_column(
        "worker_status",
        "last_seen_at",
        existing_type=sa.DateTime(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=False,
        postgresql_using="last_seen_at AT TIME ZONE 'UTC'",
    )


def downgrade() -> None:
    op.alter_column(
        "worker_status",
        "last_seen_at",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.DateTime(),
        existing_nullable=False,
        postgresql_using="last_seen_at AT TIME ZONE 'UTC'",
    )
    op.alter_column(
        "asset",
        "lease_expires_at",
        existing_type=sa.DateTime(timezone=True),
        type_=sa.DateTime(),
        existing_nullable=True,
        postgresql_using="lease_expires_at AT TIME ZONE 'UTC'",
    )
