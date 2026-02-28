"""add_assetstatus_analyzed_light

Asset status column is VARCHAR; the new 'analyzed_light' value requires no
schema change. This migration documents the addition and maintains the
revision chain. No-op for VARCHAR; if the project later uses a Postgres
ENUM for asset status, add: op.execute("ALTER TYPE assetstatus ADD VALUE IF NOT EXISTS 'analyzed_light'").

Revision ID: 020
Revises: 019
Create Date: 2026-02-27

"""
from typing import Sequence, Union

from alembic import op

revision: str = "020"
down_revision: Union[str, None] = "019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # asset.status is VARCHAR; 'analyzed_light' is a valid string. No schema change needed.
    pass


def downgrade() -> None:
    # No schema change to reverse.
    pass
