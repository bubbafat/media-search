"""scan_status_full_fast_requested

Replace legacy scan_req with full_scan_requested (spec Section 2.1).
Revision ID: 005
Revises: 004
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text("UPDATE library SET scan_status = 'full_scan_requested' WHERE scan_status = 'scan_req'")
    )


def downgrade() -> None:
    op.execute(
        sa.text("UPDATE library SET scan_status = 'scan_req' WHERE scan_status = 'full_scan_requested'")
    )
