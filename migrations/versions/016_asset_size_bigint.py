"""asset_size_bigint

Change asset.size from INTEGER to BIGINT for large media files over 2GB.

Revision ID: 016
Revises: 015
Create Date: 2025-02-26

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "016"
down_revision: Union[str, None] = "015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "asset",
        "size",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
    )


def downgrade() -> None:
    op.alter_column(
        "asset",
        "size",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
    )
