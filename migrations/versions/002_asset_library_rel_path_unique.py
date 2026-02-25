"""asset_library_rel_path_unique

Revision ID: 002
Revises: 001
Create Date: 2025-02-24

"""
from typing import Sequence, Union

from alembic import op


revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_asset_library_rel_path", table_name="asset")
    op.create_index(
        "ix_asset_library_rel_path",
        "asset",
        ["library_id", "rel_path"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_asset_library_rel_path", table_name="asset")
    op.create_index(
        "ix_asset_library_rel_path",
        "asset",
        ["library_id", "rel_path"],
        unique=False,
    )
