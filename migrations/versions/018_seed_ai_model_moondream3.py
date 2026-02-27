"""seed_ai_model_moondream3

- Ensure aimodel row for moondream3 (name, version preview) exists.
- Does NOT change default_ai_model_id; moondream2 remains default.

Revision ID: 018
Revises: 017
Create Date: 2026-02-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "018"
down_revision: Union[str, None] = "017"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

MOONDREAM3_NAME = "moondream3"
MOONDREAM3_VERSION = "preview"


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "INSERT INTO aimodel (name, version) VALUES (:name, :version) "
            "ON CONFLICT (name, version) DO NOTHING"
        ),
        {"name": MOONDREAM3_NAME, "version": MOONDREAM3_VERSION},
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "DELETE FROM aimodel WHERE name = :name AND version = :version"
        ),
        {"name": MOONDREAM3_NAME, "version": MOONDREAM3_VERSION},
    )
