"""seed_default_ai_model_moondream2

- Ensure aimodel row for moondream2 (name, version 2025-01-09) exists.
- Set system_metadata default_ai_model_id to that model's id.

Revision ID: 012
Revises: 011
Create Date: 2025-02-26

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "012"
down_revision: Union[str, None] = "011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

MOONDREAM2_NAME = "moondream2"
MOONDREAM2_VERSION = "2025-01-09"
DEFAULT_AI_MODEL_ID_KEY = "default_ai_model_id"


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "INSERT INTO aimodel (name, version) VALUES (:name, :version) "
            "ON CONFLICT (name, version) DO NOTHING"
        ),
        {"name": MOONDREAM2_NAME, "version": MOONDREAM2_VERSION},
    )
    row = conn.execute(
        sa.text(
            "SELECT id FROM aimodel WHERE name = :name AND version = :version"
        ),
        {"name": MOONDREAM2_NAME, "version": MOONDREAM2_VERSION},
    ).fetchone()
    assert row is not None, "aimodel row for moondream2 must exist after insert"
    model_id = row[0]
    conn.execute(
        sa.text(
            "INSERT INTO system_metadata (key, value) VALUES (:key, :value) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ),
        {"key": DEFAULT_AI_MODEL_ID_KEY, "value": str(model_id)},
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text("DELETE FROM system_metadata WHERE key = :key"),
        {"key": DEFAULT_AI_MODEL_ID_KEY},
    )
