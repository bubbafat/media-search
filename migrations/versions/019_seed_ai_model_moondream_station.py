"""seed_ai_model_moondream_station

- Ensure aimodel row for moondream-station (name, version local) exists.
- Does NOT change default_ai_model_id.
- Used when inference runs via Moondream Station (e.g. md3p-int4 on Apple Silicon).

Revision ID: 019
Revises: 018
Create Date: 2026-02-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "019"
down_revision: Union[str, None] = "018"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

STATION_NAME = "moondream-station"
STATION_VERSION = "local"


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "INSERT INTO aimodel (name, version) VALUES (:name, :version) "
            "ON CONFLICT (name, version) DO NOTHING"
        ),
        {"name": STATION_NAME, "version": STATION_VERSION},
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "DELETE FROM aimodel WHERE name = :name AND version = :version"
        ),
        {"name": STATION_NAME, "version": STATION_VERSION},
    )
