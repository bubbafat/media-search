"""aimodel_name_version_asset_analysis

- aimodel: replace slug with name; add composite unique (name, version).
- asset: add analysis_model_id (FK aimodel.id) and visual_analysis (JSONB).

Revision ID: 009
Revises: 008
Create Date: 2025-02-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # aimodel: add name, backfill from slug, drop slug, add unique (name, version)
    op.add_column("aimodel", sa.Column("name", sa.String(), nullable=True))
    op.execute(sa.text("UPDATE aimodel SET name = slug WHERE name IS NULL"))
    op.drop_index(op.f("ix_aimodel_slug"), table_name="aimodel")
    op.drop_column("aimodel", "slug")
    op.alter_column(
        "aimodel",
        "name",
        existing_type=sa.String(),
        nullable=False,
    )
    op.create_unique_constraint(
        "uq_aimodel_name_version",
        "aimodel",
        ["name", "version"],
    )

    # asset: analysis_model_id and visual_analysis
    op.add_column(
        "asset",
        sa.Column("analysis_model_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_asset_analysis_model_id_aimodel",
        "asset",
        "aimodel",
        ["analysis_model_id"],
        ["id"],
    )
    op.add_column(
        "asset",
        sa.Column("visual_analysis", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset", "visual_analysis")
    op.drop_constraint(
        "fk_asset_analysis_model_id_aimodel",
        "asset",
        type_="foreignkey",
    )
    op.drop_column("asset", "analysis_model_id")

    op.drop_constraint("uq_aimodel_name_version", "aimodel", type_="unique")
    op.add_column("aimodel", sa.Column("slug", sa.String(), nullable=True))
    op.execute(sa.text("UPDATE aimodel SET slug = name WHERE slug IS NULL"))
    op.alter_column(
        "aimodel",
        "slug",
        existing_type=sa.String(),
        nullable=False,
    )
    op.drop_column("aimodel", "name")
    op.create_index(op.f("ix_aimodel_slug"), "aimodel", ["slug"], unique=True)
