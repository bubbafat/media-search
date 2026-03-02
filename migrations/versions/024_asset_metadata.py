"""Add raw_exif, media_metadata, and metadata_status to asset for NLE metadata enrichment."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "asset",
        sa.Column("raw_exif", JSONB(), nullable=True),
    )
    op.add_column(
        "asset",
        sa.Column("media_metadata", JSONB(), nullable=True),
    )
    op.add_column(
        "asset",
        sa.Column("metadata_status", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset", "raw_exif")
    op.drop_column("asset", "media_metadata")
    op.drop_column("asset", "metadata_status")
