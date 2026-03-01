"""Add library_model_policy table for Quickwit index promotion and rollback."""
from alembic import op
import sqlalchemy as sa

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "library_model_policy",
        sa.Column("library_slug", sa.String(), primary_key=True),
        sa.Column("active_index_name", sa.String(), nullable=False),
        sa.Column("shadow_index_name", sa.String(), nullable=True),
        sa.Column("previous_index_name", sa.String(), nullable=True),
        sa.Column(
            "locked",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
        sa.Column(
            "locked_since",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "promotion_progress",
            sa.Float(),
            nullable=False,
            server_default="0.0",
        ),
        sa.ForeignKeyConstraint(["library_slug"], ["library.slug"]),
    )


def downgrade() -> None:
    op.drop_table("library_model_policy")
