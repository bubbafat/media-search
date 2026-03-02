"""Update metadata_status: drop old check constraint, migrate 'processing' rows, add new allowed values."""

from alembic import op

revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None

# New allowed values: NULL, exif_processing, exif_done, sharpness_processing, complete
NEW_CONSTRAINT_NAME = "ck_asset_metadata_status"


def upgrade() -> None:
    # Drop existing check constraint if present (e.g. added in a prior branch).
    op.execute("ALTER TABLE asset DROP CONSTRAINT IF EXISTS ck_asset_metadata_status;")
    # Migrate rows: metadata_status = 'processing' -> 'complete' if sharpness_score in media_metadata else 'exif_done'
    op.execute(
        """
        UPDATE asset
        SET metadata_status = CASE
            WHEN (media_metadata IS NOT NULL AND (media_metadata ? 'sharpness_score')) THEN 'complete'
            ELSE 'exif_done'
        END
        WHERE metadata_status = 'processing'
        """
    )
    op.execute(
        f"""
        ALTER TABLE asset ADD CONSTRAINT {NEW_CONSTRAINT_NAME} CHECK (
            metadata_status IS NULL
            OR metadata_status IN ('exif_processing', 'exif_done', 'sharpness_processing', 'complete')
        )
        """
    )


def downgrade() -> None:
    op.execute(f"ALTER TABLE asset DROP CONSTRAINT IF EXISTS {NEW_CONSTRAINT_NAME}")
    # Do not reverse the data migration (processing no longer exists; we don't restore it).
    # Optionally we could set 'exif_done' -> 'processing' and 'complete' -> 'processing' for rows
    # that had sharpness_score, but the downgrade would be lossy. Leave as-is per typical downgrade.
    pass
