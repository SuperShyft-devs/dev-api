"""Add booking_type and metadata to bookings; drop is_metsights_profile_created from engagement_participants.

Revision ID: 0044_booking_meta_col_drop
Revises: 0043_healthians_id_columns
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSON


revision = "0044_booking_meta_col_drop"
down_revision = "0043_healthians_id_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("bookings", sa.Column("booking_type", sa.String(), nullable=True))
    op.add_column("bookings", sa.Column("metadata", JSON(), nullable=True))

    op.execute(
        "UPDATE engagement_participants "
        "SET is_profile_created_on_metsights = is_metsights_profile_created "
        "WHERE is_metsights_profile_created = TRUE AND is_profile_created_on_metsights = FALSE"
    )
    op.drop_column("engagement_participants", "is_metsights_profile_created")


def downgrade() -> None:
    op.add_column(
        "engagement_participants",
        sa.Column(
            "is_metsights_profile_created",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.execute(
        "UPDATE engagement_participants "
        "SET is_metsights_profile_created = is_profile_created_on_metsights"
    )
    op.drop_column("bookings", "metadata")
    op.drop_column("bookings", "booking_type")
