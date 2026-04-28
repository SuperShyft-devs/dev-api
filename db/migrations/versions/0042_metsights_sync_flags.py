"""Add Metsights engagement and participant sync flags.

Revision ID: 0042_metsights_sync_flags
Revises: 0041_eng_participants_extra
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0042_metsights_sync_flags"
down_revision = "0041_eng_participants_extra"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "engagements",
        sa.Column("create_profile_on_metsights", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "engagements",
        sa.Column("enroll_for_fitprint_full", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("is_profile_created_on_metsights", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("is_primary_record_id_synced", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("is_fitprint_record_id_synced", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    """Downgrades are intentionally disabled."""
    raise RuntimeError("Downgrade is not supported")
