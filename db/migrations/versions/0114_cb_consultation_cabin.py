"""Add consultation_cabin to consultation_bookings.

Revision ID: 0114_cb_consultation_cabin
Revises: 0113_ep_blood_collection_cabin

Stores the consultation cabin_key chosen at B2B onboard so occupancy can
be counted per engagement + cabin + date + time slot.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0114_cb_consultation_cabin"
down_revision = "0113_ep_blood_collection_cabin"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    op.execute(
        sa.text(
            "ALTER TABLE consultation_bookings "
            "ADD COLUMN IF NOT EXISTS consultation_cabin VARCHAR"
        )
    )
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_cb_cabin_slot_occupancy "
            "ON consultation_bookings "
            "(consultation_cabin, consultation_date, consultation_slot)"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_cb_cabin_slot_occupancy"))
    op.execute(
        sa.text(
            "ALTER TABLE consultation_bookings "
            "DROP COLUMN IF EXISTS consultation_cabin"
        )
    )
