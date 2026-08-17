"""Add blood_collection_cabin to engagement_participants.

Revision ID: 0113_ep_blood_collection_cabin
Revises: 0112_org_contact_person_user_ids

Stores the cabin_key chosen at B2B onboard so occupancy can be counted
per engagement + cabin + date + time slot.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0113_ep_blood_collection_cabin"
down_revision = "0112_org_contact_person_user_ids"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    op.execute(
        sa.text(
            "ALTER TABLE engagement_participants "
            "ADD COLUMN IF NOT EXISTS blood_collection_cabin VARCHAR"
        )
    )
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_ep_cabin_slot_occupancy "
            "ON engagement_participants "
            "(engagement_id, blood_collection_cabin, engagement_date, slot_start_time)"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_ep_cabin_slot_occupancy"))
    op.execute(
        sa.text(
            "ALTER TABLE engagement_participants "
            "DROP COLUMN IF EXISTS blood_collection_cabin"
        )
    )
