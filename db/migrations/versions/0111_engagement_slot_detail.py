"""Add slot_detail JSON column to engagements.

Revision ID: 0111_engagement_slot_detail
Revises: 0110_cat_ids_is_submitted

Stores per-date cabin scheduling for blood collection and consultation:
{
  "blood_collection": { "YYYY-MM-DD": [ { cabin fields... } ] },
  "consultation": { "YYYY-MM-DD": [ { cabin fields... } ] }
}
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0111_engagement_slot_detail"
down_revision = "0110_cat_ids_is_submitted"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    op.execute(
        sa.text(
            "ALTER TABLE engagements "
            "ADD COLUMN IF NOT EXISTS slot_detail JSON"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    op.execute(
        sa.text("ALTER TABLE engagements DROP COLUMN IF EXISTS slot_detail")
    )
