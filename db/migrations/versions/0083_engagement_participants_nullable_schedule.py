"""Allow nullable schedule fields on engagement_participants for draft bookings.

Revision ID: 0083_ep_nullable_schedule
Revises: 0082_platform_default_oa
Create Date: 2026-07-13

Changes:
- engagement_participants.engagement_date: nullable
- engagement_participants.slot_start_time: nullable
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0083_ep_nullable_schedule"
down_revision = "0082_platform_default_oa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "engagement_participants",
        "engagement_date",
        existing_type=sa.Date(),
        nullable=True,
    )
    op.alter_column(
        "engagement_participants",
        "slot_start_time",
        existing_type=sa.Time(),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "engagement_participants",
        "slot_start_time",
        existing_type=sa.Time(),
        nullable=False,
    )
    op.alter_column(
        "engagement_participants",
        "engagement_date",
        existing_type=sa.Date(),
        nullable=False,
    )
