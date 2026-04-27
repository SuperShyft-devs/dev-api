"""Rename engagement_time_slots to engagement_participants.

- Rename table engagement_time_slots -> engagement_participants
- Rename PK column time_slot_id -> engagement_participant_id
- Add participants_employee_id (varchar, nullable)
- Add want_doctor_consultation (boolean, nullable)
- Add want_nutritionist_consultation (boolean, nullable)
- Add want_doctor_and_nutritionist_consultation (boolean, nullable)

Revision ID: 0040_eng_participants
Revises: 0039_health_params_risk_ranges
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0040_eng_participants"
down_revision = "0039_health_params_risk_ranges"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("engagement_time_slots", "engagement_participants")

    op.alter_column(
        "engagement_participants",
        "time_slot_id",
        new_column_name="engagement_participant_id",
    )

    op.add_column(
        "engagement_participants",
        sa.Column("participants_employee_id", sa.String(), nullable=True),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("want_doctor_consultation", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("want_nutritionist_consultation", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("want_doctor_and_nutritionist_consultation", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    """Downgrades are intentionally disabled."""
    raise RuntimeError("Downgrade is not supported")
