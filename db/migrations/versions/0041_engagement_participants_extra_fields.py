"""Add extra onboarding columns to engagement_participants.

Revision ID: 0041_eng_participants_extra
Revises: 0040_eng_participants
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0041_eng_participants_extra"
down_revision = "0040_eng_participants"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "engagement_participants",
        sa.Column("participant_department", sa.String(), nullable=True),
    )
    op.add_column(
        "engagement_participants",
        sa.Column("participant_blood_group", sa.String(), nullable=True),
    )
    op.add_column(
        "engagement_participants",
        sa.Column(
            "is_metsights_profile_created",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    """Downgrades are intentionally disabled."""
    raise RuntimeError("Downgrade is not supported")
