"""Add enforce_metsights_pro_gate on engagements.

Revision ID: 0186_enforce_metsights_pro_gate
Revises: 0185_support_ticket_resolution
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0186_enforce_metsights_pro_gate"
down_revision = "0185_support_ticket_resolution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "engagements",
        sa.Column(
            "enforce_metsights_pro_gate",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.execute(sa.text("UPDATE engagements SET enforce_metsights_pro_gate = false"))


def downgrade() -> None:
    op.drop_column("engagements", "enforce_metsights_pro_gate")
