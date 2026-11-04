"""Support ticket resolution metadata; migrate closed to resolved.

Revision ID: 0185_support_ticket_resolution
Revises: 0184_expert_consultation_modes
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0185_support_ticket_resolution"
down_revision = "0184_expert_consultation_modes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("UPDATE support_tickets SET status = 2 WHERE status = 3"))

    op.add_column(
        "support_tickets",
        sa.Column(
            "resolved_by_employee_id",
            sa.Integer(),
            sa.ForeignKey("employee.employee_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.add_column(
        "support_tickets",
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "support_tickets",
        sa.Column("resolution_notes", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
