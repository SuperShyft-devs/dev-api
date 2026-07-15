"""Add effective_from and effective_until date columns to experts table.

Revision ID: 0092_experts_effective_dates
Revises: 0091_expert_employee_backfill
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0092_experts_effective_dates"
down_revision = "0091_expert_employee_backfill"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("experts", sa.Column("effective_from", sa.Date(), nullable=True))
    op.add_column("experts", sa.Column("effective_until", sa.Date(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
