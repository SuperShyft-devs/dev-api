"""Add camp_report_sections reference table.

Revision ID: 0064_camp_report_sections
Revises: 0063_engagements_camp_no
Create Date: 2026-06-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0064_camp_report_sections"
down_revision = "0063_engagements_camp_no"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "camp_report_sections",
        sa.Column("report_sections", sa.Integer(), nullable=False),
        sa.Column("section", sa.String(), nullable=False),
        sa.Column("section_key", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("report_sections"),
        sa.UniqueConstraint("section_key"),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
