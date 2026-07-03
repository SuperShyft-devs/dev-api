"""Add blood_report_raw column to individual_health_report.

Revision ID: 0069_blood_report_raw
Revises: 0068_category_key_unique
Create Date: 2026-07-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0069_blood_report_raw"
down_revision = "0068_category_key_unique"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "individual_health_report",
        sa.Column("blood_report_raw", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("individual_health_report", "blood_report_raw")
