"""Add cached PDF URLs on individual_health_report.

Revision ID: 0035_indiv_report_pdf_urls
Revises: 0034_experts_module
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0035_indiv_report_pdf_urls"
down_revision = "0034_experts_module"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("individual_health_report", sa.Column("report_url", sa.Text(), nullable=True))
    op.add_column("individual_health_report", sa.Column("diagnostic_report_url", sa.Text(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
