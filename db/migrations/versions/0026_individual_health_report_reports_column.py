"""Rename individual_health_report.metsights_output to reports."""

from __future__ import annotations

from alembic import op


revision = "0026_indiv_report_reports_col"
down_revision = "0025_assess_type_code"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('ALTER TABLE individual_health_report RENAME COLUMN metsights_output TO reports')


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
