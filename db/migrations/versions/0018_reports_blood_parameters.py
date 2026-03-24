"""Replace diagnostics_output with blood_parameters on individual reports.

Revision ID: 0018_reports_blood_parameters
Revises: 0017_ai_metsights_record_id
Create Date: 2026-03-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0018_reports_blood_parameters"
down_revision = "0017_ai_metsights_record_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("individual_health_report", "diagnostics_output")
    op.add_column("individual_health_report", sa.Column("blood_parameters", sa.JSON(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
