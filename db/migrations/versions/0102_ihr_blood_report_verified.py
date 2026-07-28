"""Add blood_parameters_full_report and blood_parameters_verified_at to IHR.

Revision ID: 0102_ihr_blood_verified
Revises: 0101_camp_reports_city
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0102_ihr_blood_verified"
down_revision = "0101_camp_reports_city"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "individual_health_report"):
        return

    if not _column_exists(inspector, "individual_health_report", "blood_parameters_full_report"):
        op.add_column(
            "individual_health_report",
            sa.Column("blood_parameters_full_report", sa.Boolean(), nullable=True),
        )

    if not _column_exists(inspector, "individual_health_report", "blood_parameters_verified_at"):
        op.add_column(
            "individual_health_report",
            sa.Column("blood_parameters_verified_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
