"""Add report_bts JSON column to camp_reports.

Revision ID: 0103_camp_reports_bts
Revises: 0102_ihr_blood_verified
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
revision = "0103_camp_reports_bts"
down_revision = "0102_ihr_blood_verified"
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

    if not _table_exists(inspector, "camp_reports"):
        return

    if not _column_exists(inspector, "camp_reports", "report_bts"):
        op.add_column(
            "camp_reports",
            sa.Column("report_bts", sa.JSON(), nullable=True),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
