"""Add city column to camp_reports for city-scoped reports.

Revision ID: 0101_camp_reports_city
Revises: 0100_notify_consultation
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0101_camp_reports_city"
down_revision = "0100_notify_consultation"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "camp_reports"):
        return

    if not _column_exists(inspector, "camp_reports", "city"):
        op.add_column("camp_reports", sa.Column("city", sa.String(), nullable=True))

    if _index_exists(inspector, "camp_reports", "uq_camp_reports_camp_no_overall"):
        op.drop_index("uq_camp_reports_camp_no_overall", table_name="camp_reports")
    if _index_exists(inspector, "camp_reports", "uq_camp_reports_camp_no_department"):
        op.drop_index("uq_camp_reports_camp_no_department", table_name="camp_reports")

    # Re-inspect after drops
    inspector = inspect(connection)

    if not _index_exists(inspector, "camp_reports", "uq_camp_reports_camp_no_overall"):
        op.create_index(
            "uq_camp_reports_camp_no_overall",
            "camp_reports",
            ["camp_no"],
            unique=True,
            postgresql_where=sa.text("department IS NULL AND city IS NULL"),
        )
    if not _index_exists(inspector, "camp_reports", "uq_camp_reports_camp_no_department"):
        op.create_index(
            "uq_camp_reports_camp_no_department",
            "camp_reports",
            ["camp_no", "department"],
            unique=True,
            postgresql_where=sa.text("department IS NOT NULL AND city IS NULL"),
        )
    if not _index_exists(inspector, "camp_reports", "uq_camp_reports_camp_no_city"):
        op.create_index(
            "uq_camp_reports_camp_no_city",
            "camp_reports",
            ["camp_no", "city"],
            unique=True,
            postgresql_where=sa.text("department IS NULL AND city IS NOT NULL"),
        )
    if not _index_exists(inspector, "camp_reports", "uq_camp_reports_camp_no_city_department"):
        op.create_index(
            "uq_camp_reports_camp_no_city_department",
            "camp_reports",
            ["camp_no", "city", "department"],
            unique=True,
            postgresql_where=sa.text("department IS NOT NULL AND city IS NOT NULL"),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
