"""Add package_for column to diagnostic_package and diagnostic_test_groups.

Revision ID: 0038_diagnostic_package_for
Revises: 0037_otp_failed_attempts
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0038_diagnostic_package_for"
down_revision = "0037_otp_failed_attempts"
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

    if _table_exists(inspector, "diagnostic_package") and not _column_exists(
        inspector, "diagnostic_package", "package_for"
    ):
        op.add_column(
            "diagnostic_package",
            sa.Column("package_for", sa.String(), nullable=False, server_default="public"),
        )

    if _table_exists(inspector, "diagnostic_test_groups") and not _column_exists(
        inspector, "diagnostic_test_groups", "package_for"
    ):
        op.add_column(
            "diagnostic_test_groups",
            sa.Column("package_for", sa.String(), nullable=False, server_default="public"),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
