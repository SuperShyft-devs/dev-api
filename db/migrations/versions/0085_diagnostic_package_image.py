"""Add package_image column to diagnostic_package.

Revision ID: 0085_diag_pkg_image
Revises: 0084_support_query_notif
Create Date: 2026-07-14

Changes:
- diagnostic_package: package_image (nullable string URL)
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0085_diag_pkg_image"
down_revision = "0084_support_query_notif"
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
        inspector, "diagnostic_package", "package_image"
    ):
        op.add_column(
            "diagnostic_package",
            sa.Column("package_image", sa.String(), nullable=True),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
