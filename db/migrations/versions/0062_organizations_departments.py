"""Add departments JSON column to organizations.

Revision ID: 0062_organizations_departments
Revises: 0061_employee_role_enum
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0062_organizations_departments"
down_revision = "0061_employee_role_enum"
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

    if not _table_exists(inspector, "organizations"):
        return

    if not _column_exists(inspector, "organizations", "departments"):
        op.add_column("organizations", sa.Column("departments", sa.JSON(), nullable=True))


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _column_exists(inspector, "organizations", "departments"):
        op.drop_column("organizations", "departments")
