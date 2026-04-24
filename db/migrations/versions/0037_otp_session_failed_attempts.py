"""Add failed_attempts column to auth_otp_sessions.

Revision ID: 0037_otp_failed_attempts
Revises: 0036_experts_specialization
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0037_otp_failed_attempts"
down_revision = "0036_experts_specialization"
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

    if not _table_exists(inspector, "auth_otp_sessions"):
        return

    if not _column_exists(inspector, "auth_otp_sessions", "failed_attempts"):
        op.add_column(
            "auth_otp_sessions",
            sa.Column("failed_attempts", sa.Integer(), nullable=False, server_default="0"),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
