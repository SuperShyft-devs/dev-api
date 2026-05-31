"""Add require_otp to notification_services.

Revision ID: 0054_req_otp
Revises: 0053_eng_status
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0054_req_otp"
down_revision = "0053_eng_status"
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

    if _table_exists(inspector, "notification_services") and not _column_exists(
        inspector, "notification_services", "require_otp"
    ):
        op.add_column(
            "notification_services",
            sa.Column(
                "require_otp",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )


def downgrade() -> None:
    op.drop_column("notification_services", "require_otp")
