"""Add notify_users_for_consultation engagement and platform default columns.

Revision ID: 0100_notify_consultation
Revises: 0099_b2c_onboarding_defaults
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0100_notify_consultation"
down_revision = "0099_b2c_onboarding_defaults"
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

    if _table_exists(inspector, "engagements") and not _column_exists(
        inspector, "engagements", "notify_users_for_consultation"
    ):
        op.add_column(
            "engagements",
            sa.Column("notify_users_for_consultation", sa.String(), nullable=True),
        )

    if _table_exists(inspector, "platform_settings") and not _column_exists(
        inspector, "platform_settings", "default_notify_users_for_consultation"
    ):
        op.add_column(
            "platform_settings",
            sa.Column("default_notify_users_for_consultation", sa.String(500), nullable=True),
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _column_exists(inspector, "platform_settings", "default_notify_users_for_consultation"):
        op.drop_column("platform_settings", "default_notify_users_for_consultation")

    if _column_exists(inspector, "engagements", "notify_users_for_consultation"):
        op.drop_column("engagements", "notify_users_for_consultation")
