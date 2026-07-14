"""Add default support query notification service keys to platform_settings.

Revision ID: 0084_support_query_notif
Revises: 0083_ep_nullable_schedule
Create Date: 2026-07-14

Changes:
- platform_settings: default_support_query_notification (comma-separated service keys)
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0084_support_query_notif"
down_revision = "0083_ep_nullable_schedule"
branch_labels = None
depends_on = None

_COLUMN = "default_support_query_notification"


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "platform_settings") and not _column_exists(
        inspector, "platform_settings", _COLUMN
    ):
        op.add_column(
            "platform_settings",
            sa.Column(_COLUMN, sa.String(length=500), nullable=True),
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "platform_settings") and _column_exists(
        inspector, "platform_settings", _COLUMN
    ):
        op.drop_column("platform_settings", _COLUMN)
