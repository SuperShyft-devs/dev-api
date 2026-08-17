"""Add require_session_details to notification_services.

Revision ID: 0115_req_session_details
Revises: 0114_book_expert_notif_events
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0115_req_session_details"
down_revision = "0114_book_expert_notif_events"
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
        inspector, "notification_services", "require_session_details"
    ):
        op.add_column(
            "notification_services",
            sa.Column(
                "require_session_details",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )


def downgrade() -> None:
    op.drop_column("notification_services", "require_session_details")
