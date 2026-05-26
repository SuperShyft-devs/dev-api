"""Add notification_service_key to engagements.

Revision ID: 0051_eng_notif_svc_key
Revises: 0050_req_participant_detail
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

_DEFAULT_KEY = "booking-alert-whatsapp"

revision = "0051_eng_notif_svc_key"
down_revision = "0050_req_participant_detail"
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

    if not _table_exists(inspector, "engagements"):
        return

    if not _column_exists(inspector, "engagements", "notification_service_key"):
        op.add_column(
            "engagements",
            sa.Column("notification_service_key", sa.String(), nullable=True),
        )
        op.execute(
            sa.text(
                "UPDATE engagements SET notification_service_key = :key "
                "WHERE notification_service_key IS NULL"
            ).bindparams(key=_DEFAULT_KEY)
        )
        op.alter_column(
            "engagements",
            "notification_service_key",
            nullable=False,
            server_default=_DEFAULT_KEY,
        )
        if _table_exists(inspector, "notification_services"):
            op.create_foreign_key(
                "fk_engagements_notification_service_key",
                "engagements",
                "notification_services",
                ["notification_service_key"],
                ["service_key"],
            )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _column_exists(inspector, "engagements", "notification_service_key"):
        return

    try:
        op.drop_constraint(
            "fk_engagements_notification_service_key",
            "engagements",
            type_="foreignkey",
        )
    except Exception:
        pass
    op.drop_column("engagements", "notification_service_key")
