"""Replace require_record_id with require_blood_report_url and require_bio_ai_report_url.

Revision ID: 0081_report_url_flags
Revises: 0080_eng_ext_camp_id
Create Date: 2026-07-09
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0081_report_url_flags"
down_revision = "0080_eng_ext_camp_id"
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

    if not _table_exists(inspector, "notification_services"):
        return

    if not _column_exists(inspector, "notification_services", "require_blood_report_url"):
        op.add_column(
            "notification_services",
            sa.Column(
                "require_blood_report_url",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )

    if not _column_exists(inspector, "notification_services", "require_bio_ai_report_url"):
        op.add_column(
            "notification_services",
            sa.Column(
                "require_bio_ai_report_url",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )

    if _column_exists(inspector, "notification_services", "require_record_id"):
        op.drop_column("notification_services", "require_record_id")


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "notification_services"):
        return

    if not _column_exists(inspector, "notification_services", "require_record_id"):
        op.add_column(
            "notification_services",
            sa.Column(
                "require_record_id",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("true"),
            ),
        )

    if _column_exists(inspector, "notification_services", "require_blood_report_url"):
        op.drop_column("notification_services", "require_blood_report_url")

    if _column_exists(inspector, "notification_services", "require_bio_ai_report_url"):
        op.drop_column("notification_services", "require_bio_ai_report_url")
