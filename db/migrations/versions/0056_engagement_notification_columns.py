"""Add blood_report_notification and bioai_report_notification columns;
drop FK constraints on questionnaire_reminder_1/2 so all four columns
can store comma-separated service keys.

Revision ID: 0056_eng_notif_cols
Revises: 0055_eng_quest_remind
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0056_eng_notif_cols"
down_revision = "0055_eng_quest_remind"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _fk_exists(inspector: sa.Inspector, table_name: str, fk_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(fk["name"] == fk_name for fk in inspector.get_foreign_keys(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagements"):
        return

    # --- Drop FK constraints on questionnaire_reminder_1/2 ---
    for fk_name in (
        "fk_engagements_questionnaire_reminder_1",
        "fk_engagements_questionnaire_reminder_2",
    ):
        if _fk_exists(inspector, "engagements", fk_name):
            op.drop_constraint(fk_name, "engagements", type_="foreignkey")

    # --- Add new columns ---
    for col_name in ("blood_report_notification", "bioai_report_notification"):
        if not _column_exists(inspector, "engagements", col_name):
            op.add_column(
                "engagements",
                sa.Column(col_name, sa.String(), nullable=True),
            )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    has_services = _table_exists(inspector, "notification_services")

    # --- Drop new columns ---
    for col_name in ("bioai_report_notification", "blood_report_notification"):
        if _column_exists(inspector, "engagements", col_name):
            op.drop_column("engagements", col_name)

    # --- Restore FK constraints on questionnaire_reminder_1/2 ---
    if has_services:
        for col_name, fk_name in (
            ("questionnaire_reminder_1", "fk_engagements_questionnaire_reminder_1"),
            ("questionnaire_reminder_2", "fk_engagements_questionnaire_reminder_2"),
        ):
            if _column_exists(inspector, "engagements", col_name) and not _fk_exists(inspector, "engagements", fk_name):
                op.create_foreign_key(
                    fk_name,
                    "engagements",
                    "notification_services",
                    [col_name],
                    ["service_key"],
                )
