"""Add questionnaire_reminder_1 and questionnaire_reminder_2 to engagements.

Revision ID: 0055_eng_quest_remind
Revises: 0054_req_otp
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0055_eng_quest_remind"
down_revision = "0054_req_otp"
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

    has_services = _table_exists(inspector, "notification_services")

    for col_name, fk_name in (
        ("questionnaire_reminder_1", "fk_engagements_questionnaire_reminder_1"),
        ("questionnaire_reminder_2", "fk_engagements_questionnaire_reminder_2"),
    ):
        if not _column_exists(inspector, "engagements", col_name):
            op.add_column(
                "engagements",
                sa.Column(col_name, sa.String(), nullable=True),
            )
            if has_services:
                op.create_foreign_key(
                    fk_name,
                    "engagements",
                    "notification_services",
                    [col_name],
                    ["service_key"],
                )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    for col_name, fk_name in (
        ("questionnaire_reminder_2", "fk_engagements_questionnaire_reminder_2"),
        ("questionnaire_reminder_1", "fk_engagements_questionnaire_reminder_1"),
    ):
        if not _column_exists(inspector, "engagements", col_name):
            continue
        try:
            op.drop_constraint(fk_name, "engagements", type_="foreignkey")
        except Exception:
            pass
        op.drop_column("engagements", col_name)
