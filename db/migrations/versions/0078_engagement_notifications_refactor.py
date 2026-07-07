"""Drop participant_count, rename onboarding notification, add platform defaults.

Revision ID: 0078_eng_notif_refactor
Revises: 0077_engagement_booking_cols
Create Date: 2026-07-07

Changes:
- engagements: drop participant_count
- engagements: notification_service_key -> onboarding_notification (nullable, no FK)
- platform_settings: six default_* notification columns
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0078_eng_notif_refactor"
down_revision = "0077_engagement_booking_cols"
branch_labels = None
depends_on = None

_PLATFORM_DEFAULT_COLUMNS = (
    "default_onboarding_notification",
    "default_pretest_guidelines_notification",
    "default_questionnaire_reminder_1",
    "default_questionnaire_reminder_2",
    "default_blood_report_notification",
    "default_bioai_report_notification",
)


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _fk_exists(inspector: sa.Inspector, table_name: str, fk_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(fk.get("name") == fk_name for fk in inspector.get_foreign_keys(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "engagements"):
        if _fk_exists(inspector, "engagements", "fk_engagements_notification_service_key"):
            op.drop_constraint(
                "fk_engagements_notification_service_key",
                "engagements",
                type_="foreignkey",
            )

        if _column_exists(inspector, "engagements", "notification_service_key") and not _column_exists(
            inspector, "engagements", "onboarding_notification"
        ):
            op.alter_column(
                "engagements",
                "notification_service_key",
                new_column_name="onboarding_notification",
                existing_type=sa.String(),
                nullable=True,
                server_default=None,
            )
        elif not _column_exists(inspector, "engagements", "onboarding_notification"):
            op.add_column(
                "engagements",
                sa.Column("onboarding_notification", sa.String(length=500), nullable=True),
            )

        if _column_exists(inspector, "engagements", "onboarding_notification"):
            op.alter_column(
                "engagements",
                "onboarding_notification",
                existing_type=sa.String(),
                type_=sa.String(length=500),
                nullable=True,
                server_default=None,
            )

        if _column_exists(inspector, "engagements", "participant_count"):
            op.drop_column("engagements", "participant_count")

    if _table_exists(inspector, "platform_settings"):
        for col_name in _PLATFORM_DEFAULT_COLUMNS:
            if not _column_exists(inspector, "platform_settings", col_name):
                op.add_column(
                    "platform_settings",
                    sa.Column(col_name, sa.String(length=500), nullable=True),
                )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
