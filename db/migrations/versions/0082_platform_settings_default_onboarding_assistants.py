"""Add default onboarding assistant employee IDs to platform_settings.

Revision ID: 0082_platform_default_oa
Revises: 0081_report_url_flags
Create Date: 2026-07-10

Changes:
- platform_settings: default_onboarding_assistant_employee_ids (comma-separated IDs)
- Backfill existing row with "1,8" to preserve current B2C behavior
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0082_platform_default_oa"
down_revision = "0081_report_url_flags"
branch_labels = None
depends_on = None

_COLUMN = "default_onboarding_assistant_employee_ids"
_BACKFILL_VALUE = "1,8"


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "platform_settings"):
        if not _column_exists(inspector, "platform_settings", _COLUMN):
            op.add_column(
                "platform_settings",
                sa.Column(_COLUMN, sa.String(length=500), nullable=True),
            )

        op.execute(
            sa.text(
                f"""
                UPDATE platform_settings
                SET {_COLUMN} = :backfill
                WHERE settings_id = 1 AND ({_COLUMN} IS NULL OR TRIM({_COLUMN}) = '')
                """
            ).bindparams(backfill=_BACKFILL_VALUE)
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "platform_settings") and _column_exists(
        inspector, "platform_settings", _COLUMN
    ):
        op.drop_column("platform_settings", _COLUMN)
