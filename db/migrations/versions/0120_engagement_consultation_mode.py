"""Add engagements.consultation_mode enum column.

Revision ID: 0120_engagement_consultation_mode
Revises: 0119_consult_booking_alert
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0120_engagement_consultation_mode"
down_revision = "0119_consult_booking_alert"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    consultation_mode_enum = sa.Enum(
        "online",
        "offline",
        name="consultation_mode_enum",
    )
    consultation_mode_enum.create(connection, checkfirst=True)

    if not _column_exists(inspector, "engagements", "consultation_mode"):
        op.add_column(
            "engagements",
            sa.Column(
                "consultation_mode",
                consultation_mode_enum,
                nullable=True,
            ),
        )

    op.execute(
        sa.text(
            """
            UPDATE engagements
            SET consultation_mode = CASE
                WHEN organization_id IS NOT NULL THEN 'offline'::consultation_mode_enum
                ELSE 'online'::consultation_mode_enum
            END
            WHERE consultation_mode IS NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_column("engagements", "consultation_mode")
    consultation_mode_enum = sa.Enum(name="consultation_mode_enum")
    consultation_mode_enum.drop(op.get_bind(), checkfirst=True)
