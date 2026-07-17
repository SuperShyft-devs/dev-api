"""Add consultation_summary and attachments to consultation_bookings.

Revision ID: 0098_consult_summary_attach
Revises: 0097_consultation_bookings
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import ARRAY


revision = "0098_consult_summary_attach"
down_revision = "0097_consultation_bookings"
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

    if not _table_exists(inspector, "consultation_bookings"):
        return

    if not _column_exists(inspector, "consultation_bookings", "consultation_summary"):
        op.add_column(
            "consultation_bookings",
            sa.Column("consultation_summary", sa.Text(), nullable=True),
        )

    if not _column_exists(inspector, "consultation_bookings", "attachments"):
        op.add_column(
            "consultation_bookings",
            sa.Column("attachments", ARRAY(sa.Text()), nullable=True),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
