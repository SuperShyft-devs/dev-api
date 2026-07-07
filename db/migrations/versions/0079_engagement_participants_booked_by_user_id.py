"""Add booked_by_user_id to engagement_participants.

Revision ID: 0079_ep_booked_by_user_id
Revises: 0078_eng_notif_refactor
Create Date: 2026-07-07

New column:
- engagement_participants.booked_by_user_id (FK users.user_id, NOT NULL after backfill)
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0079_ep_booked_by_user_id"
down_revision = "0078_eng_notif_refactor"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(idx.get("name") == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _column_exists(inspector, "engagement_participants", "booked_by_user_id"):
        op.add_column(
            "engagement_participants",
            sa.Column("booked_by_user_id", sa.Integer(), nullable=True),
        )
        op.create_foreign_key(
            "fk_ep_booked_by_user_id",
            "engagement_participants",
            "users",
            ["booked_by_user_id"],
            ["user_id"],
        )
        op.execute(
            "UPDATE engagement_participants SET booked_by_user_id = user_id "
            "WHERE booked_by_user_id IS NULL"
        )
        op.alter_column("engagement_participants", "booked_by_user_id", nullable=False)

    if not _index_exists(inspector, "engagement_participants", "ix_ep_booked_by_user_id"):
        op.create_index(
            "ix_ep_booked_by_user_id",
            "engagement_participants",
            ["booked_by_user_id"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
