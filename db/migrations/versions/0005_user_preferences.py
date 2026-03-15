"""Create user preferences table.

Revision ID: 0005_user_preferences
Revises: 0004_qnr_multi_category_progress
Create Date: 2026-03-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0005_user_preferences"
down_revision = "0004_qnr_multi_category_progress"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "user_preferences"):
        return

    op.create_table(
        "user_preferences",
        sa.Column("preference_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("push_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("email_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("sms_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("access_to_files", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("store_downloaded_files", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.UniqueConstraint("user_id", name="uq_user_preferences_user_id"),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
