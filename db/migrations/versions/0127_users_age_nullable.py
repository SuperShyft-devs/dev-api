"""Make users.age nullable.

Revision ID: 0127_users_age_nullable
Revises: 0126_slot_detail_date_enable

Age is optional when creating users (e.g. phlebo onboarding).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0127_users_age_nullable"
down_revision = "0126_slot_detail_date_enable"
branch_labels = None
depends_on = None


def _column_exists(inspector, table: str, column: str) -> bool:
    return any(col["name"] == column for col in inspector.get_columns(table))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if not _column_exists(inspector, "users", "age"):
        return

    op.alter_column("users", "age", existing_type=sa.Integer(), nullable=True)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if not _column_exists(inspector, "users", "age"):
        return

    op.execute(sa.text("UPDATE users SET age = 0 WHERE age IS NULL"))
    op.alter_column("users", "age", existing_type=sa.Integer(), nullable=False)
