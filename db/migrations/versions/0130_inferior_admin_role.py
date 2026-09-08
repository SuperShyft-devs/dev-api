"""Add inferior_admin to employee_role.

Revision ID: 0130_inferior_admin_role
Revises: 0129_merge_indexes_prev_qnr
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text


revision = "0130_inferior_admin_role"
down_revision = "0129_merge_indexes_prev_qnr"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        text("ALTER TYPE employee_role ADD VALUE IF NOT EXISTS 'inferior_admin'")
    )
    # PostgreSQL requires enum additions to commit before a later revision can
    # safely use the new value.
    connection.execute(text("COMMIT"))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
