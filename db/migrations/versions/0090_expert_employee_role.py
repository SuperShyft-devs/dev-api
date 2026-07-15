"""Add expert employee role to employee_role enum.

Revision ID: 0090_expert_employee_role
Revises: 0089_participant_consult
Create Date: 2026-07-15
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text


revision = "0090_expert_employee_role"
down_revision = "0089_participant_consult"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    # Same pattern as 0088: ADD VALUE then COMMIT so Postgres allows using the
    # new enum value in later statements / the next revision (0091 backfill).
    # Alembic wraps upgrade head in one transaction (see env.py).
    connection.execute(text("ALTER TYPE employee_role ADD VALUE IF NOT EXISTS 'expert'"))
    connection.execute(text("COMMIT"))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
