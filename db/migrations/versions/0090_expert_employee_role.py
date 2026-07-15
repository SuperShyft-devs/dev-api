"""Add expert employee role and backfill from experts.

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
    with op.get_context().autocommit_block():
        op.execute(text("ALTER TYPE employee_role ADD VALUE IF NOT EXISTS 'expert'"))

    op.execute(
        text(
            """
            INSERT INTO employee (user_id, role, status)
            SELECT DISTINCT e.user_id, 'expert', 'active'
            FROM experts e
            WHERE e.user_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM employee emp WHERE emp.user_id = e.user_id
              )
            """
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
