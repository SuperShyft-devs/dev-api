"""Backfill employee(role=expert) for existing experts with user_id.

Revision ID: 0091_expert_employee_backfill
Revises: 0090_expert_employee_role
Create Date: 2026-07-15
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text


revision = "0091_expert_employee_backfill"
down_revision = "0090_expert_employee_role"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
            INSERT INTO employee (user_id, role, status)
            SELECT DISTINCT e.user_id, 'expert'::employee_role, 'active'
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
