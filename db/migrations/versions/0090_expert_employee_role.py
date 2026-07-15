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
    # Same pattern as 0066 (organization_manager). Do not use autocommit_block —
    # async Alembic here has no outer transaction for that helper.
    # Backfill that *uses* the new enum value must be a later revision so the
    # ADD VALUE is committed first (Postgres unsafe-use rule).
    op.execute(text("ALTER TYPE employee_role ADD VALUE IF NOT EXISTS 'expert'"))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
