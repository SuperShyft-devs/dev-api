"""Add nutrition preference fields to user_preferences.

Revision ID: 0010_user_preferences_nutrition
Revises: 0009_diagnostics_expansion
Create Date: 2026-03-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0010_user_preferences_nutrition"
down_revision = "0009_diagnostics_expansion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("user_preferences", sa.Column("diet_preference", sa.String(), nullable=True))
    op.add_column(
        "user_preferences",
        sa.Column("allergies", sa.JSON(), nullable=True, server_default=sa.text("'[]'")),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
