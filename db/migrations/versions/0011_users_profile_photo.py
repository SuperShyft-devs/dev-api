"""Add profile_photo field to users.

Revision ID: 0011_users_profile_photo
Revises: 0010_user_preferences_nutrition
Create Date: 2026-03-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0011_users_profile_photo"
down_revision = "0010_user_preferences_nutrition"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("profile_photo", sa.String(length=500), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
