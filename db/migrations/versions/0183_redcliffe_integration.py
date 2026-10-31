"""Redcliffe Labs: diagnostic and integration provider enums.

Revision ID: 0183_redcliffe
Revises: 0182_diagnostic_profile_group
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text


revision = "0183_redcliffe"
down_revision = "0182_diagnostic_profile_group"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(text("ALTER TYPE diagnostic_provider_enum ADD VALUE IF NOT EXISTS 'redcliffe'"))
    op.execute(text("ALTER TYPE integration_provider_enum ADD VALUE IF NOT EXISTS 'redcliffe'"))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
