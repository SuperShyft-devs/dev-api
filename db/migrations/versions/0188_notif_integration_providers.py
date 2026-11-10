"""Add notification direct-send providers to integration_provider_enum.

Revision ID: 0188_notif_integration_providers
Revises: 0187_notif_direct_foundation
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0188_notif_integration_providers"
down_revision = "0187_notif_direct_foundation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(text("ALTER TYPE integration_provider_enum ADD VALUE IF NOT EXISTS 'whatapi'"))
    op.execute(text("ALTER TYPE integration_provider_enum ADD VALUE IF NOT EXISTS 'smtp'"))
    op.execute(
        text(
            "ALTER TYPE integration_provider_enum ADD VALUE IF NOT EXISTS 'whatapi_fallback'"
        )
    )


def downgrade() -> None:
    pass
