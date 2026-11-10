"""Ensure integration_provider_enum has direct-send providers (staging stamp drift)."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0192_integration_provider"
down_revision = "0191_notif_binding_fixup"
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
