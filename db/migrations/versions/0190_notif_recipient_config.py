"""Add recipient_config to notification_events bindings."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0190_notif_recipient_config"
down_revision = "0189_notif_events_bindings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "notification_events",
        sa.Column(
            "recipient_config",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("notification_events", "recipient_config")
