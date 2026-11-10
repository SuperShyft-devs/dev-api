"""Idempotent fix for notification_events when table predates full 0189 apply.

Staging was stamped through 0189 without create_table; this aligns columns/constraints.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0191_notif_binding_fixup"
down_revision = "0190_notif_recipient_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            """
            ALTER TABLE notification_events
            ADD COLUMN IF NOT EXISTS engagement_type_id INTEGER
            REFERENCES engagement_types (id)
            """
        )
    )
    op.execute(
        text(
            """
            ALTER TABLE notification_events
            ADD COLUMN IF NOT EXISTS recipient_config JSONB
            """
        )
    )
    op.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ix_notification_events_event_code
            ON notification_events (event_code)
            """
        )
    )


def downgrade() -> None:
    pass
