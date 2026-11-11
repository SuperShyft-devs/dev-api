"""Add emit_condition and condition_key to notification_events bindings."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0202_notif_emit_condition"
down_revision = "0201_engagement_notif_legacy_fk"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        text(
            "ALTER TABLE notification_events "
            "ADD COLUMN IF NOT EXISTS condition_key VARCHAR NOT NULL DEFAULT 'default'"
        )
    )
    op.execute(
        text(
            "ALTER TABLE notification_events "
            "ADD COLUMN IF NOT EXISTS emit_condition JSONB"
        )
    )
    conn = op.get_bind()
    conn.execute(text('ALTER TABLE notification_events DROP CONSTRAINT IF EXISTS "uq_notification_events_binding"'))
    op.create_unique_constraint(
        "uq_notification_events_binding",
        "notification_events",
        [
            "event_code",
            "recipient_type",
            "engagement_type_id",
            "delivery_channel",
            "condition_key",
        ],
    )


def downgrade() -> None:
    op.drop_constraint("uq_notification_events_binding", "notification_events", type_="unique")
    op.drop_column("notification_events", "emit_condition")
    op.drop_column("notification_events", "condition_key")
    op.create_unique_constraint(
        "uq_notification_events_binding",
        "notification_events",
        ["event_code", "recipient_type", "engagement_type_id", "delivery_channel"],
    )
