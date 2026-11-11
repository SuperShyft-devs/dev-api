"""Drop legacy unique indexes on engagement_notifications (multi-slot bindings)."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0207_drop_legacy_en_idx"
down_revision = "0206_en_notif_slot_uq"
branch_labels = None
depends_on = None

_LEGACY_UNIQUE_NAMES = (
    "uq_engagement_notifications_eng_event_recipient",
    "uq_engagement_notification_event",
    "engagement_notifications_engagement_id_notification_event_id_key",
)


def upgrade() -> None:
    conn = op.get_bind()
    for name in _LEGACY_UNIQUE_NAMES:
        conn.execute(text(f'DROP INDEX IF EXISTS public."{name}"'))
        conn.execute(
            text(f'ALTER TABLE engagement_notifications DROP CONSTRAINT IF EXISTS "{name}"')
        )


def downgrade() -> None:
    pass
