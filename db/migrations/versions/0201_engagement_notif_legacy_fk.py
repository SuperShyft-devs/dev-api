"""Staging-compat: relax legacy notification_event_id on engagement_notifications."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0201_engagement_notif_legacy_fk"
down_revision = "0200_engagement_notif_recipient"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    cols = {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'engagement_notifications'
                """
            )
        ).fetchall()
    }
    if "notification_event_id" in cols:
        conn.execute(
            text(
                "ALTER TABLE engagement_notifications "
                "ALTER COLUMN notification_event_id DROP NOT NULL"
            )
        )


def downgrade() -> None:
    pass
