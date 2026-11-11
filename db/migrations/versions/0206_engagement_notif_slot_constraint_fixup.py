"""Normalize engagement_notifications unique constraints for multi-slot bindings."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0206_en_notif_slot_uq"
down_revision = "0205_en_source_binding"
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
    if "delivery_channel" not in cols:
        op.add_column(
            "engagement_notifications",
            sa.Column("delivery_channel", sa.String(), nullable=False, server_default="whatsapp"),
        )
    if "recipient_config" not in cols:
        op.add_column(
            "engagement_notifications",
            sa.Column("recipient_config", JSONB(astext_type=sa.Text()), nullable=True),
        )
    if "condition_key" not in cols:
        op.add_column(
            "engagement_notifications",
            sa.Column("condition_key", sa.String(), nullable=False, server_default="default"),
        )
    if "emit_condition" not in cols:
        op.add_column(
            "engagement_notifications",
            sa.Column("emit_condition", JSONB(astext_type=sa.Text()), nullable=True),
        )

    conn.execute(
        text(
            """
            UPDATE engagement_notifications
            SET delivery_channel = COALESCE(NULLIF(delivery_channel, ''), 'whatsapp'),
                condition_key = COALESCE(NULLIF(condition_key, ''), 'default')
            """
        )
    )

    for constraint in (
        "uq_engagement_notification_event",
        "uq_engagement_notifications_eng_event_recipient",
        "engagement_notifications_engagement_id_notification_event_id_key",
        "uq_engagement_notification_slot",
    ):
        conn.execute(text(f'DROP INDEX IF EXISTS public."{constraint}"'))
        conn.execute(
            text(
                f'ALTER TABLE engagement_notifications DROP CONSTRAINT IF EXISTS "{constraint}"'
            )
        )

    op.create_unique_constraint(
        "uq_engagement_notification_slot",
        "engagement_notifications",
        ["engagement_id", "event_code", "recipient_type", "delivery_channel", "condition_key"],
    )
    op.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ix_engagement_notifications_event
            ON engagement_notifications (engagement_id, event_code)
            """
        )
    )


def downgrade() -> None:
    pass
