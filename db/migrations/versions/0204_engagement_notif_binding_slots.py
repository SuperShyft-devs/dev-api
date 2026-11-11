"""Per-engagement notification binding slots (multi recipient per event)."""

from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

revision = "0204_eng_notif_slots"
down_revision = "0203_merge_type_bindings_global"
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
            UPDATE engagement_notifications en
            SET delivery_channel = COALESCE(
                (
                    SELECT LOWER(ns.channel::text)
                    FROM notification_services ns
                    WHERE ns.service_key = en.notification_services->0->>'service_key'
                    LIMIT 1
                ),
                'whatsapp'
            )
            WHERE en.delivery_channel IS NULL OR en.delivery_channel = ''
            """
        )
    )
    conn.execute(
        text(
            """
            UPDATE engagement_notifications
            SET condition_key = 'default'
            WHERE condition_key IS NULL OR condition_key = ''
            """
        )
    )

    op.drop_constraint("uq_engagement_notification_event", "engagement_notifications", type_="unique")
    op.create_unique_constraint(
        "uq_engagement_notification_slot",
        "engagement_notifications",
        ["engagement_id", "event_code", "recipient_type", "delivery_channel", "condition_key"],
    )
    op.create_index(
        "ix_engagement_notifications_event",
        "engagement_notifications",
        ["engagement_id", "event_code"],
        unique=False,
    )


def downgrade() -> None:
    pass
