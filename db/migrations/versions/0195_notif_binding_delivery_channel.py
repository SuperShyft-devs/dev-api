"""One notification_events row per recipient + delivery channel (whatsapp/email)."""

from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0195_notif_binding_channel"
down_revision = "0194_support_resolution_fixup"
branch_labels = None
depends_on = None


def _channel_for_service_key(conn, service_key: str) -> str:
    row = conn.execute(
        text("SELECT channel::text FROM notification_services WHERE service_key = :sk"),
        {"sk": service_key},
    ).fetchone()
    if row and row[0]:
        return str(row[0]).strip().lower()
    return "unknown"


def upgrade() -> None:
    op.add_column(
        "notification_events",
        sa.Column("delivery_channel", sa.String(), nullable=True),
    )
    conn = op.get_bind()
    rows = conn.execute(
        text(
            """
            SELECT id, event_code, event_name, recipient_type,
                   notification_services, engagement_type_id, recipient_config
            FROM notification_events
            ORDER BY id
            """
        )
    ).fetchall()

    for row in rows:
        raw = row.notification_services
        if isinstance(raw, str):
            services = json.loads(raw) if raw else []
        elif isinstance(raw, list):
            services = raw
        else:
            services = []

        normalized: list[dict] = []
        for item in services:
            if isinstance(item, dict) and item.get("service_key"):
                normalized.append(item)
            elif isinstance(item, str) and item.strip():
                normalized.append({"service_key": item.strip()})

        if not normalized:
            conn.execute(
                text(
                    "UPDATE notification_events SET delivery_channel = 'unknown' WHERE id = :id"
                ),
                {"id": row.id},
            )
            continue

        for index, svc_item in enumerate(normalized):
            sk = str(svc_item["service_key"])
            channel = _channel_for_service_key(conn, sk)
            payload = json.dumps([svc_item])
            rc = row.recipient_config
            rc_json = json.dumps(rc) if rc is not None else None

            if index == 0:
                conn.execute(
                    text(
                        """
                        UPDATE notification_events
                        SET notification_services = CAST(:services AS json),
                            delivery_channel = :channel
                        WHERE id = :id
                        """
                    ),
                    {"id": row.id, "services": payload, "channel": channel},
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO notification_events (
                            event_code, event_name, recipient_type,
                            notification_services, engagement_type_id,
                            recipient_config, delivery_channel
                        )
                        VALUES (
                            :event_code, :event_name, :recipient_type,
                            CAST(:services AS json), :engagement_type_id,
                            CAST(:recipient_config AS jsonb), :channel
                        )
                        """
                    ),
                    {
                        "event_code": row.event_code,
                        "event_name": row.event_name,
                        "recipient_type": row.recipient_type,
                        "services": payload,
                        "engagement_type_id": row.engagement_type_id,
                        "recipient_config": rc_json,
                        "channel": channel,
                    },
                )

    op.execute(
        text(
            "UPDATE notification_events SET delivery_channel = 'unknown' "
            "WHERE delivery_channel IS NULL"
        )
    )
    op.alter_column("notification_events", "delivery_channel", nullable=False)

    op.drop_constraint("uq_notification_events_binding", "notification_events", type_="unique")
    op.create_unique_constraint(
        "uq_notification_events_binding",
        "notification_events",
        ["event_code", "recipient_type", "engagement_type_id", "delivery_channel"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_notification_events_binding", "notification_events", type_="unique")
    op.create_unique_constraint(
        "uq_notification_events_binding",
        "notification_events",
        ["event_code", "recipient_type", "engagement_type_id"],
    )
    op.drop_column("notification_events", "delivery_channel")
