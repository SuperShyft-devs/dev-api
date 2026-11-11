"""Merge type-scoped notification_events into global defaults; delete type rows."""

from __future__ import annotations

import json

from alembic import op
from sqlalchemy import text

revision = "0203_merge_type_bindings_global"
down_revision = "0202_notif_emit_condition"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    type_rows = conn.execute(
        text(
            """
            SELECT id, event_code, event_name, recipient_type, delivery_channel,
                   notification_services, recipient_config, condition_key, emit_condition,
                   engagement_type_id
            FROM notification_events
            WHERE engagement_type_id IS NOT NULL
            ORDER BY id
            """
        )
    ).fetchall()

    for row in type_rows:
        exists = conn.execute(
            text(
                """
                SELECT 1 FROM notification_events
                WHERE event_code = :event_code
                  AND recipient_type = :recipient_type
                  AND delivery_channel = :delivery_channel
                  AND condition_key = :condition_key
                  AND engagement_type_id IS NULL
                LIMIT 1
                """
            ),
            {
                "event_code": row.event_code,
                "recipient_type": row.recipient_type,
                "delivery_channel": row.delivery_channel,
                "condition_key": row.condition_key or "default",
            },
        ).fetchone()
        if exists:
            continue
        services = row.notification_services
        if isinstance(services, str):
            services = json.loads(services)
        rc = row.recipient_config
        rc_json = json.dumps(rc) if rc is not None and not isinstance(rc, str) else rc
        ec = row.emit_condition
        ec_json = json.dumps(ec) if ec is not None and not isinstance(ec, str) else ec
        conn.execute(
            text(
                """
                INSERT INTO notification_events (
                    event_code, event_name, recipient_type, delivery_channel,
                    notification_services, engagement_type_id, recipient_config,
                    condition_key, emit_condition
                )
                VALUES (
                    :event_code, :event_name, :recipient_type, :delivery_channel,
                    CAST(:services AS jsonb), NULL,
                    CAST(:recipient_config AS jsonb),
                    :condition_key,
                    CAST(:emit_condition AS jsonb)
                )
                """
            ),
            {
                "event_code": row.event_code,
                "event_name": row.event_name,
                "recipient_type": row.recipient_type,
                "delivery_channel": row.delivery_channel,
                "services": json.dumps(services if services is not None else []),
                "recipient_config": rc_json,
                "condition_key": row.condition_key or "default",
                "emit_condition": ec_json,
            },
        )

    conn.execute(text("DELETE FROM notification_events WHERE engagement_type_id IS NOT NULL"))


def downgrade() -> None:
    pass
