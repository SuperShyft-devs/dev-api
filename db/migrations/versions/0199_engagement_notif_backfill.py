"""Backfill engagement_notifications from global bindings; add consultations.booked user."""

from __future__ import annotations

import json

from alembic import op
from sqlalchemy import text

revision = "0199_engagement_notif_backfill"
down_revision = "0198_welcome_camp13_slots"
branch_labels = None
depends_on = None


def _merge_global_services(conn) -> dict[str, list[dict]]:
    rows = conn.execute(
        text(
            """
            SELECT event_code, notification_services
            FROM notification_events
            WHERE engagement_type_id IS NULL
            ORDER BY event_code, id
            """
        )
    ).fetchall()
    merged: dict[str, list[dict]] = {}
    for event_code, services in rows:
        if not event_code or not services:
            continue
        bucket = merged.setdefault(str(event_code), [])
        seen = {item.get("service_key") for item in bucket if isinstance(item, dict)}
        if isinstance(services, str):
            try:
                services = json.loads(services)
            except json.JSONDecodeError:
                continue
        if not isinstance(services, list):
            continue
        for item in services:
            if not isinstance(item, dict):
                continue
            key = str(item.get("service_key") or "").strip()
            if key and key not in seen:
                seen.add(key)
                bucket.append({"service_key": key, "external_link": item.get("external_link")})
    return merged


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, delivery_channel,
                notification_services, engagement_type_id
            )
            SELECT
                'consultations.booked',
                'Consultation booked',
                'user',
                'whatsapp',
                CAST(:services AS jsonb),
                NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM notification_events
                WHERE event_code = 'consultations.booked'
                  AND recipient_type = 'user'
                  AND delivery_channel = 'whatsapp'
                  AND engagement_type_id IS NULL
            )
            """
        ),
        {
            "services": json.dumps(
                [{"service_key": "book-doctor-consultation-whatsapp", "external_link": None}]
            ),
        },
    )

    scoped_events = (
        "reminders.pretest",
        "reminders.questionnaire_before",
        "reminders.questionnaire_after",
        "reminders.booking_guide",
        "reminders.consultation_ready_to_book",
        "reminders.consultation_reminder",
        "reports.blood_ready",
        "reports.bioai_ready",
        "reports.face_scan_ready",
        "users.onboarded",
    )
    merged = {
        code: services
        for code, services in _merge_global_services(conn).items()
        if code in scoped_events
    }
    engagement_ids = [
        int(row[0])
        for row in conn.execute(text("SELECT engagement_id FROM engagements")).fetchall()
    ]

    for engagement_id in engagement_ids:
        for event_code, services in merged.items():
            if not services:
                continue
            conn.execute(
                text(
                    """
                    INSERT INTO engagement_notifications (engagement_id, event_code, notification_services)
                    VALUES (:eid, :event_code, CAST(:services AS jsonb))
                    ON CONFLICT ON CONSTRAINT uq_engagement_notification_event DO NOTHING
                    """
                ),
                {
                    "eid": engagement_id,
                    "event_code": event_code,
                    "services": json.dumps(services),
                },
            )


def downgrade() -> None:
    pass
