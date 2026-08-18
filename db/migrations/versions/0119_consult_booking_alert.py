"""Seed consultation_booking_alert event and notification service.

Revision ID: 0119_consult_booking_alert
Revises: 0118_eng_notif_service_configs
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0119_consult_booking_alert"
down_revision = "0118_eng_notif_service_configs"
branch_labels = None
depends_on = None

_EVENT_SEEDS: list[tuple[str, str, str]] = [
    (
        "consultation_booking_alert",
        "Consultation Booking Alert",
        "Alert sent to onboarding assistants when a participant books a consultation slot",
    ),
]

_SERVICE_SEEDS: list[dict] = [
    {
        "service_key": "consultation-booking-alert-whatsapp",
        "display_name": "Consultation Booking Alert",
        "channel": "whatsapp",
        "webhook_path": "/consultation-booking-alert-whatsapp-v1",
        "require_participant_detail": True,
        "require_session_details": True,
    },
]


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            """
            SELECT id FROM engagement_types
            WHERE code IN (
                'consultation',
                'blood_test_with_consultation',
                'bio_ai_with_consultation'
            )
            """
        )
    ).fetchall()
    type_ids = [row.id for row in rows]

    for event_code, display_name, description in _EVENT_SEEDS:
        if type_ids:
            conn.execute(
                sa.text(
                    """
                    INSERT INTO auto_notification_events
                        (engagement_type_ids, event_code, display_name, description)
                    VALUES (:type_ids, :event_code, :display_name, :description)
                    ON CONFLICT (event_code) DO NOTHING
                    """
                ),
                {
                    "type_ids": type_ids,
                    "event_code": event_code,
                    "display_name": display_name,
                    "description": description,
                },
            )

    for svc in _SERVICE_SEEDS:
        conn.execute(
            sa.text(
                """
                INSERT INTO notification_services (
                    service_key,
                    display_name,
                    channel,
                    webhook_path,
                    is_active,
                    require_blood_report_url,
                    require_bio_ai_report_url,
                    require_participant_detail,
                    require_otp,
                    require_session_details,
                    require_external_link
                ) VALUES (
                    :service_key,
                    :display_name,
                    :channel,
                    :webhook_path,
                    true,
                    false,
                    false,
                    :require_participant_detail,
                    false,
                    :require_session_details,
                    false
                )
                ON CONFLICT (service_key) DO NOTHING
                """
            ),
            svc,
        )


def downgrade() -> None:
    conn = op.get_bind()
    for svc in _SERVICE_SEEDS:
        conn.execute(
            sa.text("DELETE FROM notification_services WHERE service_key = :service_key"),
            {"service_key": svc["service_key"]},
        )
    for event_code, _, _ in _EVENT_SEEDS:
        conn.execute(
            sa.text(
                "DELETE FROM auto_notification_events WHERE event_code = :event_code"
            ),
            {"event_code": event_code},
        )
