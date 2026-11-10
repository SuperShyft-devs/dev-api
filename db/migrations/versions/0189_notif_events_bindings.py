"""Notification event bindings table; drop legacy auto_notification_events.

Revision ID: 0189_notif_events_bindings
Revises: 0188_notif_integration_providers
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision = "0189_notif_events_bindings"
down_revision = "0188_notif_integration_providers"
branch_labels = None
depends_on = None

_LEGACY_TO_NEW_SQL = """
CASE ane.event_code
    WHEN 'onboarding' THEN 'users.onboarded'
    WHEN 'pretest_guidelines' THEN 'reminders.pretest'
    WHEN 'questionnaire_reminder_before' THEN 'reminders.questionnaire_before'
    WHEN 'questionnaire_reminder_after' THEN 'reminders.questionnaire_after'
    WHEN 'blood_report_ready' THEN 'reports.blood_ready'
    WHEN 'bioai_report_ready' THEN 'reports.bioai_ready'
    WHEN 'report_ready' THEN 'reports.face_scan_ready'
    WHEN 'consultation_ready' THEN 'consultations.ready_to_book'
    WHEN 'consultation_remainder' THEN 'consultations.reminder'
    WHEN 'consultation_booking_alert' THEN 'assistants.consultation_booked'
    WHEN 'booking_guide' THEN 'reminders.booking_guide'
    ELSE ane.event_code
END
"""

_RECIPIENT_SQL = """
CASE ane.event_code
    WHEN 'onboarding' THEN 'assistants'
    WHEN 'booking_guide' THEN 'assistants.phlebo'
    WHEN 'consultation_booking_alert' THEN 'assistants'
    ELSE 'user'
END
"""

_EVENT_NAME_SQL = """
CASE ane.event_code
    WHEN 'onboarding' THEN 'User onboarded to engagement'
    WHEN 'pretest_guidelines' THEN 'Pretest guidelines'
    WHEN 'questionnaire_reminder_before' THEN 'Questionnaire reminder (day before)'
    WHEN 'questionnaire_reminder_after' THEN 'Questionnaire reminder (day after)'
    WHEN 'blood_report_ready' THEN 'Blood report ready'
    WHEN 'bioai_report_ready' THEN 'BioAI report ready'
    WHEN 'report_ready' THEN 'Face scan report ready'
    WHEN 'consultation_ready' THEN 'Ready to book consultation'
    WHEN 'consultation_remainder' THEN 'Consultation reminder'
    WHEN 'consultation_booking_alert' THEN 'Consultation booked (assistants)'
    WHEN 'booking_guide' THEN 'Booking guide for phlebos'
    ELSE ane.display_name
END
"""


def upgrade() -> None:
    op.create_table(
        "notification_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_code", sa.String(), nullable=False),
        sa.Column("event_name", sa.String(), nullable=False),
        sa.Column("recipient_type", sa.String(), nullable=False),
        sa.Column(
            "notification_services",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'"),
        ),
        sa.Column(
            "engagement_type_id",
            sa.Integer(),
            sa.ForeignKey("engagement_types.id"),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "event_code",
            "recipient_type",
            "engagement_type_id",
            name="uq_notification_events_binding",
        ),
    )
    op.create_index("ix_notification_events_event_code", "notification_events", ["event_code"])

    # Type-level defaults from engagement_notification_defaults
    op.execute(
        text(
            f"""
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, notification_services, engagement_type_id
            )
            SELECT
                {_LEGACY_TO_NEW_SQL},
                {_EVENT_NAME_SQL},
                {_RECIPIENT_SQL},
                endef.notification_services,
                endef.engagement_type_id
            FROM engagement_notification_defaults endef
            JOIN auto_notification_events ane ON ane.id = endef.notification_event_id
            """
        )
    )

    # Global platform bindings (no engagement type)
    _seed_global_bindings()

    # engagement_notifications: add event_code, backfill, drop legacy FK column
    op.add_column("engagement_notifications", sa.Column("event_code", sa.String(), nullable=True))
    op.execute(
        text(
            f"""
            UPDATE engagement_notifications en
            SET event_code = sub.mapped_code
            FROM (
                SELECT
                    en2.id AS en_id,
                    ({_LEGACY_TO_NEW_SQL.replace("ane.", "ane2.")}) AS mapped_code
                FROM engagement_notifications en2
                JOIN auto_notification_events ane2 ON ane2.id = en2.notification_event_id
            ) sub
            WHERE en.id = sub.en_id
            """
        )
    )
    op.execute(
        text(
            """
            UPDATE engagement_notifications
            SET event_code = 'unknown'
            WHERE event_code IS NULL
            """
        )
    )
    op.alter_column("engagement_notifications", "event_code", nullable=False)

    op.execute(
        text(
            """
            ALTER TABLE engagement_notifications
              DROP CONSTRAINT IF EXISTS uq_engagement_notification_event,
              DROP CONSTRAINT IF EXISTS engagement_notifications_engagement_id_notification_event_id_key
            """
        )
    )
    op.execute(
        text(
            "ALTER TABLE engagement_notifications "
            "DROP CONSTRAINT IF EXISTS engagement_notifications_notification_event_id_fkey"
        )
    )
    op.drop_column("engagement_notifications", "notification_event_id")
    op.create_unique_constraint(
        "uq_engagement_notification_event",
        "engagement_notifications",
        ["engagement_id", "event_code"],
    )

    op.drop_table("engagement_notification_defaults")
    op.drop_table("auto_notification_events")


def _seed_global_bindings() -> None:
    """Platform-wide bindings for sends that do not use engagement_notifications."""
    seeds: list[tuple[str, str, str, str]] = [
        ("auth.otp_sent", "OTP sent", "user", "[]"),
        ("support.ticket_created", "Support ticket created", "employees", "[]"),
        ("bookings.confirmed", "Booking confirmed", "user", "[]"),
        ("bookings.phlebo_assigned", "Phlebo assigned", "user", "[]"),
        ("bookings.phlebo_reassigned", "Phlebo reassigned", "user", "[]"),
        ("bookings.phlebo_enroute", "Phlebo en route", "user", "[]"),
        ("bookings.phlebo_delayed", "Phlebo delayed", "user", "[]"),
        ("family.gift_sent", "Family gift sent", "booker", "[]"),
        ("family.gift_received", "Family gift received", "members", "[]"),
        ("system.cpu_threshold", "CPU threshold alert", "admin", "[]"),
        ("reports.face_scan_ready", "Face scan report ready", "user", "[]"),
    ]
    conn = op.get_bind()
    for event_code, event_name, recipient_type, _ in seeds:
        service_rows = conn.execute(
            text(
                """
                SELECT COALESCE(
                    json_agg(
                        json_build_object('service_key', service_key)
                        ORDER BY service_key
                    ),
                    '[]'::json
                )
                FROM notification_services
                WHERE is_active = true
                  AND (
                    CASE :event_code
                      WHEN 'auth.otp_sent' THEN
                        service_key LIKE '%%otp%%' OR service_key IN ('whatapi-otp', 'email-otp')
                      WHEN 'support.ticket_created' THEN
                        service_key LIKE 'notify-admin%%' OR service_key = 'notify-admin-contact-query'
                      WHEN 'bookings.confirmed' THEN
                        service_key LIKE 'booking-confirmation%%'
                      WHEN 'bookings.phlebo_assigned' THEN
                        service_key LIKE 'phlebo-assigned%%'
                      WHEN 'bookings.phlebo_reassigned' THEN
                        service_key LIKE 'phlebo-reassigned%%'
                      WHEN 'bookings.phlebo_enroute' THEN
                        service_key LIKE 'phlebo-enroute%%'
                      WHEN 'bookings.phlebo_delayed' THEN
                        service_key LIKE 'phlebo-delay%%'
                      WHEN 'family.gift_sent' THEN
                        service_key LIKE 'family-onboard-sender%%'
                      WHEN 'family.gift_received' THEN
                        service_key LIKE 'family-onboard-receiver%%'
                      WHEN 'system.cpu_threshold' THEN
                        service_key IN ('cpu_above_threshold')
                      WHEN 'reports.face_scan_ready' THEN
                        service_key LIKE '%%face%%' OR service_key LIKE '%%aurae%%'
                        OR service_key LIKE 'report-ready%%'
                      ELSE false
                    END
                  )
                """
            ),
            {"event_code": event_code},
        ).scalar()
        services_raw = service_rows if service_rows else []
        if isinstance(services_raw, str):
            services_param = services_raw
        else:
            services_param = json.dumps(services_raw)
        conn.execute(
            text(
                """
                INSERT INTO notification_events (
                    event_code, event_name, recipient_type, notification_services, engagement_type_id
                )
                VALUES (:event_code, :event_name, :recipient_type, :services, NULL)
                ON CONFLICT ON CONSTRAINT uq_notification_events_binding DO NOTHING
                """
            ),
            {
                "event_code": event_code,
                "event_name": event_name,
                "recipient_type": recipient_type,
                "services": services_param,
            },
        )


def downgrade() -> None:
    raise NotImplementedError("0189_notif_events_bindings downgrade not supported")
