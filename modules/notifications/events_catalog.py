"""Hardcoded notification event catalog (see notification-events.md)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

EventStatus = Literal["current", "add"]


@dataclass(frozen=True, slots=True)
class EventDefinition:
    code: str
    name: str
    status: EventStatus
    variable_groups: tuple[str, ...]
    default_recipients: tuple[str, ...]
    trigger_notes: str = ""
    legacy_event_code: str | None = None


LEGACY_EVENT_CODE_MAP: dict[str, str] = {
    "onboarding": "users.onboarded",
    "pretest_guidelines": "reminders.pretest",
    "questionnaire_reminder_before": "reminders.questionnaire_before",
    "questionnaire_reminder_after": "reminders.questionnaire_after",
    "blood_report_ready": "reports.blood_ready",
    "bioai_report_ready": "reports.bioai_ready",
    "report_ready": "reports.face_scan_ready",
    "consultation_ready": "reminders.consultation_ready_to_book",
    "consultation_remainder": "reminders.consultation_reminder",
    "consultation_booking_alert": "consultations.booked",
    "consultations.ready_to_book": "reminders.consultation_ready_to_book",
    "consultations.reminder": "reminders.consultation_reminder",
    "assistants.consultation_booked": "consultations.booked",
    "booking_guide": "reminders.booking_guide",
}

_LEGACY_SERVICE_PREFIX_MAP: dict[str, str] = {
    "whatapi-otp": "auth.otp_sent",
    "email-otp": "auth.otp_sent",
    "booking-confirmation": "bookings.confirmed",
    "phlebo-assigned": "bookings.phlebo_assigned",
    "phlebo-reassigned": "bookings.phlebo_reassigned",
    "phlebo-enroute": "bookings.phlebo_enroute",
    "phlebo-delay": "bookings.phlebo_delayed",
    "family-onboard-sender": "family.gift_sent",
    "family-onboard-receiver": "family.gift_received",
    "notify-admin-contact-query": "support.ticket_created",
    "cpu_above_threshold": "system.cpu_threshold",
}

EVENTS: tuple[EventDefinition, ...] = (
    EventDefinition(
        code="auth.otp_sent",
        name="OTP sent",
        status="current",
        variable_groups=("user", "otp"),
        default_recipients=("user",),
        trigger_notes="POST /auth/send-otp, resend, employee/partner OTP",
    ),
    EventDefinition(
        code="users.registered",
        name="User registered",
        status="current",
        variable_groups=("user",),
        default_recipients=("user",),
        trigger_notes="POST /users, new user on onboard flows",
    ),
    EventDefinition(
        code="users.onboarded",
        name="User onboarded to engagement",
        status="current",
        variable_groups=("user", "engagement", "session", "booking", "payment"),
        default_recipients=("user", "assistants"),
        trigger_notes="Enrollment into engagement",
        legacy_event_code="onboarding",
    ),
    EventDefinition(
        code="payments.completed",
        name="Payment completed",
        status="add",
        variable_groups=("user", "payment", "engagement"),
        default_recipients=("user",),
        trigger_notes="Confirm payment success",
    ),
    EventDefinition(
        code="payments.failed",
        name="Payment failed",
        status="add",
        variable_groups=("user", "payment", "error"),
        default_recipients=("user",),
        trigger_notes="POST /payments/failed",
    ),
    EventDefinition(
        code="bookings.confirmed",
        name="Booking confirmed",
        status="current",
        variable_groups=("user", "engagement", "session", "booking"),
        default_recipients=("user",),
        trigger_notes="Healthians BS005 / vendor confirm",
    ),
    EventDefinition(
        code="bookings.rescheduled",
        name="Booking rescheduled",
        status="add",
        variable_groups=("user", "engagement", "session", "booking"),
        default_recipients=("user", "assistants"),
        trigger_notes="Reschedule blood test APIs",
    ),
    EventDefinition(
        code="bookings.cancelled",
        name="Booking cancelled",
        status="add",
        variable_groups=("user", "engagement", "booking"),
        default_recipients=("user", "assistants"),
        trigger_notes="Cancel blood/bio-ai booking",
    ),
    EventDefinition(
        code="bookings.phlebo_assigned",
        name="Phlebo assigned",
        status="current",
        variable_groups=("user", "session", "phlebo"),
        default_recipients=("user",),
        trigger_notes="Healthians phlebo_assigned",
    ),
    EventDefinition(
        code="bookings.phlebo_reassigned",
        name="Phlebo reassigned",
        status="current",
        variable_groups=("user", "session", "phlebo"),
        default_recipients=("user",),
        trigger_notes="Healthians phlebo_reassigned",
    ),
    EventDefinition(
        code="bookings.phlebo_enroute",
        name="Phlebo en route",
        status="current",
        variable_groups=("user", "session", "phlebo"),
        default_recipients=("user",),
        trigger_notes="Healthians phlebo_enroute",
    ),
    EventDefinition(
        code="bookings.phlebo_delayed",
        name="Phlebo delayed",
        status="current",
        variable_groups=("user", "session", "phlebo"),
        default_recipients=("user",),
        trigger_notes="Healthians phlebo_delay_notification",
    ),
    EventDefinition(
        code="bookings.sample_collected",
        name="Sample collected",
        status="add",
        variable_groups=("user", "session", "booking"),
        default_recipients=("user",),
        trigger_notes="Healthians sample_collected_confirmation",
    ),
    EventDefinition(
        code="consultations.booked",
        name="Consultation booked",
        status="current",
        variable_groups=("user", "engagement", "consultation"),
        default_recipients=("user", "assistants"),
        trigger_notes="Participant books consultation; bind user and assistants separately",
        legacy_event_code="consultation_booking_alert",
    ),
    EventDefinition(
        code="consultations.confirmed",
        name="Consultation confirmed",
        status="add",
        variable_groups=("user", "consultation"),
        default_recipients=("user",),
        trigger_notes="Expert portal confirm",
    ),
    EventDefinition(
        code="consultations.rescheduled",
        name="Consultation rescheduled",
        status="add",
        variable_groups=("user", "consultation"),
        default_recipients=("user", "assistants.expert"),
        trigger_notes="Consultation reschedule",
    ),
    EventDefinition(
        code="consultations.cancelled",
        name="Consultation cancelled",
        status="add",
        variable_groups=("user", "consultation"),
        default_recipients=("user", "assistants.expert"),
        trigger_notes="Consultation cancel",
    ),
    EventDefinition(
        code="consultations.completed",
        name="Consultation completed",
        status="add",
        variable_groups=("user", "consultation"),
        default_recipients=("user",),
        trigger_notes="Expert marks done",
    ),
    EventDefinition(
        code="reports.blood_ready",
        name="Blood report ready",
        status="current",
        variable_groups=("user", "engagement", "report"),
        default_recipients=("user",),
        trigger_notes="Cron load_blood_reports",
        legacy_event_code="blood_report_ready",
    ),
    EventDefinition(
        code="reports.bioai_ready",
        name="BioAI report ready",
        status="current",
        variable_groups=("user", "engagement", "report"),
        default_recipients=("user",),
        trigger_notes="Cron load_bioai_reports",
        legacy_event_code="bioai_report_ready",
    ),
    EventDefinition(
        code="reports.face_scan_ready",
        name="Face scan report ready",
        status="current",
        variable_groups=("user", "engagement", "report"),
        default_recipients=("user",),
        trigger_notes="Aurae webhook",
        legacy_event_code="report_ready",
    ),
    EventDefinition(
        code="reminders.pretest",
        name="Pretest guidelines",
        status="current",
        variable_groups=("user", "engagement", "session"),
        default_recipients=("user",),
        trigger_notes="Cron day-before blood test",
        legacy_event_code="pretest_guidelines",
    ),
    EventDefinition(
        code="reminders.questionnaire_before",
        name="Questionnaire reminder (day before)",
        status="current",
        variable_groups=("user", "engagement"),
        default_recipients=("user",),
        trigger_notes="Cron day-before collection",
        legacy_event_code="questionnaire_reminder_before",
    ),
    EventDefinition(
        code="reminders.questionnaire_after",
        name="Questionnaire reminder (day after)",
        status="current",
        variable_groups=("user", "engagement"),
        default_recipients=("user",),
        trigger_notes="Cron day-after collection",
        legacy_event_code="questionnaire_reminder_after",
    ),
    EventDefinition(
        code="reminders.booking_guide",
        name="Booking guide for phlebos",
        status="current",
        variable_groups=("engagement", "session"),
        default_recipients=("assistants.phlebo",),
        trigger_notes="Cron day-before camp collection",
        legacy_event_code="booking_guide",
    ),
    EventDefinition(
        code="reminders.consultation_ready_to_book",
        name="Ready to book consultation",
        status="current",
        variable_groups=("user", "engagement"),
        default_recipients=("user",),
        trigger_notes="Cron blood ready, consult not booked",
        legacy_event_code="consultation_ready",
    ),
    EventDefinition(
        code="reminders.consultation_reminder",
        name="Consultation reminder",
        status="current",
        variable_groups=("user", "consultation"),
        default_recipients=("user",),
        trigger_notes="Cron consult today",
        legacy_event_code="consultation_remainder",
    ),
    EventDefinition(
        code="support.ticket_created",
        name="Support ticket created",
        status="current",
        variable_groups=("staff", "user", "support"),
        default_recipients=("departments.ops",),
        trigger_notes="POST /support/tickets",
    ),
    EventDefinition(
        code="support.ticket_resolved",
        name="Support ticket resolved",
        status="add",
        variable_groups=("staff", "user", "support"),
        default_recipients=("user",),
        trigger_notes="Ticket status resolved",
    ),
    EventDefinition(
        code="system.auto_booking_failed",
        name="Auto booking failed",
        status="add",
        variable_groups=("user", "engagement", "booking", "error"),
        default_recipients=("assistants", "departments.ops"),
        trigger_notes="Vendor auto-book fails",
    ),
    EventDefinition(
        code="system.error",
        name="System error",
        status="add",
        variable_groups=("error", "user", "engagement"),
        default_recipients=("departments.ops",),
        trigger_notes="Unexpected runtime failure",
    ),
    EventDefinition(
        code="system.cpu_threshold",
        name="CPU threshold alert",
        status="current",
        variable_groups=("server",),
        default_recipients=("departments.ops",),
        trigger_notes="Server health CPU alert",
    ),
)

_EVENTS_BY_CODE: dict[str, EventDefinition] = {e.code: e for e in EVENTS}


def map_legacy_event_code(legacy_code: str) -> str:
    return LEGACY_EVENT_CODE_MAP.get(legacy_code, legacy_code)


def map_legacy_service_key_to_event(service_key: str) -> str | None:
    key = (service_key or "").strip()
    if key in _LEGACY_SERVICE_PREFIX_MAP:
        return _LEGACY_SERVICE_PREFIX_MAP[key]
    for prefix, event_code in _LEGACY_SERVICE_PREFIX_MAP.items():
        if key.startswith(prefix):
            return event_code
    return None


def list_events(*, status: EventStatus | None = None) -> list[EventDefinition]:
    if status is None:
        return list(EVENTS)
    return [e for e in EVENTS if e.status == status]


def get_event(code: str) -> EventDefinition | None:
    return _EVENTS_BY_CODE.get(code)


def get_event_by_legacy_code(legacy_code: str) -> EventDefinition | None:
    new_code = map_legacy_event_code(legacy_code)
    return get_event(new_code)


def variables_for_event(code: str) -> tuple[str, ...]:
    ev = get_event(code)
    return ev.variable_groups if ev else ()


def current_event_codes() -> list[str]:
    return [e.code for e in EVENTS if e.status == "current"]


def default_recipient_for_legacy_binding(legacy_event_code: str) -> str:
    """Primary recipient when migrating single-row legacy defaults."""
    ev = get_event_by_legacy_code(legacy_event_code)
    if ev is None or not ev.default_recipients:
        return "user"
    recipient = ev.default_recipients[0]
    if legacy_event_code == "onboarding":
        return "assistants"
    if legacy_event_code == "booking_guide":
        return "assistants.phlebo"
    if legacy_event_code == "consultation_booking_alert":
        return "assistants"
    return recipient
