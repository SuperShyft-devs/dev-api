"""Classify catalog events by trigger source (API vs cron vs webhook)."""

from __future__ import annotations

from typing import Literal

from modules.notifications.events_catalog import EventDefinition

TriggerSource = Literal["api", "webhook", "cron", "system"]

_CRON_CODES = frozenset(
    {
        "reminders.pretest",
        "reminders.questionnaire_before",
        "reminders.questionnaire_after",
        "reminders.booking_guide",
        "reminders.consultation_ready_to_book",
        "reminders.consultation_reminder",
        "reports.blood_ready",
        "reports.bioai_ready",
    }
)

_WEBHOOK_CODES = frozenset(
    {
        "bookings.confirmed",
        "bookings.phlebo_assigned",
        "bookings.phlebo_reassigned",
        "bookings.phlebo_enroute",
        "bookings.phlebo_delayed",
        "bookings.sample_collected",
        "reports.face_scan_ready",
    }
)

_SYSTEM_CODES = frozenset(
    {
        "system.auto_booking_failed",
        "system.error",
        "system.cpu_threshold",
    }
)


def trigger_source_for_event(ev: EventDefinition) -> TriggerSource:
    code = ev.code
    if code in _CRON_CODES:
        return "cron"
    if code in _WEBHOOK_CODES:
        return "webhook"
    if code in _SYSTEM_CODES or code.startswith("system."):
        return "system"
    notes = (ev.trigger_notes or "").lower()
    if notes.startswith("cron") or " cron " in f" {notes} ":
        return "cron"
    if "webhook" in notes or "healthians" in notes:
        return "webhook"
    return "api"


def allows_payload_recipient(ev: EventDefinition) -> bool:
    return trigger_source_for_event(ev) == "api"
