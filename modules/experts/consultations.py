"""Helpers for participant consultation preference JSON."""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Any

from core.exceptions import AppError


def empty_preference(*, want: bool = False) -> dict[str, Any]:
    return {
        "want": want,
        "date": None,
        "slot": None,
        "expert_id": None,
        "done": False,
        "meet_link": None,
        "consent": empty_consent(),
        "consultation_id": None,
        "consultation_summary": None,
        "attachments": None,
    }


def empty_consent() -> dict[str, bool]:
    return {"bio_ai": False, "blood_report": False, "questionnaire": False}


def normalize_consent(value: Any) -> dict[str, bool]:
    base = empty_consent()
    if isinstance(value, dict):
        for key in base:
            if key in value:
                base[key] = bool(value[key])
    return base


def normalize_preference(value: Any) -> dict[str, Any]:
    """Normalize a single expert-type consultation value to the object shape."""
    if value is None:
        return empty_preference(want=False)
    if isinstance(value, bool):
        return empty_preference(want=value)
    if isinstance(value, dict):
        want = bool(value.get("want", False))
        date_val = value.get("date")
        slot_val = value.get("slot")
        expert_id = value.get("expert_id")
        if expert_id is not None:
            try:
                expert_id = int(expert_id)
            except (TypeError, ValueError):
                expert_id = None
        return {
            "want": want,
            "date": str(date_val) if date_val else None,
            "slot": str(slot_val) if slot_val else None,
            "expert_id": expert_id,
            "done": bool(value.get("done", False)),
            "meet_link": str(value.get("meet_link")) if value.get("meet_link") else None,
            "consent": normalize_consent(value.get("consent")),
            "consultation_id": value.get("consultation_id"),
            "consultation_summary": (
                str(value.get("consultation_summary"))
                if value.get("consultation_summary") is not None
                else None
            ),
            "attachments": (
                [str(item) for item in value.get("attachments")]
                if isinstance(value.get("attachments"), list)
                else None
            ),
        }
    return empty_preference(want=False)


def normalize_consultations_map(raw: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    raw = raw if isinstance(raw, dict) else {}
    return {str(key): normalize_preference(value) for key, value in raw.items()}


def preference_wants(value: Any) -> bool:
    return bool(normalize_preference(value).get("want"))


def validate_requested_consultations(
    requested: dict[str, Any] | None,
    allowed: dict[str, bool] | None,
) -> dict[str, dict[str, Any]]:
    """Reject want=true unless enabled on the engagement. Returns normalized map."""
    requested_norm = normalize_consultations_map(requested)
    allowed = allowed if isinstance(allowed, dict) else {}
    invalid = sorted(
        key
        for key, pref in requested_norm.items()
        if pref.get("want") is True and allowed.get(key) is not True
    )
    if invalid:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=f"Consultation not available for this engagement: {', '.join(invalid)}",
        )
    return requested_norm


def normalize_hhmm(slot: str) -> str:
    """Normalize '10:00' or '10:00:00' to 'HH:MM'."""
    parts = (slot or "").strip().split(":")
    if len(parts) < 2:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid slot time")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError as exc:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid slot time") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid slot time")
    return f"{hour:02d}:{minute:02d}"


def consultation_datetime(date_str: str | None, slot_str: str | None) -> datetime | None:
    """Parse preference date + slot into a naive local datetime, or None if incomplete/invalid."""
    if not date_str or not slot_str:
        return None
    try:
        day = date.fromisoformat(str(date_str)[:10])
        hhmm = normalize_hhmm(str(slot_str))
        hour, minute = map(int, hhmm.split(":"))
        return datetime.combine(day, time(hour=hour, minute=minute))
    except (AppError, TypeError, ValueError):
        return None


def is_upcoming_slot(date_str: str | None, slot_str: str | None, *, now: datetime | None = None) -> bool:
    """True when date+slot is today-or-later and not already past relative to now."""
    when = consultation_datetime(date_str, slot_str)
    if when is None:
        return False
    current = now or datetime.now()
    return when >= current


def booking_to_api_preference(booking: Any) -> dict[str, Any]:
    """Map a ConsultationBooking row to the legacy API preference shape."""
    date_val = booking.consultation_date.isoformat() if booking.consultation_date else None
    attachments = booking.attachments
    if attachments is not None and not isinstance(attachments, list):
        attachments = list(attachments)
    return {
        "consultation_id": booking.consultation_id,
        "want": bool(booking.want),
        "date": date_val,
        "slot": booking.consultation_slot,
        "expert_id": booking.expert_id,
        "done": bool(booking.done),
        "meet_link": booking.meet_link,
        "consent": normalize_consent(booking.consent),
        "consultation_summary": booking.consultation_summary,
        "attachments": attachments,
    }


def bookings_to_consultations_map(bookings: list[Any]) -> dict[str, dict[str, Any]]:
    return {str(booking.expert_type): booking_to_api_preference(booking) for booking in bookings}
