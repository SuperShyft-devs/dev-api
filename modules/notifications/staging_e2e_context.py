"""Fixtures and skip rules for staging WhatsApp E2E dispatch script."""

from __future__ import annotations

from typing import Any

from modules.notifications.context_builder import build_member_context
from modules.notifications.models import NotificationService
from modules.notifications.template_variables import resolve_slots

_STAGING_PDF = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"
_STAGING_APP_LINK = "https://app.supershyft.com"

_UNFILLABLE_PREFIXES = (
    "support.",
    "server.",
    "phlebo.",
    "error.",
)

_SMOKE_SERVICE_KEYS = frozenset(
    {
        "whatapi-otp",
        "pretest-whatsapp",
        "welcome-whatsapp",
    }
)


def staging_e2e_member_overlay(*, phone: str, otp: str) -> dict[str, Any]:
    today = "2030-06-15"
    return {
        "otp": otp,
        "external_link": _STAGING_APP_LINK,
        "blood_report_url": _STAGING_PDF,
        "bio_ai_report_url": _STAGING_PDF,
        "face_scan_report_url": _STAGING_PDF,
        "session_details": {
            "date": today,
            "slot": "10:00 AM",
            "cabin": "Cabin A1",
        },
        "engagement": {
            "engagement_id": "9001",
            "engagement_code": "E2E-CAMP",
            "engagement_name": "Staging E2E Camp",
            "engagement_type": "corporate",
            "cabin_name": "Cabin A1",
        },
        "consultation": {
            "expert_type": "general",
            "expert_name": "Dr E2E",
            "meet_link": _STAGING_APP_LINK,
            "date": today,
            "time": "10:00 AM",
            "cabin": "Cabin A1",
        },
        "booking": {
            "booking_id": "E2E-BOOK-1",
            "collection_date": today,
            "collection_time": "08:00 AM",
        },
    }


def merge_e2e_contact(
    contact: dict[str, Any],
    svc: NotificationService,
    *,
    phone: str,
    otp: str,
    participant: dict[str, Any],
    session_details: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = {**contact, **staging_e2e_member_overlay(phone=phone, otp=otp)}
    if svc.require_otp:
        merged["otp"] = otp
    if svc.require_participant_detail and "participant_details" not in merged:
        merged["participant_details"] = participant
    if session_details is not None:
        merged["session_details"] = session_details
    return merged


def _refs_in_template_variables(template_variables: Any) -> list[str]:
    refs: list[str] = []
    if isinstance(template_variables, dict):
        for slot in template_variables.get("slots") or []:
            if isinstance(slot, dict):
                src = slot.get("source")
                if isinstance(src, str) and src.strip():
                    refs.append(src.strip())
    elif isinstance(template_variables, list):
        for item in template_variables:
            if not isinstance(item, dict):
                continue
            for param in item.get("parameters") or []:
                if not isinstance(param, dict):
                    continue
                ref = param.get("ref") or param.get("source")
                if isinstance(ref, str) and ref.strip():
                    refs.append(ref.strip())
    return refs


def _empty_slot_count_after_overlay(
    svc: NotificationService,
    *,
    phone: str,
    otp: str,
    participant: dict[str, Any],
) -> int:
    contact = merge_e2e_contact(
        {"first_name": "E2E", "last_name": "User", "phone": phone, "email": ""},
        svc,
        phone=phone,
        otp=otp,
        participant=participant,
        session_details=None,
    )
    context = build_member_context(contact, participant_details=contact.get("participant_details"))
    values = resolve_slots(svc.template_variables, context)
    return sum(1 for v in values if not str(v or "").strip())


def e2e_skip_reason(
    svc: NotificationService,
    *,
    phone: str,
    otp: str,
    participant: dict[str, Any],
) -> str | None:
    for ref in _refs_in_template_variables(svc.template_variables):
        lower = ref.lower()
        for prefix in _UNFILLABLE_PREFIXES:
            if lower.startswith(prefix) or f".{prefix.rstrip('.')}" in lower:
                return f"template references {ref} (no staging fixture)"

    empty = _empty_slot_count_after_overlay(
        svc, phone=phone, otp=otp, participant=participant
    )
    if empty > 1:
        return f"{empty} template body slots still empty after E2E overlay"
    return None


def service_keys_for_mode(mode: str) -> frozenset[str] | None:
    if mode == "smoke":
        return _SMOKE_SERVICE_KEYS
    return None
