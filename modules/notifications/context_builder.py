"""Build dispatch context from notification member payloads."""

from __future__ import annotations

from typing import Any


def normalize_whatsapp_phone(phone: str | None) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(digits) == 10:
        return f"91{digits}"
    return digits


def build_member_context(
    member: dict[str, Any],
    *,
    participant_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    session = member.get("session_details") or {}
    if not isinstance(session, dict):
        session = {}

    pd = participant_details or member.get("participant_details") or {}
    if not isinstance(pd, dict):
        pd = {}

    first_name = str(member.get("first_name") or "Participant").strip() or "Participant"

    gift_url = str(pd.get("gift_url") or "").strip()

    context: dict[str, Any] = {
        "user": {
            "first_name": first_name,
            "last_name": str(member.get("last_name") or "").strip(),
            "phone": normalize_whatsapp_phone(member.get("phone")),
            "email": str(member.get("email") or "").strip(),
        },
        "session": {
            "date": str(session.get("date") or "").strip(),
            "time": str(session.get("slot") or session.get("time") or "").strip(),
            "cabin": str(session.get("cabin") or "").strip(),
        },
        "report": {
            "blood_report_url": str(member.get("blood_report_url") or "").strip(),
            "bio_ai_report_url": str(member.get("bio_ai_report_url") or "").strip(),
            "face_scan_report_url": str(member.get("face_scan_report_url") or "").strip(),
        },
        "gift": {
            "booker_name": str(pd.get("booker_name") or "").strip(),
            "booker_phone": str(pd.get("booker_phone") or "").strip(),
            "recipient_name": str(pd.get("recipient_name") or first_name).strip(),
            "relationship": str(pd.get("relationship") or "").strip(),
            "gift_url": gift_url,
            "gift_card_url": gift_url,
            "gift_message": str(pd.get("gift_message") or "").strip(),
        },
        "otp": str(member.get("otp") or "").strip(),
        "participant_details": pd,
        "external_link": str(member.get("external_link") or "").strip(),
    }

    for key in ("engagement", "consultation", "booking"):
        block = member.get(key)
        if isinstance(block, dict) and block:
            context[key] = {k: v for k, v in block.items() if v is not None}

    return context
