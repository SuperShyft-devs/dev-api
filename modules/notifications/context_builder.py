"""Build dispatch context from notification member payloads."""

from __future__ import annotations

from typing import Any


def normalize_whatsapp_phone(phone: str | None) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(digits) == 10:
        return f"91{digits}"
    return digits


def format_phone_display(phone: str | None) -> str:
    """Human-readable phone for template slots (not WhatsApp E.164 routing)."""
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(digits) == 12 and digits.startswith("91"):
        return digits[2:]
    if len(digits) == 11 and digits.startswith("0"):
        return digits[1:]
    return digits or str(phone or "").strip()


def _strip_placeholder(value: object | None) -> str:
    text = str(value or "").strip()
    return "" if text == "-" else text


def _participant_block_from_pd(
    pd: dict[str, Any],
    *,
    fallback_member: dict[str, Any] | None = None,
) -> dict[str, str]:
    name = _strip_placeholder(pd.get("name"))
    first = _strip_placeholder(pd.get("first_name"))
    last = _strip_placeholder(pd.get("last_name"))
    if not first and name:
        parts = name.split(None, 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""
    if not name:
        name = f"{first} {last}".strip()
    if not name and fallback_member:
        mf = _strip_placeholder(fallback_member.get("first_name"))
        ml = _strip_placeholder(fallback_member.get("last_name"))
        first = first or mf
        last = last or ml
        name = f"{first} {ml}".strip() or first
    phone = _strip_placeholder(pd.get("phone"))
    if not phone and fallback_member:
        phone = _strip_placeholder(fallback_member.get("phone"))
    display_phone = format_phone_display(phone) if phone else ""
    return {
        "name": name or first or "Participant",
        "first_name": first,
        "last_name": last,
        "phone": display_phone,
        "email": _strip_placeholder(pd.get("email"))
        or _strip_placeholder((fallback_member or {}).get("email")),
        "age": _strip_placeholder(pd.get("age")),
        "gender": _strip_placeholder(pd.get("gender")),
        "address": _strip_placeholder(pd.get("address")),
        "pincode": _strip_placeholder(pd.get("pincode")),
    }


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

    member_first = str(member.get("first_name") or "").strip()
    member_last = str(member.get("last_name") or "").strip()
    member_name = f"{member_first} {member_last}".strip() or member_first or "Team member"

    gift_url = str(pd.get("gift_url") or "").strip()
    otp_value = str(member.get("otp") or "").strip()

    participant_block = _participant_block_from_pd(pd, fallback_member=member)
    has_enrolled_participant = bool(
        pd.get("participant_user_id")
        or _strip_placeholder(pd.get("name"))
        or _strip_placeholder(pd.get("phone"))
        or _strip_placeholder(pd.get("email"))
    )

    pd_user = pd.get("user") if isinstance(pd.get("user"), dict) else {}
    if pd_user:
        user_block = {k: str(v or "").strip() for k, v in pd_user.items()}
    elif has_enrolled_participant:
        user_block = dict(participant_block)
        user_block["phone"] = normalize_whatsapp_phone(
            str(pd.get("phone") or participant_block.get("phone") or "")
        )
    else:
        legacy_first = member_first or "Participant"
        user_block = {
            "first_name": legacy_first,
            "last_name": member_last,
            "name": member_name if member_name != "Team member" else legacy_first,
            "phone": normalize_whatsapp_phone(member.get("phone")),
            "email": str(member.get("email") or "").strip(),
            "address": str(pd.get("address") or member.get("address") or "").strip(),
        }

    session_date = str(session.get("date") or pd.get("collection_date") or "").strip()
    session_time = str(
        session.get("slot") or session.get("time") or pd.get("collection_time") or ""
    ).strip()
    session_cabin = str(session.get("cabin") or pd.get("cabin_name") or "").strip()
    # Strip display placeholders from legacy payloads
    if session_date == "-":
        session_date = ""
    if session_time == "-":
        session_time = ""
    if session_cabin == "-":
        session_cabin = ""

    context: dict[str, Any] = {
        "staff": {
            "name": member_name,
            "first_name": member_first,
            "last_name": member_last,
            "phone": str(member.get("phone") or "").strip(),
            "email": str(member.get("email") or "").strip(),
        },
        "user": user_block,
        "participant": participant_block,
        "session": {
            "date": session_date,
            "time": session_time,
            "cabin": session_cabin,
        },
        "report": {
            "blood_report_url": str(member.get("blood_report_url") or "").strip(),
            "bio_ai_report_url": str(member.get("bio_ai_report_url") or "").strip(),
            "face_scan_report_url": str(member.get("face_scan_report_url") or "").strip(),
        },
        "gift": {
            "booker_name": str(pd.get("booker_name") or "").strip(),
            "booker_phone": str(pd.get("booker_phone") or "").strip(),
            "recipient_name": str(pd.get("recipient_name") or user_block.get("first_name") or "").strip(),
            "relationship": str(pd.get("relationship") or "").strip(),
            "gift_url": gift_url,
            "gift_card_url": gift_url,
            "gift_message": str(pd.get("gift_message") or "").strip(),
        },
        # Catalog variables use otp.otp; keep a string alias for legacy {{otp}} templates.
        "otp": {"otp": otp_value},
        "participant_details": pd,
        "external_link": str(member.get("external_link") or "").strip(),
    }

    support_pd = pd.get("support") if isinstance(pd.get("support"), dict) else pd
    if isinstance(support_pd, dict) and (
        support_pd.get("ticket_id") or support_pd.get("query_text")
    ):
        context["support"] = {
            "ticket_id": str(support_pd.get("ticket_id") or "").strip(),
            "query_text": str(support_pd.get("query_text") or "").strip(),
        }

    for key in ("engagement", "consultation", "booking"):
        block = member.get(key)
        if isinstance(block, dict) and block:
            context[key] = {k: v for k, v in block.items() if v is not None}

    if pd:
        coll_date = str(pd.get("collection_date") or "").strip()
        coll_time = str(pd.get("collection_time") or "").strip()
        if coll_date or coll_time:
            booking = dict(context.get("booking") or {})
            if coll_date:
                booking.setdefault("collection_date", coll_date)
            if coll_time:
                booking.setdefault("collection_time", coll_time)
            context["booking"] = booking
        eng_name = str(pd.get("engagement_name") or "").strip()
        eng_code = str(pd.get("engagement_code") or "").strip()
        eng_label = str(pd.get("engagement") or "").strip()
        if eng_name in ("-", ""):
            eng_name = ""
        if eng_code in ("-", ""):
            eng_code = ""
        if eng_label in ("-", ""):
            eng_label = ""
        if eng_name or eng_code or eng_label:
            engagement = dict(context.get("engagement") or {})
            if eng_name:
                engagement["engagement_name"] = eng_name
            elif eng_label and not engagement.get("engagement_name"):
                engagement["engagement_name"] = eng_label
            if eng_code:
                engagement["engagement_code"] = eng_code
            if eng_label and not engagement.get("engagement_code"):
                engagement["engagement_code"] = eng_label
            context["engagement"] = engagement

    return context
