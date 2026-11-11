"""Emit-context contact fields for payload recipient bindings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ContactKind = Literal["phone", "email", "user_id"]
ContactCardinality = Literal["single", "list"]
FieldGroup = Literal[
    "payer",
    "participants",
    "contact",
    "primary",
    "booker",
    "members",
    "inline",
    "payment",
]
FieldLayout = Literal["default", "payment_roles", "otp_simple"]


@dataclass(frozen=True, slots=True)
class PayloadContactField:
    id: str
    label: str
    path: str
    kind: ContactKind
    cardinality: ContactCardinality
    group: FieldGroup
    description: str = ""
    example: str = ""
    audience: str = ""
    channels: tuple[str, ...] = ()
    sort_order: int = 0


def _channels_for_kind(kind: ContactKind) -> tuple[str, ...]:
    if kind == "phone":
        return ("whatsapp",)
    if kind == "email":
        return ("email",)
    return ("whatsapp", "email")


_PAYMENTS_COMPLETED_FIELDS: tuple[PayloadContactField, ...] = (
    PayloadContactField(
        id="booker.phone",
        label="Phone — person who paid",
        path="booker.phone",
        kind="phone",
        cardinality="single",
        group="payer",
        audience="payer",
        channels=("whatsapp",),
        sort_order=10,
        description="Booker or payer mobile (family gift checkout uses the booker).",
    ),
    PayloadContactField(
        id="booker.email",
        label="Email — person who paid",
        path="booker.email",
        kind="email",
        cardinality="single",
        group="payer",
        audience="payer",
        channels=("email",),
        sort_order=20,
        description="Booker or payer email address.",
    ),
    PayloadContactField(
        id="payer_user_id",
        label="User account — person who paid",
        path="payer_user_id",
        kind="user_id",
        cardinality="single",
        group="payer",
        audience="payer",
        channels=("whatsapp", "email"),
        sort_order=30,
        description="Looks up phone/email from the payer’s user profile.",
    ),
    PayloadContactField(
        id="members.phones",
        label="All participants — phone numbers",
        path="members",
        kind="phone",
        cardinality="list",
        group="participants",
        audience="members",
        channels=("whatsapp",),
        sort_order=40,
        description="One message per distinct mobile on paid order lines (deduped).",
    ),
    PayloadContactField(
        id="members.emails",
        label="All participants — email addresses",
        path="members",
        kind="email",
        cardinality="list",
        group="participants",
        audience="members",
        channels=("email",),
        sort_order=50,
        description="One message per distinct email on paid order lines (deduped).",
    ),
    PayloadContactField(
        id="members.user_ids",
        label="All participants — user accounts",
        path="members",
        kind="user_id",
        cardinality="list",
        group="participants",
        audience="members",
        channels=("whatsapp", "email"),
        sort_order=60,
        description="Uses each member’s saved phone/email from their profile.",
    ),
)

# Legacy ids still accepted on saved bindings (hidden from new picker).
_PAYMENTS_COMPLETED_LEGACY_IDS: frozenset[str] = frozenset(
    {
        "user_id",
        "participant_user_id",
        "user_ids",
        "phone",
        "email",
        "booker.user_id",
    }
)

_OTP_SIMPLE_FIELDS: tuple[PayloadContactField, ...] = (
    PayloadContactField(
        id="phone",
        label="Phone number from request",
        path="phone",
        kind="phone",
        cardinality="single",
        group="contact",
        audience="recipient",
        channels=("whatsapp",),
        sort_order=1,
        description="Mobile entered or resolved for this OTP.",
    ),
    PayloadContactField(
        id="email",
        label="Email from request",
        path="email",
        kind="email",
        cardinality="single",
        group="contact",
        audience="recipient",
        channels=("email",),
        sort_order=2,
    ),
    PayloadContactField(
        id="user_id",
        label="User account (if logged in)",
        path="user_id",
        kind="user_id",
        cardinality="single",
        group="contact",
        audience="recipient",
        channels=("whatsapp", "email"),
        sort_order=3,
    ),
)

_STD_ONBOARD_FIELDS: tuple[PayloadContactField, ...] = (
    PayloadContactField(
        id="user_id",
        label="Primary user account",
        path="user_id",
        kind="user_id",
        cardinality="single",
        group="primary",
        audience="participant",
        channels=_channels_for_kind("user_id"),
        sort_order=1,
    ),
    PayloadContactField(
        id="participant_user_id",
        label="Participant user account",
        path="participant_user_id",
        kind="user_id",
        cardinality="single",
        group="primary",
        audience="participant",
        channels=_channels_for_kind("user_id"),
        sort_order=2,
        description="When different from primary user_id.",
    ),
    PayloadContactField(
        id="user_ids",
        label="Batch user accounts",
        path="user_ids",
        kind="user_id",
        cardinality="list",
        group="primary",
        audience="participants",
        channels=_channels_for_kind("user_id"),
        sort_order=3,
    ),
)

_EVENT_CONTACT_FIELDS: dict[str, tuple[PayloadContactField, ...]] = {
    "auth.otp_sent": _OTP_SIMPLE_FIELDS,
    "users.registered": _STD_ONBOARD_FIELDS,
    "users.onboarded": _STD_ONBOARD_FIELDS,
    "payments.completed": _PAYMENTS_COMPLETED_FIELDS,
    "payments.failed": _PAYMENTS_COMPLETED_FIELDS,
    "support.ticket_created": _STD_ONBOARD_FIELDS,
    "support.ticket_resolved": _STD_ONBOARD_FIELDS,
    "consultations.booked": _STD_ONBOARD_FIELDS,
    "consultations.confirmed": _STD_ONBOARD_FIELDS,
    "consultations.rescheduled": _STD_ONBOARD_FIELDS,
    "consultations.cancelled": _STD_ONBOARD_FIELDS,
    "consultations.completed": _STD_ONBOARD_FIELDS,
    "bookings.rescheduled": _STD_ONBOARD_FIELDS,
    "bookings.cancelled": _STD_ONBOARD_FIELDS,
}

_EVENT_PAYLOAD_LAYOUT: dict[str, FieldLayout] = {
    "payments.completed": "payment_roles",
    "payments.failed": "payment_roles",
    "auth.otp_sent": "otp_simple",
}

_LEGACY_FIELD_DEFS: dict[str, PayloadContactField] = {
    "user_id": PayloadContactField(
        id="user_id",
        label="Primary user ID (legacy)",
        path="user_id",
        kind="user_id",
        cardinality="single",
        group="primary",
    ),
    "participant_user_id": PayloadContactField(
        id="participant_user_id",
        label="Participant user ID (legacy)",
        path="participant_user_id",
        kind="user_id",
        cardinality="single",
        group="primary",
    ),
    "user_ids": PayloadContactField(
        id="user_ids",
        label="User ID list (legacy)",
        path="user_ids",
        kind="user_id",
        cardinality="list",
        group="primary",
    ),
    "phone": PayloadContactField(
        id="phone",
        label="Phone (legacy)",
        path="phone",
        kind="phone",
        cardinality="single",
        group="inline",
    ),
    "email": PayloadContactField(
        id="email",
        label="Email (legacy)",
        path="email",
        kind="email",
        cardinality="single",
        group="inline",
    ),
    "booker.user_id": PayloadContactField(
        id="booker.user_id",
        label="Booker user ID (legacy)",
        path="booker.user_id",
        kind="user_id",
        cardinality="single",
        group="booker",
    ),
}

_DEFAULT_API_FIELDS = _STD_ONBOARD_FIELDS


def payload_layout_for_event(event_code: str) -> str:
    return _EVENT_PAYLOAD_LAYOUT.get(event_code, "default")


def _field_to_dict(f: PayloadContactField) -> dict[str, Any]:
    channels = f.channels or _channels_for_kind(f.kind)
    return {
        "id": f.id,
        "label": f.label,
        "path": f.path,
        "kind": f.kind,
        "cardinality": f.cardinality,
        "group": f.group,
        "description": f.description,
        "example": f.example,
        "audience": f.audience or f.group,
        "channels": list(channels),
        "sort_order": f.sort_order,
    }


def payload_contact_fields_for_event(event_code: str) -> list[dict[str, Any]]:
    fields = _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS)
    ordered = sorted(fields, key=lambda f: (f.sort_order, f.label))
    return [_field_to_dict(f) for f in ordered]


def get_payload_contact_field(event_code: str, field_id: str) -> PayloadContactField | None:
    for f in _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS):
        if f.id == field_id:
            return f
    legacy = _LEGACY_FIELD_DEFS.get(field_id)
    if legacy and event_code in ("payments.completed", "payments.failed"):
        return legacy
    return None


def allowed_payload_field_ids(event_code: str) -> frozenset[str]:
    base = frozenset(f.id for f in _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS))
    if event_code in ("payments.completed", "payments.failed"):
        return base | _PAYMENTS_COMPLETED_LEGACY_IDS
    return base


def _normalize_phone(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_email(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _coerce_user_id(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _get_path_value(context: dict[str, Any], path: str) -> Any:
    parts = path.split(".")
    cur: Any = context
    for part in parts:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _extract_from_members_list(
    items: list,
    *,
    kind: ContactKind,
) -> list[str] | list[int]:
    out: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if kind == "user_id":
            uid = _coerce_user_id(item.get("user_id"))
            if uid is not None:
                out.append(uid)
        elif kind == "phone":
            phone = _normalize_phone(item.get("phone"))
            if phone:
                out.append(phone)
        elif kind == "email":
            email = _normalize_email(item.get("email"))
            if email:
                out.append(email)
    return out


def extract_values_for_field(context: dict[str, Any], field: PayloadContactField) -> list[Any]:
    if field.path == "members" and field.cardinality == "list":
        raw = context.get("members")
        if not isinstance(raw, list):
            return []
        return list(_extract_from_members_list(raw, kind=field.kind))

    raw = _get_path_value(context, field.path)
    if field.id == "user_id" and raw is None:
        raw = context.get("payer_user_id")
    if field.cardinality == "list":
        if not isinstance(raw, list):
            return []
        return raw
    if raw is None:
        return []
    return [raw]


def resolve_payload_targets(
    context: dict[str, Any],
    field_ids: list[str],
    event_code: str,
) -> tuple[list[int], list[dict[str, str]]]:
    user_ids: list[int] = []
    seen_uids: set[int] = set()
    phones: list[str] = []
    emails: list[str] = []
    seen_phones: set[str] = set()
    seen_emails: set[str] = set()

    catalog_fields = {f.id: f for f in _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS)}

    for fid in field_ids:
        field = catalog_fields.get(fid) or _LEGACY_FIELD_DEFS.get(fid)
        if field is None:
            continue
        values = extract_values_for_field(context, field)
        if field.kind == "user_id":
            for v in values:
                uid = _coerce_user_id(v)
                if uid is not None and uid not in seen_uids:
                    seen_uids.add(uid)
                    user_ids.append(uid)
        elif field.kind == "phone":
            for v in values:
                phone = _normalize_phone(v)
                if phone and phone not in seen_phones:
                    seen_phones.add(phone)
                    phones.append(phone)
        elif field.kind == "email":
            for v in values:
                email = _normalize_email(v)
                if email and email not in seen_emails:
                    seen_emails.add(email)
                    emails.append(email)

    contacts: list[dict[str, str]] = [{"phone": p} for p in phones] + [{"email": e} for e in emails]
    return user_ids, contacts
