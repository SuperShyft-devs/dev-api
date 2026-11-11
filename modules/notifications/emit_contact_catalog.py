"""Emit-context contact fields for payload recipient bindings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

ContactKind = Literal["phone", "email", "user_id"]
ContactCardinality = Literal["single", "list"]
FieldGroup = Literal["primary", "booker", "members", "inline", "payment"]


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


def _std_user_fields(*, event_specific: tuple[PayloadContactField, ...] = ()) -> tuple[PayloadContactField, ...]:
    base = (
        PayloadContactField(
            id="user_id",
            label="Primary user ID",
            path="user_id",
            kind="user_id",
            cardinality="single",
            group="primary",
            description="user_id on emit context",
            example="12345",
        ),
        PayloadContactField(
            id="participant_user_id",
            label="Participant user ID",
            path="participant_user_id",
            kind="user_id",
            cardinality="single",
            group="primary",
            description="participant_user_id when distinct from user_id",
        ),
        PayloadContactField(
            id="user_ids",
            label="User ID list",
            path="user_ids",
            kind="user_id",
            cardinality="list",
            group="primary",
            description="Batch user IDs",
        ),
        PayloadContactField(
            id="phone",
            label="Phone (inline)",
            path="phone",
            kind="phone",
            cardinality="single",
            group="inline",
            description="Top-level phone on emit context (e.g. OTP)",
        ),
        PayloadContactField(
            id="email",
            label="Email (inline)",
            path="email",
            kind="email",
            cardinality="single",
            group="inline",
            description="Top-level email on emit context",
        ),
    )
    return base + event_specific


_PAYMENT_EXTRA = (
    PayloadContactField(
        id="payer_user_id",
        label="Payer user ID",
        path="payer_user_id",
        kind="user_id",
        cardinality="single",
        group="payment",
        description="User who paid (booker on family orders)",
    ),
    PayloadContactField(
        id="booker.user_id",
        label="Booker user ID",
        path="booker.user_id",
        kind="user_id",
        cardinality="single",
        group="booker",
    ),
    PayloadContactField(
        id="booker.phone",
        label="Booker phone",
        path="booker.phone",
        kind="phone",
        cardinality="single",
        group="booker",
    ),
    PayloadContactField(
        id="booker.email",
        label="Booker email",
        path="booker.email",
        kind="email",
        cardinality="single",
        group="booker",
    ),
    PayloadContactField(
        id="members.user_ids",
        label="Member user IDs",
        path="members",
        kind="user_id",
        cardinality="list",
        group="members",
        description="user_id from each members[] object",
    ),
    PayloadContactField(
        id="members.phones",
        label="Member phones",
        path="members",
        kind="phone",
        cardinality="list",
        group="members",
        description="phone from each members[] object",
    ),
    PayloadContactField(
        id="members.emails",
        label="Member emails",
        path="members",
        kind="email",
        cardinality="list",
        group="members",
        description="email from each members[] object",
    ),
)

_EVENT_CONTACT_FIELDS: dict[str, tuple[PayloadContactField, ...]] = {
    "auth.otp_sent": (
        PayloadContactField(
            id="user_id",
            label="User ID",
            path="user_id",
            kind="user_id",
            cardinality="single",
            group="primary",
        ),
        PayloadContactField(
            id="phone",
            label="Phone",
            path="phone",
            kind="phone",
            cardinality="single",
            group="inline",
        ),
        PayloadContactField(
            id="email",
            label="Email",
            path="email",
            kind="email",
            cardinality="single",
            group="inline",
        ),
    ),
    "users.registered": _std_user_fields(),
    "users.onboarded": _std_user_fields(),
    "payments.completed": _std_user_fields(event_specific=_PAYMENT_EXTRA),
    "payments.failed": _std_user_fields(event_specific=_PAYMENT_EXTRA),
    "support.ticket_created": _std_user_fields(),
    "support.ticket_resolved": _std_user_fields(),
    "consultations.booked": _std_user_fields(),
    "consultations.confirmed": _std_user_fields(),
    "consultations.rescheduled": _std_user_fields(),
    "consultations.cancelled": _std_user_fields(),
    "consultations.completed": _std_user_fields(),
    "bookings.rescheduled": _std_user_fields(event_specific=_PAYMENT_EXTRA),
    "bookings.cancelled": _std_user_fields(),
}

_DEFAULT_API_FIELDS = _std_user_fields()


def payload_contact_fields_for_event(event_code: str) -> list[dict[str, str]]:
    fields = _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS)
    return [
        {
            "id": f.id,
            "label": f.label,
            "path": f.path,
            "kind": f.kind,
            "cardinality": f.cardinality,
            "group": f.group,
            "description": f.description,
            "example": f.example,
        }
        for f in fields
    ]


def get_payload_contact_field(event_code: str, field_id: str) -> PayloadContactField | None:
    for f in _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS):
        if f.id == field_id:
            return f
    return None


def allowed_payload_field_ids(event_code: str) -> frozenset[str]:
    return frozenset(f.id for f in _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS))


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
    """Resolve dot paths; ``members`` returns the list at ``members``."""
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
    """Return raw values for one field (may be empty)."""
    if field.path == "members" and field.cardinality == "list":
        raw = context.get("members")
        if not isinstance(raw, list):
            return []
        return list(_extract_from_members_list(raw, kind=field.kind))

    raw = _get_path_value(context, field.path)
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
    """
    Resolve selected payload fields to deduped user_ids and contact dicts.
    Contacts may include phone and/or email on the same dict (merged per key).
    """
    user_ids: list[int] = []
    seen_uids: set[int] = set()
    phones: list[str] = []
    emails: list[str] = []
    seen_phones: set[str] = set()
    seen_emails: set[str] = set()

    fields_map = {f.id: f for f in _EVENT_CONTACT_FIELDS.get(event_code, _DEFAULT_API_FIELDS)}

    for fid in field_ids:
        field = fields_map.get(fid)
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
