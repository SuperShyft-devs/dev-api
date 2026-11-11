"""Per-event emit-context fields selectable for payload recipient bindings."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from modules.notifications.events_catalog import TriggerKind, get_event

ContactValueType = Literal["phone", "email"]
ContactCardinality = Literal["scalar", "array"]


@dataclass(frozen=True, slots=True)
class PayloadContactField:
    path: str
    label: str
    value_type: ContactValueType
    cardinality: ContactCardinality
    description: str
    group: str = "Context"

    def to_dict(self) -> dict[str, str]:
        return {
            "path": self.path,
            "label": self.label,
            "value_type": self.value_type,
            "cardinality": self.cardinality,
            "description": self.description,
            "group": self.group,
        }


def allows_payload_recipient(event_code: str) -> bool:
    ev = get_event(event_code)
    if ev is None:
        return False
    return ev.trigger_kind == "http"


def _fields(*items: PayloadContactField) -> tuple[PayloadContactField, ...]:
    return items


_COMMON_ROOT = _fields(
    PayloadContactField("phone", "Context phone", "phone", "scalar", "Top-level phone on emit context", "Context"),
    PayloadContactField("email", "Context email", "email", "scalar", "Top-level email on emit context", "Context"),
)

_PARTICIPANT_DETAILS = _fields(
    PayloadContactField(
        "participant_details.phone",
        "Participant phone",
        "phone",
        "scalar",
        "Phone inside participant_details",
        "Participant",
    ),
    PayloadContactField(
        "participant_details.email",
        "Participant email",
        "email",
        "scalar",
        "Email inside participant_details",
        "Participant",
    ),
)

_PAYMENTS_COMPLETED = _fields(
    PayloadContactField(
        "booker.phone",
        "Booker phone",
        "phone",
        "scalar",
        "Phone of the payer / booker on this order",
        "Booker",
    ),
    PayloadContactField(
        "booker.email",
        "Booker email",
        "email",
        "scalar",
        "Email of the payer / booker",
        "Booker",
    ),
    PayloadContactField(
        "members[].phone",
        "Member phones",
        "phone",
        "array",
        "One message per member with a phone in members[]",
        "Members",
    ),
    PayloadContactField(
        "members[].email",
        "Member emails",
        "email",
        "array",
        "One message per member with an email in members[]",
        "Members",
    ),
)

_AUTH_OTP = _fields(
    PayloadContactField(
        "participant_details.staff.phone",
        "Staff phone (OTP)",
        "phone",
        "scalar",
        "Partner/employee OTP phone under participant_details.staff",
        "Staff",
    ),
)

_SUPPORT = _fields(
    PayloadContactField(
        "participant_details.user.phone",
        "Ticket user phone",
        "phone",
        "scalar",
        "Phone on participant_details.user",
        "Participant",
    ),
    PayloadContactField(
        "participant_details.user.email",
        "Ticket user email",
        "email",
        "scalar",
        "Email on participant_details.user",
        "Participant",
    ),
)

_EVENT_PAYLOAD_FIELDS: dict[str, tuple[PayloadContactField, ...]] = {
    "payments.completed": _PAYMENTS_COMPLETED,
    "payments.failed": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "auth.otp_sent": _COMMON_ROOT + _AUTH_OTP,
    "users.registered": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "users.onboarded": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "consultations.booked": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "consultations.confirmed": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "consultations.rescheduled": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "consultations.cancelled": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "consultations.completed": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "support.ticket_created": _SUPPORT,
    "support.ticket_resolved": _SUPPORT,
    "bookings.rescheduled": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "bookings.cancelled": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "system.auto_booking_failed": _COMMON_ROOT + _PARTICIPANT_DETAILS,
    "system.error": _COMMON_ROOT + _PARTICIPANT_DETAILS,
}

_DEFAULT_HTTP_FIELDS = _COMMON_ROOT + _PARTICIPANT_DETAILS


def payload_contact_fields_for_event(event_code: str) -> list[dict[str, str]]:
    if not allows_payload_recipient(event_code):
        return []
    fields = _EVENT_PAYLOAD_FIELDS.get(event_code, _DEFAULT_HTTP_FIELDS)
    return [f.to_dict() for f in fields]


def get_payload_contact_field(event_code: str, path: str) -> PayloadContactField | None:
    if not path:
        return None
    for item in _EVENT_PAYLOAD_FIELDS.get(event_code, _DEFAULT_HTTP_FIELDS):
        if item.path == path:
            return item
    return None
