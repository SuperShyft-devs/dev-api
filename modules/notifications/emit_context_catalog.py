"""Participant identifier fields for user recipient bindings (trigger API or emit context)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParticipantField:
    key: str
    label: str
    description: str
    source: str = "emit"  # emit | trigger
    trigger_api: str = ""


_EMIT_FIELD_META: dict[str, ParticipantField] = {
    "user_id": ParticipantField(
        "user_id",
        "user_id",
        "User ID on the notification emit payload",
        "emit",
    ),
    "user_ids": ParticipantField(
        "user_ids",
        "user_ids",
        "List of user IDs on the emit payload",
        "emit",
    ),
    "participant_user_id": ParticipantField(
        "participant_user_id",
        "participant_user_id",
        "Participant user ID when distinct from user_id",
        "emit",
    ),
    "booker_user_id": ParticipantField(
        "booker_user_id",
        "booker_user_id",
        "Family booker user ID (gift / family flows)",
        "emit",
    ),
}

_TRIGGER_FIELD_META: dict[str, ParticipantField] = {
    "phone": ParticipantField(
        "phone",
        "phone",
        "Phone from the request JSON body",
        "trigger",
        "POST /auth/send-otp",
    ),
    "email": ParticipantField(
        "email",
        "email",
        "Email from the request JSON body",
        "trigger",
        "POST /auth/send-otp",
    ),
}

_DEFAULT_EMIT_KEYS: tuple[str, ...] = ("user_id", "user_ids", "participant_user_id")

_EVENT_PARTICIPANT_FIELDS: dict[str, tuple[str, ...]] = {
    "auth.otp_sent": ("phone", "email"),
    "users.registered": ("user_id",),
    "users.onboarded": ("user_id", "participant_user_id", "user_ids"),
    "family.gift_sent": ("booker_user_id", "user_id", "participant_user_id"),
    "family.gift_received": ("user_id", "participant_user_id"),
    "support.ticket_created": ("user_id", "participant_user_id"),
    "support.ticket_resolved": ("user_id",),
}


def _meta_for_key(key: str) -> ParticipantField | None:
    return _TRIGGER_FIELD_META.get(key) or _EMIT_FIELD_META.get(key)


def user_context_fields_for_event(event_code: str) -> list[dict[str, str]]:
    keys = _EVENT_PARTICIPANT_FIELDS.get(event_code, _DEFAULT_EMIT_KEYS)
    out: list[dict[str, str]] = []
    for key in keys:
        meta = _meta_for_key(key)
        if meta is None:
            out.append({"key": key, "label": key, "description": "", "source": "emit"})
        else:
            out.append(
                {
                    "key": meta.key,
                    "label": meta.label,
                    "description": meta.description,
                    "source": meta.source,
                    **({"trigger_api": meta.trigger_api} if meta.trigger_api else {}),
                }
            )
    return out


def default_user_context_key(event_code: str) -> str:
    fields = user_context_fields_for_event(event_code)
    if event_code == "auth.otp_sent":
        return "phone"
    return fields[0]["key"] if fields else "user_id"


ALLOWED_USER_CONTEXT_KEYS = frozenset(_EMIT_FIELD_META.keys()) | frozenset(
    _TRIGGER_FIELD_META.keys()
)
