"""Emit-context fields for participant (user) recipient bindings."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class UserContextField:
    key: str
    label: str
    description: str


_USER_FIELD_META: dict[str, UserContextField] = {
    "user_id": UserContextField(
        key="user_id",
        label="user_id",
        description="Primary user ID set when the event is emitted",
    ),
    "user_ids": UserContextField(
        key="user_ids",
        label="user_ids",
        description="Array of user IDs for batch participant delivery",
    ),
    "participant_user_id": UserContextField(
        key="participant_user_id",
        label="participant_user_id",
        description="Participant user ID when distinct from user_id",
    ),
    "booker_user_id": UserContextField(
        key="booker_user_id",
        label="booker_user_id",
        description="Family booker user ID (gift / family flows)",
    ),
}

_DEFAULT_USER_KEYS: tuple[str, ...] = ("user_id", "user_ids", "participant_user_id")

_EVENT_USER_CONTEXT_KEYS: dict[str, tuple[str, ...]] = {
    "auth.otp_sent": ("user_id",),
    "users.registered": ("user_id",),
    "users.onboarded": ("user_id", "participant_user_id", "user_ids"),
    "family.gift_sent": ("booker_user_id", "user_id", "participant_user_id"),
    "family.gift_received": ("user_id", "participant_user_id"),
    "support.ticket_created": ("user_id", "participant_user_id"),
    "support.ticket_resolved": ("user_id",),
}


def user_context_fields_for_event(event_code: str) -> list[dict[str, str]]:
    keys = _EVENT_USER_CONTEXT_KEYS.get(event_code, _DEFAULT_USER_KEYS)
    out: list[dict[str, str]] = []
    for key in keys:
        meta = _USER_FIELD_META.get(key)
        if meta is None:
            out.append({"key": key, "label": key, "description": ""})
        else:
            out.append(
                {
                    "key": meta.key,
                    "label": meta.label,
                    "description": meta.description,
                }
            )
    return out


def default_user_context_key(event_code: str) -> str:
    fields = user_context_fields_for_event(event_code)
    return fields[0]["key"] if fields else "user_id"


ALLOWED_USER_CONTEXT_KEYS = frozenset(_USER_FIELD_META.keys())
