"""Build notification `user.*` context from a users row."""

from __future__ import annotations

from typing import Any


def user_notification_context(user: Any) -> dict[str, str]:
    first = str(getattr(user, "first_name", None) or "").strip()
    last = str(getattr(user, "last_name", None) or "").strip()
    name = f"{first} {last}".strip()
    gender = getattr(user, "gender", None)
    if gender is not None and hasattr(gender, "value"):
        gender = gender.value
    age = getattr(user, "age", None)
    return {
        "first_name": first,
        "last_name": last,
        "name": name,
        "phone": str(getattr(user, "phone", None) or "").strip(),
        "email": str(getattr(user, "email", None) or "").strip(),
        "age": "" if age is None else str(age),
        "gender": str(gender or "").strip(),
        "address": str(getattr(user, "address", None) or "").strip(),
        "pincode": str(getattr(user, "pin_code", None) or "").strip(),
    }
