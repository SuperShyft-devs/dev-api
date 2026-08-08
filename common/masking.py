"""PII masking helpers for API responses."""

from __future__ import annotations


def mask_phone(phone: str | None) -> str | None:
    """Mask a phone number, keeping the last 4 characters visible.

    Example: ``9600004773`` → ``******4773``. Null/empty stays as-is.
    """
    if phone is None:
        return None
    value = str(phone)
    if not value:
        return value
    if len(value) <= 4:
        return value
    return ("*" * (len(value) - 4)) + value[-4:]


def mask_email(email: str | None) -> str | None:
    """Mask the local-part of an email, keeping the last 4 chars and full domain.

    Example: ``pratheek.fitnastic@gmail.com`` → ``*************stic@gmail.com``.
    Null/empty or emails without ``@`` are handled safely.
    """
    if email is None:
        return None
    value = str(email)
    if not value:
        return value

    at = value.rfind("@")
    if at <= 0:
        return mask_phone(value)

    local = value[:at]
    domain = value[at:]  # includes '@'
    if len(local) <= 4:
        return ("*" * len(local)) + domain
    return ("*" * (len(local) - 4)) + local[-4:] + domain
