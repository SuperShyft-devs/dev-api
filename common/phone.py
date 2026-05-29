"""Shared phone normalization for matching stored user.phone values."""

from __future__ import annotations

from core.exceptions import AppError

_ALLOWED_SYMBOLS = frozenset("0123456789+ -()")
_MIN_DIGITS = 8


def phone_lookup_candidates(phone: str, *, strict: bool = False) -> list[str]:
    """Build ordered unique phone strings to match stored user.phone values.

    Supports Indian local (+91) forms and international E.164-style numbers
    (e.g. +66961275268).

    When ``strict`` is True, invalid input raises ``AppError`` (400).
    Otherwise invalid input returns an empty list.
    """

    raw = (phone or "").strip()
    if not raw:
        if strict:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return []

    if any(ch not in _ALLOWED_SYMBOLS for ch in raw):
        if strict:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return []

    stripped = raw.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    digits = "".join(ch for ch in raw if ch.isdigit())

    if len(digits) < _MIN_DIGITS:
        if strict:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return []

    ordered: list[str] = []

    def _add(*values: str) -> None:
        for value in values:
            if value and value not in ordered:
                ordered.append(value)

    _add(raw, stripped)

    if len(digits) == 10:
        _add(digits, f"+91{digits}", f"91{digits}")
    elif len(digits) == 12 and digits.startswith("91"):
        base10 = digits[2:]
        _add(base10, f"+91{base10}", f"91{base10}", f"+{digits}")
    elif len(digits) == 11 and digits.startswith("0"):
        base10 = digits[1:]
        _add(base10, f"+91{base10}", f"91{base10}")
    else:
        _add(f"+{digits}", digits)

    return ordered
