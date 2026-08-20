"""Shared Pydantic field validators for API input sanitization."""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Annotated, Any, TypeVar

from pydantic import BeforeValidator, EmailStr, Field, StringConstraints

# ── Regex patterns ────────────────────────────────────────────────────────────

_PIN_CODE_RE = re.compile(r"^\d{6}$")
_OTP_RE = re.compile(r"^\d{4,10}$")
_SLUG_KEY_RE = re.compile(r"^[a-z0-9_]+$")
_PHONE_ALLOWED = frozenset("0123456789+ -()")
_SCRIPT_PATTERN = re.compile(r"(?i)<\s*script|javascript\s*:|on\w+\s*=")

_SAFE_DISPLAY_EXTRA = frozenset(".,&-'/()")
_CITY_STATE_EXTRA = frozenset(".-'")

# ── Limits (from plan) ────────────────────────────────────────────────────────

PERSON_NAME_MAX = 100
SAFE_DISPLAY_NAME_MAX = 200
ADDRESS_MAX = 500
ORG_ADDRESS_MAX = 2000
CITY_STATE_COUNTRY_MAX = 100
LANDMARK_MAX = 200
PHONE_MAX = 15
EMAIL_MAX = 254
PIN_CODE_LEN = 6
SAFE_TEXT_DEFAULT_MAX = 1200
SUPPORT_QUERY_MAX = 1000
CHECKLIST_TEXT_MAX = 500
EXPERT_ABOUT_MAX = 1200
STATUS_MAX = 30
OTP_MIN = 4
OTP_MAX = 10

# Nested structure limits
NESTED_MAX_DEPTH = 10
NESTED_MAX_SERIALIZED_BYTES = 100_000


class ValidationError(ValueError):
    """Raised when input fails sanitization rules."""


def _is_letter(ch: str) -> bool:
    return len(ch) == 1 and unicodedata.category(ch).startswith("L")


def _is_letter_or_digit(ch: str) -> bool:
    cat = unicodedata.category(ch)
    return cat.startswith("L") or cat.startswith("N")


def _chars_only_letters_and_spaces(value: str) -> bool:
    for ch in value:
        if ch == " ":
            continue
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("M"):
            continue
        return False
    return True


def _chars_safe_display(value: str) -> bool:
    for ch in value:
        if ch == " " or _is_letter_or_digit(ch) or ch in _SAFE_DISPLAY_EXTRA:
            continue
        return False
    return True


def _chars_city_state_country(value: str) -> bool:
    for ch in value:
        if ch == " " or _is_letter(ch) or ch in _CITY_STATE_EXTRA:
            continue
        return False
    return True


def _strip_collapse(value: str) -> str:
    return " ".join(value.strip().split())


def _reject_control_chars(value: str) -> str:
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise ValidationError("Control characters are not allowed")
    return value


def _reject_html_script(value: str) -> str:
    if "<" in value or ">" in value:
        raise ValidationError("HTML tags are not allowed")
    if _SCRIPT_PATTERN.search(value):
        raise ValidationError("Script-like content is not allowed")
    return value


def _validate_max_length(value: str, max_len: int, label: str = "value") -> str:
    if len(value) > max_len:
        raise ValidationError(f"{label} must be at most {max_len} characters")
    return value


def _make_person_name_validator(max_len: int = PERSON_NAME_MAX):
    def _validate(value: Any) -> str:
        if value is None:
            raise ValidationError("Name is required")
        if not isinstance(value, str):
            raise ValidationError("Name must be a string")
        cleaned = _strip_collapse(value)
        if not cleaned:
            raise ValidationError("Name cannot be empty")
        _reject_control_chars(cleaned)
        _validate_max_length(cleaned, max_len, "Name")
        if not _chars_only_letters_and_spaces(cleaned):
            raise ValidationError("Name may only contain letters and spaces")
        return cleaned

    return _validate


def _make_safe_display_name_validator(max_len: int = SAFE_DISPLAY_NAME_MAX):
    def _validate(value: Any) -> str:
        if value is None:
            raise ValidationError("Value is required")
        if not isinstance(value, str):
            raise ValidationError("Value must be a string")
        cleaned = _strip_collapse(value)
        if not cleaned:
            raise ValidationError("Value cannot be empty")
        _reject_control_chars(cleaned)
        _reject_html_script(cleaned)
        _validate_max_length(cleaned, max_len)
        if not _chars_safe_display(cleaned):
            raise ValidationError("Value contains disallowed characters")
        return cleaned

    return _validate


def _make_safe_text_validator(max_len: int = SAFE_TEXT_DEFAULT_MAX):
    def _validate(value: Any) -> str:
        if value is None:
            raise ValidationError("Value is required")
        if not isinstance(value, str):
            raise ValidationError("Value must be a string")
        cleaned = value.strip()
        if not cleaned:
            raise ValidationError("Value cannot be empty")
        _reject_control_chars(cleaned)
        _reject_html_script(cleaned)
        _validate_max_length(cleaned, max_len)
        return cleaned

    return _validate


def _make_optional_validator(validator_fn):
    def _validate(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return validator_fn(value)

    return _validate


def _make_city_state_country_validator(max_len: int = CITY_STATE_COUNTRY_MAX):
    def _validate(value: Any) -> str:
        if value is None:
            raise ValidationError("Value is required")
        if not isinstance(value, str):
            raise ValidationError("Value must be a string")
        cleaned = _strip_collapse(value)
        if not cleaned:
            raise ValidationError("Value cannot be empty")
        _reject_control_chars(cleaned)
        _reject_html_script(cleaned)
        _validate_max_length(cleaned, max_len)
        if not _chars_city_state_country(cleaned):
            raise ValidationError("Value may only contain letters, spaces, and . - '")
        return cleaned

    return _validate


def _validate_phone(value: Any) -> str:
    if value is None:
        raise ValidationError("Phone is required")
    if not isinstance(value, str):
        raise ValidationError("Phone must be a string")
    cleaned = value.strip()
    if not cleaned:
        raise ValidationError("Phone cannot be empty")
    if len(cleaned) > PHONE_MAX:
        raise ValidationError(f"Phone must be at most {PHONE_MAX} characters")
    if any(ch not in _PHONE_ALLOWED for ch in cleaned):
        raise ValidationError("Phone contains invalid characters")
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if len(digits) < 8:
        raise ValidationError("Phone must contain at least 8 digits")
    return cleaned


def _validate_optional_phone(value: Any) -> str | None:
    if value is None:
        return None
    return _validate_phone(value)


def _validate_pin_code(value: Any) -> str:
    if value is None:
        raise ValidationError("Pin code is required")
    if not isinstance(value, str):
        raise ValidationError("Pin code must be a string")
    cleaned = value.strip()
    if not _PIN_CODE_RE.match(cleaned):
        raise ValidationError(f"Pin code must be exactly {PIN_CODE_LEN} digits")
    return cleaned


def _validate_optional_pin_code(value: Any) -> str | None:
    if value is None:
        return None
    return _validate_pin_code(value)


def _validate_otp(value: Any) -> str:
    if value is None:
        raise ValidationError("OTP is required")
    if not isinstance(value, str):
        raise ValidationError("OTP must be a string")
    cleaned = value.strip()
    if not _OTP_RE.match(cleaned):
        raise ValidationError(f"OTP must be {OTP_MIN}-{OTP_MAX} digits")
    return cleaned


def _validate_slug_key(value: Any) -> str:
    if value is None:
        raise ValidationError("Key is required")
    if not isinstance(value, str):
        raise ValidationError("Key must be a string")
    cleaned = value.strip().lower()
    if not cleaned:
        raise ValidationError("Key cannot be empty")
    if len(cleaned) > 100:
        raise ValidationError("Key must be at most 100 characters")
    if not _SLUG_KEY_RE.match(cleaned):
        raise ValidationError("Key must be lowercase alphanumeric with underscores only")
    return cleaned


def _validate_optional_slug_key(value: Any) -> str | None:
    if value is None:
        return None
    return _validate_slug_key(value)


def reject_unsafe_strings(
    obj: Any,
    *,
    max_len: int = SAFE_TEXT_DEFAULT_MAX,
    depth: int = 0,
) -> Any:
    """Recursively validate string values in nested dicts/lists for unsafe content."""
    if depth > NESTED_MAX_DEPTH:
        raise ValidationError("Nested structure exceeds maximum depth")

    if isinstance(obj, str):
        return _make_safe_text_validator(max_len)(obj)

    if isinstance(obj, dict):
        result = {k: reject_unsafe_strings(v, max_len=max_len, depth=depth + 1) for k, v in obj.items()}
        serialized = json.dumps(result, default=str)
        if len(serialized.encode("utf-8")) > NESTED_MAX_SERIALIZED_BYTES:
            raise ValidationError("Nested structure exceeds maximum size")
        return result

    if isinstance(obj, list):
        result = [reject_unsafe_strings(item, max_len=max_len, depth=depth + 1) for item in obj]
        serialized = json.dumps(result, default=str)
        if len(serialized.encode("utf-8")) > NESTED_MAX_SERIALIZED_BYTES:
            raise ValidationError("Nested structure exceeds maximum size")
        return result

    return obj


def validate_nested_strings(obj: Any, *, max_len: int = SAFE_TEXT_DEFAULT_MAX) -> Any:
    """Public entry point for nested string validation; raises ValidationError on failure."""
    return reject_unsafe_strings(obj, max_len=max_len)


def sanitize_search_query(value: str | None, *, max_len: int = 200) -> str:
    """Sanitize free-text search query parameters."""
    if value is None:
        return ""
    raw = value.strip()
    if not raw:
        return ""
    return _make_safe_text_validator(max_len)(raw)


def optional_search_query(value: str | None, *, max_len: int = 200) -> str | None:
    """Sanitize optional search query parameters; empty string becomes None."""
    if value is None:
        return None
    cleaned = sanitize_search_query(value, max_len=max_len)
    return cleaned or None


# ── Factory for parameterized optional/required types ───────────────────────

T = TypeVar("T")


def _optional_type(base_validator, max_len: int | None = None):
    if max_len is not None:
        fn = base_validator(max_len) if callable(base_validator) and max_len else base_validator
    else:
        fn = base_validator
    return Annotated[str | None, BeforeValidator(_make_optional_validator(fn))]


# ── Annotated types (required) ────────────────────────────────────────────────

PersonName = Annotated[str, BeforeValidator(_make_person_name_validator())]
OptionalPersonName = Annotated[str | None, BeforeValidator(_make_optional_validator(_make_person_name_validator()))]

SafeDisplayName = Annotated[str, BeforeValidator(_make_safe_display_name_validator())]
OptionalSafeDisplayName = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_display_name_validator()))
]

SafeText = Annotated[str, BeforeValidator(_make_safe_text_validator())]
OptionalSafeText = Annotated[str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator()))]

SupportQueryText = Annotated[str, BeforeValidator(_make_safe_text_validator(SUPPORT_QUERY_MAX))]
ChecklistText = Annotated[str, BeforeValidator(_make_safe_text_validator(CHECKLIST_TEXT_MAX))]
ExpertAboutText = Annotated[str, BeforeValidator(_make_safe_text_validator(EXPERT_ABOUT_MAX))]
OptionalExpertAboutText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(EXPERT_ABOUT_MAX)))
]
QuestionText = Annotated[str, BeforeValidator(_make_safe_text_validator(2000))]
OptionalQuestionText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(2000)))
]
OptionalChecklistText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(CHECKLIST_TEXT_MAX)))
]

AddressText = Annotated[str, BeforeValidator(_make_safe_text_validator(ADDRESS_MAX))]
OptionalAddressText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(ADDRESS_MAX)))
]
OrgAddressText = Annotated[str, BeforeValidator(_make_safe_text_validator(ORG_ADDRESS_MAX))]
OptionalOrgAddressText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(ORG_ADDRESS_MAX)))
]

CityStateCountry = Annotated[str, BeforeValidator(_make_city_state_country_validator())]
OptionalCityStateCountry = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_city_state_country_validator()))
]

LandmarkText = Annotated[str, BeforeValidator(_make_safe_text_validator(LANDMARK_MAX))]
OptionalLandmarkText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(LANDMARK_MAX)))
]

PhoneStr = Annotated[str, BeforeValidator(_validate_phone)]
OptionalPhoneStr = Annotated[str | None, BeforeValidator(_validate_optional_phone)]

PinCode = Annotated[str, BeforeValidator(_validate_pin_code)]
OptionalPinCode = Annotated[str | None, BeforeValidator(_validate_optional_pin_code)]

OtpCode = Annotated[str, BeforeValidator(_validate_otp)]

SlugKey = Annotated[str, BeforeValidator(_validate_slug_key)]
OptionalSlugKey = Annotated[str | None, BeforeValidator(_validate_optional_slug_key)]

EngagementCode = Annotated[str, BeforeValidator(_make_safe_display_name_validator(50))]
OptionalEngagementCode = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_display_name_validator(50)))
]

ShortSafeText = Annotated[str, BeforeValidator(_make_safe_text_validator(20))]
OptionalShortSafeText = Annotated[
    str | None, BeforeValidator(_make_optional_validator(_make_safe_text_validator(20)))
]

PositiveIntId = Annotated[int, Field(gt=0)]
NonNegativeInt = Annotated[int, Field(ge=0)]

StatusStr = Annotated[str, StringConstraints(min_length=1, max_length=STATUS_MAX, strip_whitespace=True)]

# Email uses pydantic EmailStr with max length constraint via Field on schema fields
EmailField = EmailStr
