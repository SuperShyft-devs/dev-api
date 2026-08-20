"""Best-effort DB value sanitization using the same rules as common.validation."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from pydantic import EmailStr, ValidationError as PydanticValidationError

from common.slug import sanitize_cabin_key, sanitize_service_key
from common.validation import (
    ADDRESS_MAX,
    CHECKLIST_TEXT_MAX,
    CITY_STATE_COUNTRY_MAX,
    EXPERT_ABOUT_MAX,
    LANDMARK_MAX,
    ORG_ADDRESS_MAX,
    PERSON_NAME_MAX,
    SAFE_DISPLAY_NAME_MAX,
    SAFE_TEXT_DEFAULT_MAX,
    STATUS_MAX,
    SUPPORT_QUERY_MAX,
    ValidationError,
    _make_city_state_country_validator,
    _make_person_name_validator,
    _make_safe_display_name_validator,
    _make_safe_text_validator,
    _validate_phone,
    _validate_pin_code,
    _validate_service_key,
    _validate_slug_key,
    validate_nested_strings,
)

_SCRIPT_PATTERN = re.compile(r"(?i)<\s*script|javascript\s*:|on\w+\s*=")
_SAFE_DISPLAY_EXTRA = frozenset(".,&-'/()")
_CITY_STATE_EXTRA = frozenset(".-'")
_PHONE_ALLOWED = frozenset("0123456789+ -()")


def _strip_collapse(value: str) -> str:
    return " ".join(value.strip().split())


class SanitizeKind(str, Enum):
    PERSON_NAME = "person_name"
    SAFE_DISPLAY_NAME = "safe_display_name"
    SAFE_TEXT = "safe_text"
    ADDRESS_TEXT = "address_text"
    ORG_ADDRESS_TEXT = "org_address_text"
    LANDMARK_TEXT = "landmark_text"
    CITY_STATE_COUNTRY = "city_state_country"
    PHONE = "phone"
    PIN_CODE = "pin_code"
    SLUG_KEY = "slug_key"
    SERVICE_KEY = "service_key"
    ENGAGEMENT_CODE = "engagement_code"
    SHORT_SAFE_TEXT = "short_safe_text"
    STATUS_STR = "status_str"
    QUESTION_TEXT = "question_text"
    CHECKLIST_TEXT = "checklist_text"
    SUPPORT_QUERY_TEXT = "support_query_text"
    EXPERT_ABOUT_TEXT = "expert_about_text"
    EMAIL = "email"
    NESTED_JSON = "nested_json"


class SanitizeStatus(str, Enum):
    OK = "ok"
    NULL = "null"
    SKIP = "skip"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class SanitizeOutcome:
    status: SanitizeStatus
    value: Any
    reason: str | None = None

    @property
    def changed(self) -> bool:
        return self.status in {SanitizeStatus.OK, SanitizeStatus.NULL}


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _try_validate(validator: Callable[[Any], Any], value: Any) -> tuple[bool, Any, str | None]:
    try:
        return True, validator(value), None
    except (ValidationError, ValueError, PydanticValidationError) as exc:
        return False, value, str(exc)


def _strip_control_chars(value: str) -> str:
    return "".join(ch for ch in value if ord(ch) >= 32 and ord(ch) != 127)


def _strip_html(value: str) -> str:
    cleaned = value
    cleaned = re.sub(r"<[^>]*>", "", cleaned)
    cleaned = _SCRIPT_PATTERN.sub("", cleaned)
    return cleaned


def _letters_and_spaces_only(value: str) -> str:
    parts: list[str] = []
    for ch in value:
        if ch == " ":
            parts.append(ch)
            continue
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("M"):
            parts.append(ch)
    return _strip_collapse("".join(parts))


def _safe_display_chars_only(value: str) -> str:
    kept: list[str] = []
    for ch in value:
        cat = unicodedata.category(ch)
        if ch == " " or cat.startswith("L") or cat.startswith("N") or ch in _SAFE_DISPLAY_EXTRA:
            kept.append(ch)
    return _strip_collapse("".join(kept))


def _city_state_chars_only(value: str) -> str:
    kept: list[str] = []
    for ch in value:
        if ch == " " or (len(ch) == 1 and unicodedata.category(ch).startswith("L")) or ch in _CITY_STATE_EXTRA:
            kept.append(ch)
    return _strip_collapse("".join(kept))


def _repair_phone(value: str) -> str:
    cleaned = value.strip()
    cleaned = "".join(ch for ch in cleaned if ch in _PHONE_ALLOWED)
    return cleaned.strip()


def _repair_pin_code(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) >= 6:
        return digits[:6]
    return digits


def _truncate(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[:max_len].rstrip()


def _repair_safe_text(value: str, max_len: int) -> str:
    cleaned = _strip_control_chars(value.strip())
    cleaned = _strip_html(cleaned)
    return _truncate(cleaned, max_len)


def _kind_max_len(kind: SanitizeKind) -> int:
    mapping = {
        SanitizeKind.PERSON_NAME: PERSON_NAME_MAX,
        SanitizeKind.SAFE_DISPLAY_NAME: SAFE_DISPLAY_NAME_MAX,
        SanitizeKind.ADDRESS_TEXT: ADDRESS_MAX,
        SanitizeKind.ORG_ADDRESS_TEXT: ORG_ADDRESS_MAX,
        SanitizeKind.LANDMARK_TEXT: LANDMARK_MAX,
        SanitizeKind.CITY_STATE_COUNTRY: CITY_STATE_COUNTRY_MAX,
        SanitizeKind.ENGAGEMENT_CODE: 50,
        SanitizeKind.SHORT_SAFE_TEXT: 20,
        SanitizeKind.STATUS_STR: STATUS_MAX,
        SanitizeKind.QUESTION_TEXT: 2000,
        SanitizeKind.CHECKLIST_TEXT: CHECKLIST_TEXT_MAX,
        SanitizeKind.SUPPORT_QUERY_TEXT: SUPPORT_QUERY_MAX,
        SanitizeKind.EXPERT_ABOUT_TEXT: EXPERT_ABOUT_MAX,
        SanitizeKind.SAFE_TEXT: SAFE_TEXT_DEFAULT_MAX,
    }
    return mapping.get(kind, SAFE_TEXT_DEFAULT_MAX)


def _validator_for_kind(kind: SanitizeKind) -> Callable[[Any], Any]:
    if kind == SanitizeKind.PERSON_NAME:
        return _make_person_name_validator()
    if kind == SanitizeKind.SAFE_DISPLAY_NAME:
        return _make_safe_display_name_validator()
    if kind == SanitizeKind.ADDRESS_TEXT:
        return _make_safe_text_validator(ADDRESS_MAX)
    if kind == SanitizeKind.ORG_ADDRESS_TEXT:
        return _make_safe_text_validator(ORG_ADDRESS_MAX)
    if kind == SanitizeKind.LANDMARK_TEXT:
        return _make_safe_text_validator(LANDMARK_MAX)
    if kind == SanitizeKind.CITY_STATE_COUNTRY:
        return _make_city_state_country_validator()
    if kind == SanitizeKind.ENGAGEMENT_CODE:
        return _make_safe_display_name_validator(50)
    if kind == SanitizeKind.SHORT_SAFE_TEXT:
        return _make_safe_text_validator(20)
    if kind == SanitizeKind.QUESTION_TEXT:
        return _make_safe_text_validator(2000)
    if kind == SanitizeKind.CHECKLIST_TEXT:
        return _make_safe_text_validator(CHECKLIST_TEXT_MAX)
    if kind == SanitizeKind.SUPPORT_QUERY_TEXT:
        return _make_safe_text_validator(SUPPORT_QUERY_MAX)
    if kind == SanitizeKind.EXPERT_ABOUT_TEXT:
        return _make_safe_text_validator(EXPERT_ABOUT_MAX)
    if kind == SanitizeKind.SAFE_TEXT:
        return _make_safe_text_validator(SAFE_TEXT_DEFAULT_MAX)
    if kind == SanitizeKind.PHONE:
        return _validate_phone
    if kind == SanitizeKind.PIN_CODE:
        return _validate_pin_code
    if kind == SanitizeKind.SLUG_KEY:
        return _validate_slug_key
    if kind == SanitizeKind.SERVICE_KEY:
        return _validate_service_key
    if kind == SanitizeKind.STATUS_STR:
        def _status(value: Any) -> str:
            if not isinstance(value, str):
                raise ValidationError("Status must be a string")
            cleaned = value.strip()
            if not cleaned:
                raise ValidationError("Status cannot be empty")
            if len(cleaned) > STATUS_MAX:
                raise ValidationError(f"Status must be at most {STATUS_MAX} characters")
            return cleaned

        return _status
    raise ValueError(f"No validator for kind {kind}")


def _repair_for_kind(kind: SanitizeKind, value: str) -> str:
    max_len = _kind_max_len(kind)
    if kind == SanitizeKind.PERSON_NAME:
        return _truncate(_letters_and_spaces_only(_strip_control_chars(value)), max_len)
    if kind in {
        SanitizeKind.SAFE_DISPLAY_NAME,
        SanitizeKind.ENGAGEMENT_CODE,
    }:
        return _truncate(_safe_display_chars_only(_strip_html(_strip_control_chars(value))), max_len)
    if kind == SanitizeKind.CITY_STATE_COUNTRY:
        return _truncate(_city_state_chars_only(_strip_html(_strip_control_chars(value))), max_len)
    if kind == SanitizeKind.PHONE:
        return _repair_phone(value)
    if kind == SanitizeKind.PIN_CODE:
        return _repair_pin_code(value)
    if kind == SanitizeKind.SLUG_KEY:
        return sanitize_cabin_key(value)
    if kind == SanitizeKind.SERVICE_KEY:
        return sanitize_service_key(value)
    if kind in {
        SanitizeKind.SAFE_TEXT,
        SanitizeKind.ADDRESS_TEXT,
        SanitizeKind.ORG_ADDRESS_TEXT,
        SanitizeKind.LANDMARK_TEXT,
        SanitizeKind.SHORT_SAFE_TEXT,
        SanitizeKind.QUESTION_TEXT,
        SanitizeKind.CHECKLIST_TEXT,
        SanitizeKind.SUPPORT_QUERY_TEXT,
        SanitizeKind.EXPERT_ABOUT_TEXT,
    }:
        return _repair_safe_text(value, max_len)
    if kind == SanitizeKind.STATUS_STR:
        return _truncate(_strip_control_chars(value.strip()), max_len)
    return value


def sanitize_email(value: Any, *, required: bool) -> SanitizeOutcome:
    if _is_blank(value):
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "Email is required")
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)
    if not isinstance(value, str):
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "Email must be a string")
        return SanitizeOutcome(SanitizeStatus.NULL, None, "Email must be a string")
    cleaned = value.strip().lower()
    try:
        validated = str(EmailStr(cleaned))
        if len(validated) > 254:
            if required:
                return SanitizeOutcome(SanitizeStatus.SKIP, value, "Email exceeds max length")
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Email exceeds max length")
        if validated == value:
            return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
        return SanitizeOutcome(SanitizeStatus.OK, validated)
    except PydanticValidationError:
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "Invalid email format")
        return SanitizeOutcome(SanitizeStatus.NULL, None, "Invalid email format")


def _repair_nested_strings(obj: Any, *, max_len: int = SAFE_TEXT_DEFAULT_MAX, depth: int = 0) -> Any:
    if depth > 10:
        return obj
    if isinstance(obj, str):
        return _repair_safe_text(obj, max_len)
    if isinstance(obj, dict):
        return {k: _repair_nested_strings(v, max_len=max_len, depth=depth + 1) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_repair_nested_strings(item, max_len=max_len, depth=depth + 1) for item in obj]
    return obj


def sanitize_nested_json(value: Any, *, required: bool, max_len: int = SAFE_TEXT_DEFAULT_MAX) -> SanitizeOutcome:
    if value is None:
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "JSON value is required")
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)
    try:
        cleaned = validate_nested_strings(value, max_len=max_len)
    except ValidationError:
        try:
            repaired = _repair_nested_strings(value, max_len=max_len)
            cleaned = validate_nested_strings(repaired, max_len=max_len)
        except ValidationError as exc:
            if required:
                return SanitizeOutcome(SanitizeStatus.SKIP, value, str(exc))
            return SanitizeOutcome(SanitizeStatus.NULL, None, str(exc))
    if cleaned == value:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
    return SanitizeOutcome(SanitizeStatus.OK, cleaned)


def sanitize_value(value: Any, *, kind: SanitizeKind, required: bool = False) -> SanitizeOutcome:
    """Normalize and validate a scalar DB value."""
    if kind == SanitizeKind.EMAIL:
        return sanitize_email(value, required=required)
    if kind == SanitizeKind.NESTED_JSON:
        return sanitize_nested_json(value, required=required)

    if _is_blank(value):
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "Value is required")
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)

    if not isinstance(value, str):
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "Value must be a string")
        return SanitizeOutcome(SanitizeStatus.NULL, None, "Value must be a string")

    validator = _validator_for_kind(kind)
    ok, validated, _ = _try_validate(validator, value)
    if ok:
        if validated == value:
            return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
        return SanitizeOutcome(SanitizeStatus.OK, validated)

    repaired = _repair_for_kind(kind, value)
    if not repaired.strip():
        if required:
            return SanitizeOutcome(SanitizeStatus.SKIP, value, "Value empty after repair")
        return SanitizeOutcome(SanitizeStatus.NULL, None, "Value empty after repair")

    ok, validated, reason = _try_validate(validator, repaired)
    if ok:
        return SanitizeOutcome(SanitizeStatus.OK, validated)

    if kind == SanitizeKind.PHONE and not required:
        return SanitizeOutcome(SanitizeStatus.NULL, None, reason or "Invalid phone")
    if kind == SanitizeKind.PIN_CODE and not required:
        return SanitizeOutcome(SanitizeStatus.NULL, None, reason or "Invalid pin code")
    if kind in {SanitizeKind.SLUG_KEY, SanitizeKind.SERVICE_KEY} and not required:
        return SanitizeOutcome(SanitizeStatus.NULL, None, reason or "Invalid key")

    if required:
        return SanitizeOutcome(SanitizeStatus.SKIP, value, reason or "Validation failed after repair")
    return SanitizeOutcome(SanitizeStatus.NULL, None, reason or "Validation failed after repair")
