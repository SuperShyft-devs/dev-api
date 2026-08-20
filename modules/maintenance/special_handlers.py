"""Special sanitization handlers for JSON and composite columns."""

from __future__ import annotations

import copy
import math
from typing import Any
from urllib.parse import urlparse

from common.data_sanitize import (
    SanitizeKind,
    SanitizeOutcome,
    SanitizeStatus,
    sanitize_nested_json,
    sanitize_value,
)
from common.slug import (
    collect_cabin_keys_from_slot_detail,
    migrate_slot_detail_cabin_keys,
    sanitize_cabin_key,
)
from db.seed.questionnaire_field_config import MAX_MULTI_SELECT_CHOICES
from modules.metsights.anthropometry_validation import ANTHROPOMETRY_SCALE_KEYS, clamp_scale_value

_QUESTION_TYPE_ALIASES = {"multi_choice": "multiple_choice"}


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _normalize_question_type(value: str | None) -> str:
    normalized = _normalize_text(value)
    return _QUESTION_TYPE_ALIASES.get(normalized, normalized)


def clamp_scale_answer(question_key: str, answer: dict[str, Any]) -> dict[str, Any]:
    """Clamp anthropometry scale values into valid Metsights ranges."""
    key = (question_key or "").strip()
    if key not in ANTHROPOMETRY_SCALE_KEYS:
        return answer
    return clamp_scale_value(key, answer)


def _coerce_health_priorities(answer: Any) -> list[str]:
    if isinstance(answer, str):
        stripped = answer.strip()
        return [stripped] if stripped else []
    if isinstance(answer, list):
        result: list[str] = []
        for item in answer:
            if isinstance(item, str) and item.strip() and item.strip() not in result:
                result.append(item.strip())
        max_allowed = MAX_MULTI_SELECT_CHOICES.get("health_priorities", 2)
        return result[:max_allowed]
    if answer is not None:
        text = str(answer).strip()
        return [text] if text else []
    return []


def sanitize_questionnaire_answer(
    answer: Any,
    *,
    question_key: str | None,
    question_type: str | None,
    allowed_option_values: set[str] | None = None,
) -> SanitizeOutcome:
    if answer is None:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)

    qkey = _normalize_text(question_key)
    qtype = _normalize_question_type(question_type)
    allowed = {_normalize_text(v) for v in (allowed_option_values or set()) if v}

    try:
        nested = sanitize_nested_json(answer, required=False)
        if nested.status == SanitizeStatus.NULL:
            return nested
        cleaned = nested.value if nested.value is not None else answer
    except Exception as exc:
        return SanitizeOutcome(SanitizeStatus.NULL, None, str(exc))

    if qkey == "health_priorities":
        cleaned = _coerce_health_priorities(cleaned)

    if qtype == "scale" and isinstance(cleaned, dict):
        cleaned = clamp_scale_answer(qkey, cleaned)
        value = cleaned.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Invalid scale value")
        unit = _normalize_text(cleaned.get("unit"))
        if not unit:
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Missing scale unit")

    elif qtype == "single_choice":
        if isinstance(cleaned, list) and cleaned:
            cleaned = cleaned[0]
        if not isinstance(cleaned, str):
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Single choice must be string")
        if allowed and _normalize_text(cleaned) not in allowed:
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Invalid single choice option")

    elif qtype == "multiple_choice":
        if isinstance(cleaned, str):
            cleaned = [cleaned.strip()] if cleaned.strip() else []
        if not isinstance(cleaned, list):
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Multiple choice must be list")
        filtered: list[str] = []
        for item in cleaned:
            if not isinstance(item, str):
                continue
            if allowed and _normalize_text(item) not in allowed:
                continue
            if item.strip() and item.strip() not in filtered:
                filtered.append(item.strip())
        max_allowed = MAX_MULTI_SELECT_CHOICES.get(qkey)
        if max_allowed is not None:
            filtered = filtered[:max_allowed]
        cleaned = filtered
        if not cleaned:
            return SanitizeOutcome(SanitizeStatus.NULL, None, "No valid multiple choice options")

    elif qtype == "text":
        if not isinstance(cleaned, str):
            return SanitizeOutcome(SanitizeStatus.NULL, None, "Text answer must be string")

    if cleaned == answer:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, answer)
    return SanitizeOutcome(SanitizeStatus.OK, cleaned)


def sanitize_slot_detail(value: Any) -> SanitizeOutcome:
    if value is None:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)
    if not isinstance(value, dict):
        return SanitizeOutcome(SanitizeStatus.NULL, None, "slot_detail must be object")

    updated = copy.deepcopy(value)
    for section_name in ("blood_collection", "consultation"):
        section = updated.get(section_name)
        if not isinstance(section, dict):
            continue
        for cabins in section.values():
            if not isinstance(cabins, list):
                continue
            for cabin in cabins:
                if not isinstance(cabin, dict):
                    continue
                name_out = sanitize_value(cabin.get("cabin_name"), kind=SanitizeKind.SAFE_DISPLAY_NAME)
                if name_out.status == SanitizeStatus.OK:
                    cabin["cabin_name"] = name_out.value
                elif name_out.status == SanitizeStatus.NULL:
                    cabin["cabin_name"] = None
                expert_type = cabin.get("expert_type")
                if expert_type is not None:
                    et_out = sanitize_value(expert_type, kind=SanitizeKind.SLUG_KEY)
                    if et_out.status == SanitizeStatus.OK:
                        cabin["expert_type"] = et_out.value
                    elif et_out.status == SanitizeStatus.NULL:
                        cabin.pop("expert_type", None)

    migrated, _ = migrate_slot_detail_cabin_keys(updated)
    if migrated == value:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
    return SanitizeOutcome(SanitizeStatus.OK, migrated)


def sanitize_org_departments(value: Any) -> SanitizeOutcome:
    if value is None:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)
    if not isinstance(value, list):
        return SanitizeOutcome(SanitizeStatus.NULL, None, "departments must be list")

    changed = False
    cleaned_list: list[Any] = []
    for item in value:
        if not isinstance(item, dict):
            changed = True
            continue
        row = dict(item)
        dept = row.get("department")
        if dept is not None:
            out = sanitize_value(dept, kind=SanitizeKind.SAFE_DISPLAY_NAME)
            if out.status == SanitizeStatus.OK:
                row["department"] = out.value
                if out.value != dept:
                    changed = True
            elif out.status == SanitizeStatus.NULL:
                row["department"] = None
                changed = True
        cleaned_list.append(row)

    if not changed:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
    return SanitizeOutcome(SanitizeStatus.OK, cleaned_list)


def _is_valid_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value.strip())
    except ValueError:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def sanitize_notification_services_json(value: Any) -> SanitizeOutcome:
    if value is None:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)
    if not isinstance(value, list):
        return SanitizeOutcome(SanitizeStatus.NULL, None, "notification_services must be list")

    changed = False
    cleaned: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            changed = True
            continue
        row = dict(item)
        sk = row.get("service_key")
        if sk is not None:
            out = sanitize_value(sk, kind=SanitizeKind.SERVICE_KEY, required=True)
            if out.status == SanitizeStatus.SKIP:
                changed = True
                continue
            if out.status == SanitizeStatus.OK:
                row["service_key"] = out.value
                if out.value != sk:
                    changed = True
        link = row.get("external_link")
        if link is not None and isinstance(link, str):
            if not _is_valid_http_url(link):
                row["external_link"] = None
                changed = True
        cleaned.append(row)

    if not changed:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
    return SanitizeOutcome(SanitizeStatus.OK, cleaned)


def sanitize_expert_languages(value: Any) -> SanitizeOutcome:
    if value is None:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)
    if not isinstance(value, list):
        return SanitizeOutcome(SanitizeStatus.NULL, None, "languages must be list")

    changed = False
    cleaned: list[str] = []
    for item in value:
        if not isinstance(item, str):
            changed = True
            continue
        out = sanitize_value(item, kind=SanitizeKind.SAFE_DISPLAY_NAME)
        if out.status in {SanitizeStatus.OK, SanitizeStatus.UNCHANGED} and out.value:
            if out.value != item:
                changed = True
            cleaned.append(str(out.value))
        else:
            changed = True

    if not changed:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, value)
    return SanitizeOutcome(SanitizeStatus.OK, cleaned)


def sanitize_camp_report_json(value: Any) -> SanitizeOutcome:
    return sanitize_nested_json(value, required=False)


def resolve_cabin_or_null(
    cabin_value: Any,
    *,
    valid_keys: set[str],
) -> SanitizeOutcome:
    if cabin_value is None or (isinstance(cabin_value, str) and not cabin_value.strip()):
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, None)

    out = sanitize_value(cabin_value, kind=SanitizeKind.SLUG_KEY)
    if out.status == SanitizeStatus.SKIP:
        return SanitizeOutcome(SanitizeStatus.NULL, None, out.reason)
    if out.status == SanitizeStatus.NULL:
        return SanitizeOutcome(SanitizeStatus.NULL, None, out.reason)

    key = str(out.value or sanitize_cabin_key(str(cabin_value)))
    if not key:
        return SanitizeOutcome(SanitizeStatus.NULL, None, "Empty cabin key")
    if valid_keys and key not in valid_keys:
        return SanitizeOutcome(SanitizeStatus.NULL, None, "Orphan cabin key")
    if key == cabin_value:
        return SanitizeOutcome(SanitizeStatus.UNCHANGED, cabin_value)
    return SanitizeOutcome(SanitizeStatus.OK, key)
