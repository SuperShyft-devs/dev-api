"""Normalize raw MetSights / assessment JSON into AssessmentPayload."""

from __future__ import annotations

from typing import Any

from modules.bioai_report.report_engine.models.assessment import (
    AssessmentDisease,
    AssessmentPayload,
)


def _unwrap_data(raw: dict[str, Any]) -> dict[str, Any]:
    """Prefer nested ``data`` when top-level scored fields are absent."""
    if "diseases" in raw or "metabolic_score" in raw or "metabolic_age" in raw:
        return raw
    nested = raw.get("data")
    if isinstance(nested, dict):
        return nested
    return raw


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_optional_number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
        if number.is_integer():
            return int(number)
        return number
    return None


def _parse_diseases(raw_diseases: Any) -> list[AssessmentDisease]:
    if not isinstance(raw_diseases, list):
        return []
    diseases: list[AssessmentDisease] = []
    for entry in raw_diseases:
        if not isinstance(entry, dict):
            continue
        code = entry.get("code")
        if not isinstance(code, str) or not code.strip():
            continue
        diseases.append(
            AssessmentDisease(
                code=code.strip(),
                name=_as_optional_str(entry.get("name")),
                risk_status=_as_optional_str(entry.get("risk_status")),
                risk_score_scaled=_as_optional_number(entry.get("risk_score_scaled")),
                healthy_percentile=_as_optional_number(entry.get("healthy_percentile")),
                lifestyle_contribution=_as_optional_number(entry.get("lifestyle_contribution")),
                disease_percentile=_as_optional_number(entry.get("disease_percentile")),
                risk_status_message=_as_optional_str(entry.get("risk_status_message")),
                lifestyle_contribution_message=_as_optional_str(
                    entry.get("lifestyle_contribution_message")
                ),
            )
        )
    return diseases


def normalize_assessment(
    raw: dict[str, Any],
    *,
    record_id: str | None = None,
) -> AssessmentPayload:
    """Convert MetSights report JSON into a typed AssessmentPayload."""
    if not isinstance(raw, dict):
        raise TypeError("assessment payload must be a dict")

    body = _unwrap_data(raw)
    resolved_record_id = (
        _as_optional_str(record_id)
        or _as_optional_str(body.get("record_id"))
        or _as_optional_str(body.get("id"))
        or _as_optional_str(raw.get("record_id"))
    )

    sex = _as_optional_str(body.get("sex"))
    gender = _as_optional_str(body.get("gender")) or sex
    if sex is None and gender is not None:
        sex = gender

    return AssessmentPayload(
        record_id=resolved_record_id,
        name=_as_optional_str(body.get("name")),
        age=_as_optional_number(body.get("age")),
        sex=sex,
        gender=gender,
        date_of_birth=_as_optional_str(body.get("date_of_birth")),
        height=_as_optional_number(body.get("height")),
        weight=_as_optional_number(body.get("weight")),
        bmi=_as_optional_number(body.get("bmi")),
        profile_id=_as_optional_str(body.get("profile_id")),
        metabolic_age=_as_optional_number(body.get("metabolic_age")),
        metabolic_score=_as_optional_number(body.get("metabolic_score")),
        metabolic_health_status=_as_optional_str(body.get("metabolic_health_status")),
        assessment_code=_as_optional_str(body.get("assessment_code")),
        assessment_date=_as_optional_str(body.get("assessment_date"))
        or _as_optional_str(body.get("created_at")),
        created_at=_as_optional_str(body.get("created_at")),
        diseases=_parse_diseases(body.get("diseases")),
        raw=raw,
    )
