"""Shared anthropometry range validation for questionnaire saves and Metsights push."""

from __future__ import annotations

import logging
from typing import Any

from core.exceptions import AppError
from modules.metsights.strategies import normalize_metsights_unit_code

logger = logging.getLogger(__name__)

_INCHES_TO_CM = 2.54

ANTHROPOMETRY_SCALE_KEYS = frozenset(
    {
        "height",
        "weight",
        "waist_circumference",
        "hip_circumference",
        "body_fat",
    }
)


def _unit_code(raw: str | None) -> str:
    unit = normalize_metsights_unit_code(str(raw or "").strip())
    return (unit or str(raw or "").strip()).lower()


def _to_cm(value: float, unit: str) -> float:
    if unit in {"1", "in", "inch", "inches"}:
        return value * _INCHES_TO_CM
    return value


def validate_scale_answer(question_key: str, answer: dict[str, Any]) -> None:
    """Reject out-of-range anthropometry scale answers at questionnaire save time."""
    key = (question_key or "").strip()
    if key not in ANTHROPOMETRY_SCALE_KEYS:
        return

    raw_val = answer.get("value")
    try:
        value = float(raw_val)
    except (TypeError, ValueError):
        return

    unit = _unit_code(answer.get("unit"))

    if key == "weight":
        if unit in {"0", "kg"} and (value < 20 or value > 300):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Weight {value} kg is out of range (expected 20–300 kg)",
            )
        if unit in {"1", "lb", "lbs"} and (value < 44 or value > 660):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Weight {value} lb is out of range (expected 44–660 lb)",
            )
        return

    if key == "height":
        if unit in {"0", "cm"} and (value < 50 or value > 250):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Height {value} cm is out of range (expected 50–250 cm)",
            )
        if unit in {"2", "ft/in", "ft", "feet", "ftin"} and (value < 1.5 or value > 8.5):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Height {value} ft/in is out of range (expected 1.5–8.5 ft/in)",
            )
        return

    if key == "waist_circumference":
        if unit in {"0", "cm"} and (value < 60 or value > 150):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Waist {value} cm is out of range (expected 60–150 cm)",
            )
        if unit in {"1", "in", "inch", "inches"} and (value < 23.62 or value > 59.05):
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Waist {value} in is out of range (expected 23.62–59.05 in)",
            )
        return

    if key == "hip_circumference":
        cm_value = _to_cm(value, unit)
        if cm_value < 27.56 or cm_value > 62.99:
            if unit in {"1", "in", "inch", "inches"}:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_INPUT",
                    message=(
                        f"Hip {value} in is out of range "
                        f"(expected 10.85–24.80 in, or 27.56–62.99 cm)"
                    ),
                )
            raise AppError(
                status_code=422,
                error_code="INVALID_INPUT",
                message=f"Hip {value} cm is out of range (expected 27.56–62.99 cm)",
            )


def validate_metsights_payload_ranges(payload: dict[str, Any]) -> None:
    """Reject out-of-range anthropometry values in a Metsights push payload."""
    for base in ANTHROPOMETRY_SCALE_KEYS:
        value = payload.get(base)
        if value is None:
            continue
        unit_key = f"{base}_unit"
        unit_raw = payload.get(unit_key)
        try:
            validate_scale_answer(base, {"value": value, "unit": unit_raw})
        except AppError:
            raise


def normalize_anthropometry_for_metsights(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert hip circumference from inches to cm for Metsights fitness-parameters."""
    out = dict(payload)
    hip = out.get("hip_circumference")
    if hip is None:
        return out
    hip_unit = _unit_code(out.get("hip_circumference_unit"))
    if hip_unit not in {"1", "in", "inch", "inches"}:
        return out
    try:
        inches = float(hip)
    except (TypeError, ValueError):
        return out
    out["hip_circumference"] = round(inches * _INCHES_TO_CM, 2)
    out["hip_circumference_unit"] = "0"
    return out


def drop_invalid_optional_hip(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove hip circumference when out of Metsights range (optional on FitPrint)."""
    out = dict(payload)
    hip = out.get("hip_circumference")
    if hip is None:
        return out
    try:
        value = float(hip)
    except (TypeError, ValueError):
        out.pop("hip_circumference", None)
        out.pop("hip_circumference_unit", None)
        return out
    cm_value = _to_cm(value, _unit_code(out.get("hip_circumference_unit")))
    if cm_value < 27.56 or cm_value > 62.99:
        logger.warning(
            "Dropping out-of-range hip_circumference before Metsights push: %s (unit=%r, ~%.2f cm)",
            hip,
            out.get("hip_circumference_unit"),
            cm_value,
        )
        out.pop("hip_circumference", None)
        out.pop("hip_circumference_unit", None)
    return out


def prepare_anthropometry_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize, drop invalid optional hip, and validate required circumferences for push."""
    prepared = normalize_anthropometry_for_metsights(payload)
    prepared = drop_invalid_optional_hip(prepared)
    validate_metsights_payload_ranges(prepared)
    return prepared
