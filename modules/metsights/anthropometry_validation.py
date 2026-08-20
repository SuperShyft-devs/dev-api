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

# Metsights-accepted ranges keyed by question → unit aliases → (min, max).
# Circumference inch bounds are cm_min/2.54 and cm_max/2.54.
_SCALE_RANGES: dict[str, dict[frozenset[str], tuple[float, float, str]]] = {
    "weight": {
        frozenset({"0", "kg"}): (20.0, 300.0, "kg"),
        frozenset({"1", "lb", "lbs"}): (44.0, 660.0, "lb"),
    },
    "height": {
        frozenset({"0", "cm"}): (50.0, 250.0, "cm"),
        frozenset({"2", "ft/in", "ft", "feet", "ftin"}): (1.5, 8.5, "ft/in"),
    },
    "waist_circumference": {
        frozenset({"0", "cm"}): (60.0, 150.0, "cm"),
        frozenset({"1", "in", "inch", "inches"}): (23.62, 59.05, "in"),
    },
    "hip_circumference": {
        frozenset({"0", "cm"}): (70.0, 160.0, "cm"),
        frozenset({"1", "in", "inch", "inches"}): (27.56, 62.99, "in"),
    },
    "body_fat": {
        frozenset({"0", "%", "percent", "pct", "percentage"}): (1.0, 60.0, "%"),
    },
}

_HIP_CM_MIN = 70.0
_HIP_CM_MAX = 160.0


def _unit_code(raw: str | None) -> str:
    unit = normalize_metsights_unit_code(str(raw or "").strip())
    return (unit or str(raw or "").strip()).lower()


def _to_cm(value: float, unit: str) -> float:
    if unit in {"1", "in", "inch", "inches"}:
        return value * _INCHES_TO_CM
    return value


def get_scale_range(question_key: str, unit: str | None) -> tuple[float, float, str] | None:
    """Return (min, max, unit_label) for an anthropometry scale answer, if known."""
    by_unit = _SCALE_RANGES.get((question_key or "").strip())
    if not by_unit:
        return None
    normalized = _unit_code(unit)
    for aliases, bounds in by_unit.items():
        if normalized in aliases:
            return bounds
    return None


def clamp_scale_value(question_key: str, answer: dict[str, Any]) -> dict[str, Any]:
    """Clamp an anthropometry scale answer into its Metsights range for the given unit."""
    out = dict(answer)
    raw_val = out.get("value")
    try:
        value = float(raw_val)
    except (TypeError, ValueError):
        return out
    bounds = get_scale_range(question_key, out.get("unit"))
    if bounds is None:
        return out
    lo, hi, _label = bounds
    out["value"] = min(max(value, lo), hi)
    return out


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

    bounds = get_scale_range(key, answer.get("unit"))
    if bounds is None:
        return

    lo, hi, label = bounds
    if value < lo or value > hi:
        pretty = {
            "weight": "Weight",
            "height": "Height",
            "waist_circumference": "Waist",
            "hip_circumference": "Hip",
            "body_fat": "Body fat",
        }.get(key, key)
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message=f"{pretty} {value} {label} is out of range (expected {lo}–{hi} {label})",
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
    if cm_value < _HIP_CM_MIN or cm_value > _HIP_CM_MAX:
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
