"""Convert questionnaire scale answers to Aurae onboard units."""

from __future__ import annotations

from typing import Any


def _parse_numeric(raw: Any) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def extract_scale_answer(answer: Any) -> tuple[float | None, str | None]:
    """Return (value, unit) from a questionnaire scale answer dict."""
    if not isinstance(answer, dict):
        return None, None
    value = _parse_numeric(answer.get("value"))
    raw_unit = answer.get("unit")
    unit = str(raw_unit).strip() if raw_unit is not None and str(raw_unit).strip() else None
    return value, unit


def height_to_cm(value: float, unit: str | None) -> int:
    """Convert height to centimetres (Aurae expects int cm).

    Units: ``0`` / ``cm`` = centimetres; ``2`` / ``ft/in`` = feet + inches
    where the integer part is feet and the fractional digits are inches
    (e.g. ``5.10`` / ``5.1`` → 5′10″, ``5.9`` → 5′9″).
    """
    normalized = (unit or "0").strip().lower()
    if normalized in {"0", "cm"}:
        return int(round(value))
    if normalized in {"2", "ft/in", "ft", "feet", "ftin"}:
        feet = int(value)
        frac = abs(value) - abs(feet)
        hundredths = int(round(frac * 100))
        if hundredths <= 11:
            inches = hundredths
        else:
            tenths = int(round(frac * 10))
            inches = tenths if tenths <= 11 else min(hundredths, 11)
        total_inches = feet * 12 + inches
        return int(round(total_inches * 2.54))
    raise ValueError(f"Unsupported height unit: {unit}")


def weight_to_kg(value: float, unit: str | None) -> int:
    """Convert weight to kilograms (Aurae expects int kg)."""
    normalized = (unit or "0").strip().lower()
    if normalized in {"0", "kg"}:
        return int(round(value))
    if normalized in {"1", "lb", "lbs", "pound", "pounds"}:
        return int(round(value * 0.453592))
    raise ValueError(f"Unsupported weight unit: {unit}")


def waist_to_inches(value: float, unit: str | None) -> int:
    """Convert waist circumference to inches (Aurae expects int inches)."""
    normalized = (unit or "0").strip().lower()
    if normalized in {"1", "in", "inch", "inches"}:
        return int(round(value))
    if normalized in {"0", "cm"}:
        return int(round(value / 2.54))
    raise ValueError(f"Unsupported waist unit: {unit}")


def normalize_aurae_gender(raw: str | None) -> str | None:
    """Map stored gender labels to Aurae ``male`` | ``female``."""
    v = (raw or "").strip().lower()
    if not v:
        return None
    if v in {"1", "m", "male", "man"}:
        return "male"
    if v in {"2", "f", "female", "woman"}:
        return "female"
    if v.startswith("m"):
        return "male"
    if v.startswith("f"):
        return "female"
    return None
