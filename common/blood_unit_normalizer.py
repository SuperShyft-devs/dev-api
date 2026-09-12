"""Shared blood-parameter unit normalization for draft and Metsights push."""

from __future__ import annotations

import re
from typing import Any, Protocol


class _UnitOption(Protocol):
    option_value: Any
    display_name: Any


# Canonical synonym groups: any label in a group maps to the same fingerprint.
_UNIT_SYNONYM_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"miuml", "mluml"}),  # mIU/mL + Healthians mlU/ml typo only
    frozenset({"miul"}),  # mIU/L — not equivalent to mIU/mL
    frozenset({"uiuml", "uiul"}),
    frozenset({"mgdl"}),
    frozenset({"gdl"}),
    frozenset({"ngml"}),
    frozenset({"ngdl"}),
    frozenset({"pgml"}),
    frozenset({"ugdl"}),
    frozenset({"103ul", "103µl", "103μl", "thousand/ul"}),
    frozenset({"106ul", "106µl", "million/ul"}),
    frozenset({"mmhr", "mm1sthour"}),
    frozenset({"mlmin173m2"}),
)


def fingerprint_unit_label(unit: str | None) -> str:
    """Normalize a unit string to a comparable fingerprint."""
    if unit is None:
        return ""
    s = str(unit).strip().lower()
    s = s.replace("µ", "u").replace("μ", "u")
    s = s.replace("³", "3").replace("⁶", "6").replace("²", "2")
    s = re.sub(r"[^a-z0-9]", "", s)
    # Healthians typo: mlU/ml uses letter L instead of I
    if s == "mluml":
        s = "miuml"
    return s


def units_equivalent(a: str | None, b: str | None) -> bool:
    fa = fingerprint_unit_label(a)
    fb = fingerprint_unit_label(b)
    if not fa or not fb:
        return False
    if fa == fb:
        return True
    for group in _UNIT_SYNONYM_GROUPS:
        if fa in group and fb in group:
            return True
    return False


def map_unit_to_metsights_option_value(
    unit: str | None,
    options: list[_UnitOption],
) -> str | None:
    """Map a Healthians/IHR unit string to questionnaire ``option_value`` (Metsights code)."""
    if unit is None:
        return None
    candidate = str(unit).strip()
    if not candidate:
        return None

    normalized = fingerprint_unit_label(candidate)
    # Pass 1: exact fingerprint match on option value or display name
    for option in options:
        option_value = str(getattr(option, "option_value", None) or "").strip()
        display_name = str(getattr(option, "display_name", None) or "").strip()
        if fingerprint_unit_label(option_value) == normalized:
            return option_value
        if fingerprint_unit_label(display_name) == normalized:
            return option_value
    # Pass 2: synonym groups (e.g. mlU/ml → mIU/mL, mm/1st hour → mm/hr)
    for option in options:
        option_value = str(getattr(option, "option_value", None) or "").strip()
        display_name = str(getattr(option, "display_name", None) or "").strip()
        if units_equivalent(candidate, display_name):
            return option_value
        if units_equivalent(candidate, option_value):
            return option_value
    return None


# Display labels that may appear in questionnaire answers instead of codes.
_BLOOD_UNIT_LABEL_TO_CODE: dict[str, str] = {
    "0": "0",
    "1": "1",
    "2": "2",
    "3": "3",
    "4": "4",
    "5": "5",
    "g/dl": "0",
    "mg/dl": "0",
    "mg/l": "0",
    "u/l": "0",
    "iu/l": "1",
    "miu/l": "0",
    "miu/ml": "3",
    "ng/ml": "2",
    "ng/dl": "0",
    "pg/ml": "1",
    "ug/dl": "1",
    "µg/dl": "1",
    "μg/dl": "1",
    "%": "0",
    "mm/hr": "0",
    "mm/1st hour": "0",
    "ml/min/1.73 m²": "0",
    "ml/min/1.73m2": "0",
    "10³/μl": "3",
    "10^3/uL": "3",
    "10^3/µl": "3",
}


def normalize_blood_unit_code_for_push(unit: str | None) -> str | None:
    """Map stored unit (code or display label) to Metsights unit code for push."""
    if unit is None:
        return None
    raw = str(unit).strip()
    if not raw:
        return None
    if raw in {"0", "1", "2", "3", "4", "5"}:
        return raw
    mapped = _BLOOD_UNIT_LABEL_TO_CODE.get(raw) or _BLOOD_UNIT_LABEL_TO_CODE.get(raw.lower())
    if mapped is not None:
        return mapped
    return raw


# Admin visualization: known unit string normalizations
UNIT_SYNONYM_EXAMPLES: list[dict[str, str]] = [
    {"healthians": "mlU/ml", "maps_to": "mIU/mL", "metsights_code": "3"},
    {"healthians": "mIU/mL", "maps_to": "mIU/mL", "metsights_code": "3"},
    {"healthians": "µIU/mL", "maps_to": "μIU/mL", "metsights_code": "2"},
    {"healthians": "ng/mL", "maps_to": "ng/mL", "metsights_code": "2"},
    {"healthians": "ng/dl", "maps_to": "ng/dl", "metsights_code": "0"},
    {"healthians": "mm/1st hour", "maps_to": "mm/hr", "metsights_code": "0"},
    {"healthians": "mL/min/1.73m2", "maps_to": "ml/min/1.73 m²", "metsights_code": "0"},
    {"healthians": "10^3/uL", "maps_to": "10³/μl", "metsights_code": "3"},
    {"healthians": "mg/dl", "maps_to": "mg/dL", "metsights_code": "0"},
    {"healthians": "ug/dl", "maps_to": "μg/dL", "metsights_code": "1"},
]
