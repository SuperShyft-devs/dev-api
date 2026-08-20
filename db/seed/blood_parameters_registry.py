"""Blood parameter question registry from Metsights OPTIONS snapshots.

Source files (Metsights OPTIONS responses):
  - db/seed/data/blood-parameters.txt
  - db/seed/data/advanced-blood-parameters.txt
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DATA_DIR = Path(__file__).resolve().parent / "data"

BLOOD_PARAMETER_CATEGORY_KEY = "blood-parameters"
ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY = "advanced-blood-parameters"

BLOOD_PARAMETER_CATEGORY_DISPLAY = "Blood Parameters"
ADVANCED_BLOOD_PARAMETER_CATEGORY_DISPLAY = "Advanced Blood Parameters"

# Metsights record detail nested keys (GET /records/:id/)
BLOOD_PARAMETER_DETAIL_FIELD = "blood_parameter"
ADVANCED_BLOOD_PARAMETER_DETAIL_FIELD = "advanced_blood_parameter"

_ADVANCED_OVERRIDE_KEYS = frozenset({"lh_value", "fsh_value", "testosterone"})


@dataclass(frozen=True)
class BloodParameterField:
    question_key: str
    label: str
    help_text: str | None
    required: bool
    units: tuple[tuple[str, str], ...]
    unitless: bool = False


def _parse_options_file(path: Path) -> list[BloodParameterField]:
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    fields: dict[str, Any] = data["actions"]["POST"]
    params: list[BloodParameterField] = []

    for key, meta in fields.items():
        if key == "id" or key.endswith("_unit"):
            continue
        if meta.get("type") != "float":
            continue

        unit_key = f"{key}_unit"
        unitless = unit_key not in fields
        units: list[tuple[str, str]] = []
        if not unitless:
            units = [
                (str(choice["value"]), str(choice["display_name"]))
                for choice in fields[unit_key].get("choices", [])
            ]
        elif key == "ptinr_value":
            units = [("0", "ratio")]
            unitless = True

        if not units:
            continue

        params.append(
            BloodParameterField(
                question_key=key,
                label=str(meta.get("label") or key),
                help_text=(str(meta["help_text"]).strip() or None) if meta.get("help_text") else None,
                required=bool(meta.get("required", False)),
                units=tuple(units),
                unitless=unitless,
            )
        )
    return params


def _build_registries() -> tuple[
    tuple[BloodParameterField, ...],
    tuple[BloodParameterField, ...],
    frozenset[str],
]:
    blood_raw = _parse_options_file(_DATA_DIR / "blood-parameters.txt")
    advanced_raw = _parse_options_file(_DATA_DIR / "advanced-blood-parameters.txt")
    advanced_by_key = {field.question_key: field for field in advanced_raw}

    blood_fields: list[BloodParameterField] = []
    for field in blood_raw:
        override = advanced_by_key.get(field.question_key)
        if override is not None:
            blood_fields.append(
                BloodParameterField(
                    question_key=field.question_key,
                    label=override.label,
                    help_text=override.help_text or field.help_text,
                    required=field.required or override.required,
                    units=field.units or override.units,
                    unitless=field.unitless,
                )
            )
        else:
            blood_fields.append(field)

    all_keys = frozenset(field.question_key for field in blood_fields)
    return tuple(blood_fields), tuple(advanced_raw), all_keys


(
    BLOOD_PARAMETER_FIELDS,
    ADVANCED_BLOOD_PARAMETER_FIELDS,
    ALL_BLOOD_PARAMETER_KEYS,
) = _build_registries()

BLOOD_PARAMETER_QUESTION_ORDER: tuple[str, ...] = tuple(
    field.question_key for field in BLOOD_PARAMETER_FIELDS
)
ADVANCED_BLOOD_PARAMETER_QUESTION_ORDER: tuple[str, ...] = tuple(
    field.question_key for field in ADVANCED_BLOOD_PARAMETER_FIELDS
)

UNITLESS_BLOOD_PARAMETER_KEYS: frozenset[str] = frozenset(
    field.question_key for field in BLOOD_PARAMETER_FIELDS if field.unitless
)

FIELD_BY_KEY: dict[str, BloodParameterField] = {
    field.question_key: field for field in BLOOD_PARAMETER_FIELDS
}

# Internal average fallbacks used when a *full* blood report cannot be pushed to
# Metsights (missing/rejected mandatory digital values). Values are
# ``(numeric_value, metsights_unit_option_code)``. Never apply for partial reports.
BLOOD_PARAMETER_INTERNAL_FALLBACKS: dict[str, tuple[float, str]] = {
    "total_cholesterol": (170.0, "0"),  # mg/dL
    "hdlc_value": (50.0, "0"),  # mg/dL
    "ldlc_value": (100.0, "0"),  # mg/dL
    "triglycerides": (100.0, "0"),  # mg/dL
    "glucose_fasting": (90.0, "0"),  # mg/dL
    "glycated_haemoglobin": (5.2, "0"),  # %
    "insulin": (6.0, "0"),  # µIU/mL / uIU/mL
    "triiodothyronine": (1.1, "1"),  # ng/mL
    "thyroxine": (8.0, "1"),  # µg/dL
    "tsh_value": (2.0, "0"),  # mIU/L
    "wbc_value": (7.0, "3"),  # 10³/µL
    "platelets": (250.0, "3"),  # 10³/µL
    "monocytes": (6.0, "4"),  # %
    "alt_value": (20.0, "0"),  # U/L
    "ast_value": (20.0, "0"),  # U/L
    "uric_acid": (5.0, "0"),  # mg/dL
    "ggt_value": (20.0, "0"),  # U/L
    "lh_value": (5.0, "0"),  # mIU/L
    "fsh_value": (5.0, "0"),  # mIU/L
    "testosterone": (400.0, "0"),  # ng/dl
}


def build_blood_parameter_metsights_sync(question_key: str) -> dict[str, Any]:
    """Return metsights_sync JSON for a blood parameter question."""
    if question_key in UNITLESS_BLOOD_PARAMETER_KEYS:
        return {
            "pull": {"enabled": True, "strategy": "scale_ingest_unitless"},
            "push": {"enabled": True, "strategy": "scale_emit_unitless"},
        }
    return {
        "pull": {"enabled": True, "strategy": "scale_ingest"},
        "push": {"enabled": True, "strategy": "scale_emit"},
    }
