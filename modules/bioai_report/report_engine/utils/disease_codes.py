"""Map MetSights disease codes to knowledge-base disease ids."""

from __future__ import annotations

# MetSights / assessment codes → KB file stem (disease_id).
_DISEASE_CODE_ALIASES: dict[str, str] = {
    "type_2_diabetes": "type2_diabetes",
    "type2_diabetes": "type2_diabetes",
    "diabetes": "type2_diabetes",
    "thyroid": "thyroid_health",
    "thyroid_health": "thyroid_health",
    "nafld": "nafld",
    "hypertension": "hypertension",
    "obesity": "obesity",
    "oxidative_stress": "oxidative_stress",
    "dyslipidemia": "dyslipidemia",
    "cardiac_health": "cardiac_health",
    "metabolic_syndrome": "metabolic_syndrome",
    "pcos": "pcos",
    "polycystic_ovary_syndrome": "pcos",
    "polycystic_ovarian_syndrome": "pcos",
}


def normalize_disease_code(code: str | None) -> str | None:
    """Return the canonical KB disease_id for an assessment disease code."""
    if not code or not isinstance(code, str):
        return None
    key = code.strip().lower().replace("-", "_").replace(" ", "_")
    if not key:
        return None
    if key in _DISEASE_CODE_ALIASES:
        return _DISEASE_CODE_ALIASES[key]
    # Pass through unknown codes so new KB files work without code changes.
    return key
