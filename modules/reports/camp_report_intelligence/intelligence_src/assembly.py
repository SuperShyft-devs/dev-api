"""Section-aware assembly: enrich Camp Report JSON with intelligence in place.

Keeps ``generate_report_insights`` generation logic unchanged. Only maps its
output onto existing camp-report section objects under an ``intelligence`` key.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, Mapping, MutableMapping, Optional

from .engine import generate_report_insights, generate_section_insights

# Camp section keys that may receive an ``intelligence`` field (engine-backed).
INTELLIGENCE_CAMP_SECTIONS: frozenset[str] = frozenset(
    {
        "overall_risk_score",
        "distribution_by_physical_activity_frequency",
        "distribution_by_sleeping_hours",
        "distribution_by_oxidative_stress",
        "distribution_by_gender_by_metabolic_syndrome",
        "participation_by_age",
        "positive_wins",
    }
)

# Engine ``concerns`` keys → camp section key (1:1 narratives).
_CONCERN_TO_SECTION: Mapping[str, str] = {
    "overall_risk": "overall_risk_score",
    "physical_activity": "distribution_by_physical_activity_frequency",
    "sleep": "distribution_by_sleeping_hours",
    "oxidative_stress": "distribution_by_oxidative_stress",
    "participation": "participation_by_age",
}

_SECTION_ALIASES: Mapping[str, str] = {
    **{camp_key: camp_key for camp_key in INTELLIGENCE_CAMP_SECTIONS},
    **_CONCERN_TO_SECTION,
    "disease_risks": "distribution_by_gender_by_metabolic_syndrome",
    "disease_deep_dive": "distribution_by_gender_by_metabolic_syndrome",
    "gender_comparison": "distribution_by_gender_by_metabolic_syndrome",
    "positive_highlights": "positive_wins",
    "positives": "positive_wins",
}


def resolve_intelligence_section(section: str) -> str:
    normalized = (section or "").strip()
    if not normalized:
        raise ValueError("section is required")
    camp_key = _SECTION_ALIASES.get(normalized)
    if camp_key is None:
        raise ValueError(f"unsupported intelligence section: {normalized}")
    return camp_key


def enrich_camp_report_with_intelligence(report: dict) -> dict:
    """Return a deep copy of ``report`` with section-level ``intelligence`` attached.

    Preserves exact top-level keys and each section's ``data``, ``name``, and
    ``description``. Does not add new top-level sections. Does not include
    ``profile`` or ``leadership_cards`` in the camp JSON. Attached
    ``intelligence`` is the frontend contract only (tone / observation /
    explanation / recommendation); engine metadata stays on the internal
    ``generate_report_insights`` payload.

    Unmapped sections (``kpis``, ``blood_and_lab_intelligence``,
    ``company_average_scores``, ``ranking``, ``meta``, …) are left unchanged.
    """
    if not isinstance(report, dict):
        raise TypeError("report must be a dict")

    enriched: Dict[str, Any] = copy.deepcopy(report)
    insights = generate_report_insights(report)
    concerns = insights.get("concerns") or {}
    if not isinstance(concerns, dict):
        concerns = {}

    for concern_key, section_key in _CONCERN_TO_SECTION.items():
        if concern_key not in concerns:
            continue
        _attach_intelligence(enriched, section_key, concerns[concern_key])

    metabolic_intel = _metabolic_intelligence(concerns)
    if metabolic_intel is not None:
        _attach_intelligence(
            enriched,
            "distribution_by_gender_by_metabolic_syndrome",
            metabolic_intel,
        )

    positives_intel = _positives_intelligence(concerns, insights)
    if positives_intel is not None:
        _attach_intelligence(enriched, "positive_wins", positives_intel)

    return enriched


def generate_camp_section_intelligence(report: dict, section: str) -> tuple[str, Any]:
    if not isinstance(report, dict):
        raise TypeError("report must be a dict")

    camp_key = resolve_intelligence_section(section)
    if camp_key == "distribution_by_gender_by_metabolic_syndrome":
        disease_risks = generate_section_insights(report, "disease_risks")
        deep_dive = generate_section_insights(report, "disease_deep_dive")
        concerns = {
            **(disease_risks.get("concerns") or {}),
            **(deep_dive.get("concerns") or {}),
        }
        intelligence = _metabolic_intelligence(concerns)
    elif camp_key == "positive_wins":
        insights = generate_section_insights(report, "positive_highlights")
        intelligence = _positives_intelligence(insights.get("concerns") or {}, insights)
    else:
        concern_key = next(
            (engine_id for engine_id, mapped in _CONCERN_TO_SECTION.items() if mapped == camp_key),
            None,
        )
        if concern_key is None:
            raise ValueError(f"unsupported intelligence section: {section}")
        insights = generate_section_insights(report, concern_key)
        concerns = insights.get("concerns") or {}
        intelligence = concerns.get(concern_key)

    if intelligence is None:
        raise ValueError(f"no intelligence produced for section: {section}")
    return camp_key, _public_intelligence(intelligence)


def _metabolic_intelligence(concerns: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    payload: Dict[str, Any] = {}
    if "disease_risks" in concerns:
        payload["disease_risks"] = concerns["disease_risks"]
    if "disease_deep_dive" in concerns:
        payload["disease_deep_dive"] = concerns["disease_deep_dive"]
    return payload or None


def _positives_intelligence(
    concerns: Mapping[str, Any],
    insights: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    payload: Dict[str, Any] = {}
    if "positive_highlights" in concerns:
        payload["positive_highlights"] = concerns["positive_highlights"]
    if "positives" in insights:
        payload["positives"] = insights["positives"]
    return payload or None


# Frontend dashboard contract. Internal engine metadata is calculated and
# retained on ``generate_report_insights``; it is stripped only here.
_PUBLIC_NARRATIVE_KEYS: tuple[str, str, str, str] = (
    "tone",
    "observation",
    "explanation",
    "recommendation",
)


def _is_engine_narrative(payload: Mapping[str, Any]) -> bool:
    """True for a serialized ChartNarrative (tone/text/structured/confidence)."""
    if "structured" in payload:
        return True
    if "tone" in payload and ("text" in payload or "observation" in payload):
        return True
    return False


def _public_narrative(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Project one engine narrative onto the frontend intelligence contract."""
    structured = payload.get("structured")
    structured = structured if isinstance(structured, Mapping) else {}
    values = {
        "tone": payload.get("tone") or structured.get("tone") or "",
        "observation": structured.get("observation") or payload.get("observation") or "",
        "explanation": structured.get("explanation") or payload.get("explanation") or "",
        "recommendation": structured.get("recommendation") or payload.get("recommendation") or "",
    }
    return {key: values[key] for key in _PUBLIC_NARRATIVE_KEYS}


def _public_intelligence(payload: Any) -> Any:
    """Recursively expose only frontend fields; keep nested section maps."""
    if not isinstance(payload, Mapping):
        return payload
    if _is_engine_narrative(payload):
        return _public_narrative(payload)
    return {str(key): _public_intelligence(value) for key, value in payload.items()}


def _attach_intelligence(
    report: MutableMapping[str, Any],
    section_key: str,
    intelligence: Any,
) -> None:
    """Attach intelligence only if the section already exists as a dict."""
    section = report.get(section_key)
    if not isinstance(section, dict):
        return
    section["intelligence"] = _public_intelligence(intelligence)
