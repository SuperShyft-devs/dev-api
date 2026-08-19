"""Public Health Intelligence Engine entrypoint.

``generate_report_insights(report_json)`` builds the raw intelligence payload
(profile / concerns / leadership_cards). For camp-report–shaped responses, use
``enrich_camp_report_with_intelligence`` in ``assembly.py`` (or the package
export). No FastAPI, SQLAlchemy, or auth dependencies.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .analyzer import (
    dashboard_input_from_camp_report,
    finding_from_disease,
    finding_from_overall_risk,
    finding_from_oxidative,
    finding_from_participation,
    finding_from_physical_activity,
    finding_from_positive_wins,
    finding_from_sleep,
    finding_from_top_disease,
)
from .formatter import leadership_to_dict, narrative_to_dict, to_jsonable
from .generator import (
    RecommendationLedger,
    compose_company_profile,
    generate_insight,
    generate_leadership_takeaways,
    generate_positive_insights,
)
from .knowledge import disease_metric_from_name
from .models import LifestyleGenderView, MetricFinding, SectionId

_GENDER_VIEWS: List[LifestyleGenderView] = ["both", "male", "female"]

_SIMPLE_SECTIONS: List[SectionId] = [
    "overall_risk",
    "disease_risks",
    "oxidative_stress",
    "positive_highlights",
    "participation",
]


def generate_report_insights(report_json: dict) -> dict:
    """Build organisational intelligence from a stored camp report JSON.

    Returns::

        {
            "profile": {...},
            "concerns": {
                "overall_risk": narrative,
                "physical_activity": {"both"|"male"|"female": narrative},
                "sleep": {"both"|"male"|"female": narrative},
                "disease_risks": narrative,
                "disease_deep_dive": {metric_id: narrative, ...},
                "oxidative_stress": narrative,
                "positive_highlights": narrative,
                "participation": narrative,
            },
            "positives": narrative,
            "leadership_cards": [...],
        }
    """
    data = dashboard_input_from_camp_report(report_json)
    profile = compose_company_profile(data)
    weights = _gender_weights_dict(data)
    chart_findings = _chart_findings_for_profile(data, weights)
    ledger = RecommendationLedger()

    concerns: Dict[str, Any] = {}

    for section_id in _SIMPLE_SECTIONS:
        chart = chart_findings.get(section_id)
        narrative = generate_insight(section_id, chart, profile, ledger=ledger)
        concerns[section_id] = narrative_to_dict(narrative)

    # Gender-specific lifestyle narratives (feature-parity with FE toggle).
    concerns["physical_activity"] = _lifestyle_by_gender(
        section_id="physical_activity",
        pair=data.physical_activity,
        profile=profile,
        weights=weights,
        finder=finding_from_physical_activity,
        ledger=ledger,
    )
    concerns["sleep"] = _lifestyle_by_gender(
        section_id="sleep",
        pair=data.sleep,
        profile=profile,
        weights=weights,
        finder=finding_from_sleep,
        ledger=ledger,
    )

    # Per-disease deep-dive narratives (feature-parity with DiseaseDeepDive tabs).
    concerns["disease_deep_dive"] = _disease_deep_dive_map(data, profile, ledger=ledger)

    leadership = generate_leadership_takeaways(profile)
    positive = generate_positive_insights(profile)

    return {
        "profile": {
            "overall_severity": profile.overall_severity,
            "overall_burden": profile.overall_burden,
            "dominant_cluster": profile.dominant_cluster,
            "lifestyle_priority": profile.lifestyle_priority,
            "profile_confidence": profile.profile_confidence,
            "coverage": list(profile.coverage),
            "chart_top_diseases": to_jsonable(profile.chart_top_diseases),
            "top_risks": to_jsonable(profile.top_risks),
            "strengths": to_jsonable(profile.strengths),
            "one_thing": to_jsonable(profile.one_thing),
            "emergent_priorities": to_jsonable(profile.emergent_priorities),
        },
        "concerns": concerns,
        "leadership_cards": leadership_to_dict(leadership),
        "positives": narrative_to_dict(positive),
    }


def generate_section_insights(report_json: dict, section_id: SectionId) -> dict:
    """Build intelligence for a single engine section (not the full dashboard).

    Profile composition still reads the stored camp report so scoring/context
    stay consistent; only the requested section's ``generate_insight`` path runs.
    """
    data = dashboard_input_from_camp_report(report_json)
    profile = compose_company_profile(data)
    weights = _gender_weights_dict(data)
    chart_findings = _chart_findings_for_profile(data, weights)

    concerns: Dict[str, Any] = {}
    extras: Dict[str, Any] = {}

    if section_id == "physical_activity":
        concerns["physical_activity"] = _lifestyle_by_gender(
            section_id="physical_activity",
            pair=data.physical_activity,
            profile=profile,
            weights=weights,
            finder=finding_from_physical_activity,
        )
    elif section_id == "sleep":
        concerns["sleep"] = _lifestyle_by_gender(
            section_id="sleep",
            pair=data.sleep,
            profile=profile,
            weights=weights,
            finder=finding_from_sleep,
        )
    elif section_id == "disease_deep_dive":
        concerns["disease_deep_dive"] = _disease_deep_dive_map(data, profile)
    elif section_id in ("disease_risks", "overall_risk", "oxidative_stress", "participation"):
        chart = chart_findings.get(section_id)
        concerns[section_id] = narrative_to_dict(generate_insight(section_id, chart, profile))
    elif section_id == "positive_highlights":
        chart = chart_findings.get("positive_highlights")
        narrative = generate_insight("positive_highlights", chart, profile)
        as_dict = narrative_to_dict(narrative)
        concerns["positive_highlights"] = as_dict
        extras["positives"] = as_dict
    else:
        chart = chart_findings.get(section_id)
        concerns[section_id] = narrative_to_dict(generate_insight(section_id, chart, profile))

    return {"concerns": concerns, **extras}


def _lifestyle_by_gender(
    *,
    section_id: SectionId,
    pair,
    profile,
    weights: Optional[dict],
    finder,
    ledger: RecommendationLedger | None = None,
) -> Dict[str, Any]:
    """Generate both/male/female narratives using the same engine as the FE toggle.

    Only the ``both`` view registers on the cross-section ledger so male/female
    toggles can share the section action without exhausting levers for sleep etc.
    """
    out: Dict[str, Any] = {}
    if pair is None:
        for view in _GENDER_VIEWS:
            active = ledger if view == "both" else None
            out[view] = narrative_to_dict(
                generate_insight(section_id, None, profile, ledger=active)
            )
        return out

    for view in _GENDER_VIEWS:
        finding: MetricFinding = finder(pair, view, weights)
        active = ledger if view == "both" else None
        out[view] = narrative_to_dict(
            generate_insight(section_id, finding, profile, ledger=active)
        )
    return out


def _disease_deep_dive_map(
    data, profile, *, ledger: RecommendationLedger | None = None
) -> Dict[str, Any]:
    """One deep-dive narrative per disease matrix row (same path as FE tabs)."""
    out: Dict[str, Any] = {}
    if not data.diseases:
        return out

    for disease in data.diseases:
        metric_id = disease_metric_from_name(disease.disease.name) or disease.disease.code
        finding = finding_from_disease(disease)
        narrative = generate_insight(
            "disease_deep_dive",
            finding,
            profile,
            {"active_metric_id": metric_id},
            ledger=ledger,
        )
        out[str(metric_id)] = narrative_to_dict(narrative)
    return out


def _chart_findings_for_profile(data, weights: Optional[dict]):
    """Attach chart-local findings so narrative facts match displayed chart values."""
    out = {}

    if data.overall_risk_score:
        out["overall_risk"] = finding_from_overall_risk(data.overall_risk_score)
    if data.top_high_risk_diseases:
        lead = finding_from_top_disease(data.top_high_risk_diseases[0])
        # Copy denominator flags from the matching disease matrix when present.
        # Does not change elevated_share, scoring, or tone.
        if data.diseases:
            for disease in data.diseases:
                metric_id = disease_metric_from_name(disease.disease.name) or disease.disease.code
                if metric_id != lead.metric_id:
                    continue
                matrix = finding_from_disease(disease)
                flags = {
                    key: (matrix.extras or {}).get(key)
                    for key in ("male_has_data", "female_has_data", "population_label", "denominator_label")
                    if (matrix.extras or {}).get(key) is not None
                }
                if flags:
                    lead.extras = {**(lead.extras or {}), **flags}
                break
        out["disease_risks"] = lead
    if data.oxidative_stress:
        out["oxidative_stress"] = finding_from_oxidative(
            data.oxidative_stress, data.oxidative_headcounts
        )
    if data.positive_wins:
        out["positive_highlights"] = finding_from_positive_wins(data.positive_wins)
    if data.participation_by_age:
        out["participation"] = finding_from_participation(data.participation_by_age)
    return out


def _gender_weights_dict(data) -> Optional[dict]:
    if not data.gender_weights:
        return None
    weights = {}
    if data.gender_weights.male is not None:
        weights["male"] = float(data.gender_weights.male)
    if data.gender_weights.female is not None:
        weights["female"] = float(data.gender_weights.female)
    return weights or None
