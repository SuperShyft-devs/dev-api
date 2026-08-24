"""Build the executive summary from patient + disease sections."""

from __future__ import annotations

from modules.bioai_report.report_engine import config
from modules.bioai_report.report_engine.models.report import (
    DiseaseHighlight,
    DiseaseSection,
    ExecutiveSummary,
    PatientInfo,
)
from modules.bioai_report.report_engine.utils.insight_dedupe import dedupe_insights


def _to_highlight(section: DiseaseSection) -> DiseaseHighlight:
    return DiseaseHighlight(
        disease_id=section.disease_id,
        title=section.title,
        score=section.score,
        risk=section.risk,
        band=section.band,
    )


def _sorted_by_score_desc(sections: list[DiseaseSection]) -> list[DiseaseSection]:
    return sorted(sections, key=lambda s: (-s.score, s.disease_id))


def _sorted_by_score_asc(sections: list[DiseaseSection]) -> list[DiseaseSection]:
    return sorted(sections, key=lambda s: (s.score, s.disease_id))


def collect_actionable_insights(
    sections: list[DiseaseSection],
    *,
    prefer_high_risk_first: bool = True,
    limit: int = config.TOP_ACTIONABLE_INSIGHTS,
) -> list[str]:
    """Gather actionable tips across diseases, highest-risk first, deduplicated."""
    ordered = _sorted_by_score_desc(sections) if prefer_high_risk_first else list(sections)
    candidates: list[str] = []
    for section in ordered:
        candidates.extend(section.lifestyle.tips)
        candidates.extend(section.nutrition.recommendations)
        candidates.extend(section.lifestyle.exercise)
    return dedupe_insights(candidates, limit=limit)


def build_executive_summary(
    *,
    patient: PatientInfo,
    disease_sections: list[DiseaseSection],
    top_risk_count: int = config.TOP_RISKS,
    top_strength_count: int = config.TOP_STRENGTHS,
    insight_limit: int = config.TOP_ACTIONABLE_INSIGHTS,
) -> ExecutiveSummary:
    """Assemble the executive summary placed before disease pages."""
    by_desc = _sorted_by_score_desc(disease_sections)
    by_asc = _sorted_by_score_asc(disease_sections)

    top_risk = [_to_highlight(s) for s in by_desc[: max(0, top_risk_count)]]
    top_risk_ids = {h.disease_id for h in top_risk}
    strengths: list[DiseaseHighlight] = []
    for section in by_asc:
        if section.disease_id in top_risk_ids:
            continue
        strengths.append(_to_highlight(section))
        if len(strengths) >= max(0, top_strength_count):
            break

    return ExecutiveSummary(
        patient=patient,
        assessment_date=patient.assessment_date,
        metabolic_score=patient.metabolic_score,
        metabolic_age=patient.metabolic_age,
        overall_status=patient.metabolic_health_status,
        top_disease_risks=top_risk,
        top_health_strengths=strengths,
        actionable_insights=collect_actionable_insights(
            disease_sections,
            limit=insight_limit,
        ),
    )
