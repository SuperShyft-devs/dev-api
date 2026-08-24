"""Build a single disease report section from assessment score + KB band.

For each disease score the builder:
1. Maps the assessment code to a knowledge-base disease id
2. Resolves the risk band and score slab
3. Selects only that slab's KB content (never the full knowledge base)
4. Caps recommendation arrays via ``config`` limits
"""

from __future__ import annotations

import logging

from modules.bioai_report.report_engine import config
from modules.bioai_report.report_engine.exceptions import KnowledgeBaseError
from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.models.assessment import AssessmentDisease
from modules.bioai_report.report_engine.models.knowledge_base import DiseaseKnowledgeBase
from modules.bioai_report.report_engine.models.report import (
    DiseaseCurrentStatus,
    DiseaseLifestyle,
    DiseaseMonitoring,
    DiseaseNutrition,
    DiseaseResultFactors,
    DiseaseSection,
)
from modules.bioai_report.report_engine.utils.content_limits import limit_items
from modules.bioai_report.report_engine.utils.disease_codes import normalize_disease_code
from modules.bioai_report.report_engine.utils.result_factors import (
    RESULT_FACTOR_TITLE,
    score_to_result_factor_set_key,
)
from modules.bioai_report.report_engine.utils.score_bands import (
    clamp_score,
    risk_display_label,
    score_to_risk_band,
    score_to_slab,
    slab_key,
    slab_label,
)

logger = logging.getLogger(__name__)


def resolve_disease_id(disease: AssessmentDisease) -> str | None:
    """Return the KB disease_id for an assessment disease entry."""
    return normalize_disease_code(disease.code)


def _resolve_risk_label(*, score: int, assessment_risk: str | None) -> str:
    """Prefer assessment risk_status when present; otherwise derive from score band."""
    if assessment_risk and assessment_risk.strip():
        return assessment_risk.strip()
    return risk_display_label(score_to_risk_band(score))


def build_disease_section(
    disease: AssessmentDisease,
    *,
    kb_store: KnowledgeBaseStore,
    kb: DiseaseKnowledgeBase | None = None,
) -> DiseaseSection | None:
    """Assemble one render-ready disease section, or ``None`` to soft-skip.

    Maps KB score-band fields into nested PDF blocks. Array lengths are capped
    by ``config`` so the frontend never slices content. Missing KB files or
    score slabs are logged and skipped so one gap never aborts the full report.
    """
    disease_id = resolve_disease_id(disease)
    if disease_id is None:
        return None
    if disease.risk_score_scaled is None:
        return None

    if kb is None:
        if not kb_store.has_disease(disease_id):
            return None
        try:
            kb = kb_store.get(disease_id)
        except KnowledgeBaseError as exc:
            logger.warning(
                "Skipping disease '%s': knowledge base unavailable (%s)",
                disease_id,
                exc,
            )
            return None

    score = clamp_score(disease.risk_score_scaled)
    lo, hi = score_to_slab(score)
    key = slab_key(lo, hi)
    band = slab_label(lo, hi)
    score_band = kb.get_score_band(key)
    if score_band is None:
        logger.warning(
            "Skipping disease '%s': score band '%s' (key '%s') not found in knowledge base",
            disease_id,
            band,
            key,
        )
        return None

    title = (disease.name or "").strip() or kb.display_name
    risk = _resolve_risk_label(score=score, assessment_risk=disease.risk_status)

    factor_set_key = score_to_result_factor_set_key(score)
    factor_set = kb.result_factor_sets.get(factor_set_key)
    result_factors = DiseaseResultFactors(
        title=RESULT_FACTOR_TITLE,
        factors=list(factor_set.factors) if factor_set is not None else [],
    )

    return DiseaseSection(
        disease_id=disease_id,
        title=title,
        overview=kb.overview,
        current_status=DiseaseCurrentStatus(
            score=score,
            risk=risk,
            band=band,
            percentile=disease.disease_percentile,
            healthy_percentile=disease.healthy_percentile,
            lifestyle_contribution=disease.lifestyle_contribution,
            interpretation=score_band.risk_interpretation,
            clinical_warning=score_band.clinical_warning,
        ),
        lifestyle=DiseaseLifestyle(
            tips=limit_items(score_band.lifestyle_tips, config.TOP_LIFESTYLE_TIPS),
            exercise=limit_items(
                score_band.exercise_recommendations, config.TOP_EXERCISE
            ),
        ),
        nutrition=DiseaseNutrition(
            recommendations=limit_items(
                score_band.diet_recommendations, config.TOP_DIET_TIPS
            ),
            foods_to_include=limit_items(score_band.foods_to_include, config.TOP_FOODS),
            foods_to_avoid=limit_items(score_band.foods_to_avoid, config.TOP_FOODS),
        ),
        monitoring=DiseaseMonitoring(
            recommendations=limit_items(
                score_band.monitoring_recommendations, config.TOP_MONITORING
            ),
        ),
        positive_takeaway=score_band.positive_takeaway,
        result_factors=result_factors,
    )


def build_disease_sections(
    diseases: list[AssessmentDisease],
    *,
    kb_store: KnowledgeBaseStore,
) -> list[DiseaseSection]:
    """Build disease sections sorted highest → lowest risk score (display order)."""
    sections: list[DiseaseSection] = []
    seen: set[str] = set()
    for disease in diseases:
        disease_id = resolve_disease_id(disease)
        if disease_id is None or disease_id in seen:
            continue
        if not kb_store.has_disease(disease_id):
            continue
        section = build_disease_section(disease, kb_store=kb_store)
        if section is None:
            continue
        seen.add(disease_id)
        sections.append(section)

    sections.sort(key=lambda s: (-s.score, s.disease_id))
    return sections
