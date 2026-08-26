"""Reasoning layer — decides WHAT should be communicated for a section.

Flat merge (no business-logic changes) of:
  - intelligence/reasoning/confidence.py
  - intelligence/reasoning/section_strategies/helpers.py
  - intelligence/reasoning/section_strategies/overall_risk.py
  - intelligence/reasoning/section_strategies/lifestyle.py
  - intelligence/reasoning/section_strategies/disease.py
  - intelligence/reasoning/section_strategies/misc.py
  - intelligence/reasoning/section_strategies/leadership.py
  - intelligence/reasoning/reason_about_section.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .knowledge import (
    CONFIDENCE_BANDS,
    frame_id_for_metric,
    get_default_levers,
    get_disease_knowledge,
    get_lifestyle_knowledge,
    is_disease_metric,
    severity_from_prevalence,
    tone_from_severity,
)
from .models import (
    CategoryShare,
    CompanyHealthProfile,
    ConfidenceFactors,
    InsightConfidence,
    InsightExplanation,
    InsightObservation,
    InsightPlan,
    InsightPlanLeadership,
    InsightRecommendation,
    InterventionId,
    MetricFinding,
    MetricId,
    SectionId,
)

# ---------------------------------------------------------------------------
# confidence.py
# ---------------------------------------------------------------------------


def compute_insight_confidence(
    *,
    profile: CompanyHealthProfile,
    metric_id: MetricId,
    section_id: SectionId,
    effect_ids: list[str],
) -> InsightConfidence:
    score = profile.scores.get(metric_id)
    notes: list[str] = []

    data_quality = score.data_quality if score else profile.profile_confidence
    if data_quality < 0.6:
        notes.append("limited_sample")

    burden = score.burden_score if score else 0
    strength = score.strength_score if score else 0
    # Mid-range ambiguous signals are weaker
    decisiveness = max(abs(burden - 40), abs(strength - 50)) / 50
    signal_strength = max(0.3, min(1, decisiveness))
    if signal_strength < 0.5:
        notes.append("ambiguous_signal")

    relevant_activations = [
        a
        for a in profile.graph.activations
        if a.from_ == metric_id or a.to == metric_id or a.effect_id in effect_ids
    ]
    graph_support = (
        0.45
        if not relevant_activations
        else min(1, 0.55 + relevant_activations[0].activation)
    )

    has_knowledge = bool(get_disease_knowledge(metric_id) or get_lifestyle_knowledge(metric_id))
    knowledge_coverage = 1 if has_knowledge else 0.4
    if not has_knowledge:
        notes.append("limited_knowledge")

    if len(profile.coverage) < 3:
        notes.append("partial_profile")

    overall = (
        0.35 * data_quality + 0.3 * signal_strength + 0.2 * graph_support + 0.15 * knowledge_coverage
    )

    band = (
        "high"
        if overall >= CONFIDENCE_BANDS["high"]
        else "moderate"
        if overall >= CONFIDENCE_BANDS["moderate"]
        else "low"
    )

    return InsightConfidence(
        score=round(overall, 2),
        band=band,
        factors=ConfidenceFactors(
            data_quality=round(data_quality, 2),
            signal_strength=round(signal_strength, 2),
            graph_support=round(graph_support, 2),
            knowledge_coverage=knowledge_coverage,
        ),
        notes=notes,
    )


def empty_confidence(profile: CompanyHealthProfile) -> InsightConfidence:
    band = (
        "high"
        if profile.profile_confidence >= CONFIDENCE_BANDS["high"]
        else "moderate"
        if profile.profile_confidence >= CONFIDENCE_BANDS["moderate"]
        else "low"
    )
    return InsightConfidence(
        score=profile.profile_confidence,
        band=band,
        factors=ConfidenceFactors(
            data_quality=profile.profile_confidence,
            signal_strength=0.5,
            graph_support=0.45,
            knowledge_coverage=0.8,
        ),
        notes=["profile_level"],
    )


def with_confidence(plan_fields: dict[str, Any], profile: CompanyHealthProfile) -> InsightPlan:
    """Attach confidence to a partially built plan.

    ``plan_fields`` mirrors the TS `Omit<InsightPlan, 'confidence'>` object — a
    dict of the InsightPlan constructor kwargs, everything except `confidence`.
    """
    confidence = compute_insight_confidence(
        profile=profile,
        metric_id=plan_fields["observation"].metric_id,
        section_id=plan_fields["section_id"],
        effect_ids=plan_fields["explanation"].effect_ids,
    )
    return InsightPlan(confidence=confidence, **plan_fields)


# ---------------------------------------------------------------------------
# section_strategies/helpers.py
# ---------------------------------------------------------------------------


def effects_for_metric(profile: CompanyHealthProfile, metric_id: MetricId) -> list[str]:
    return [
        a.effect_id
        for a in profile.graph.activations
        if a.from_ == metric_id or a.to == metric_id
    ]


# Lifestyle-priority → organisational lever (mirrors leadership lifestyle card).
_LIFESTYLE_PRIORITY_LEVER: dict[str, InterventionId] = {
    "physical": "movement_programme",
    "sleep": "sleep_health",
    "nutrition": "nutrition_heart_healthy",
    "recovery": "recovery_programme",
}

# Cluster → complementary levers when disease sections need a non-repeating action.
_CLUSTER_SUPPORT_LEVERS: dict[str, list[InterventionId]] = {
    "cardiovascular": ["lipid_screening", "bp_screening", "nutrition_heart_healthy", "stress_management"],
    "metabolic": ["metabolic_screening", "nutrition_refined_carb", "movement_programme", "weight_management"],
    "hormonal": ["thyroid_screening", "stress_management", "womens_health", "sleep_health"],
    "lifestyle": ["movement_programme", "sleep_health", "nutrition_heart_healthy", "recovery_programme"],
    "mixed": ["scale_preventive_care", "target_high_risk", "movement_programme"],
    "healthy": ["maintain_wellness", "scale_preventive_care"],
}


def _lifestyle_aligns(
    metric_id: MetricId,
    section_id: SectionId | None,
    lifestyle_priority: str,
) -> bool:
    if lifestyle_priority == "physical":
        return metric_id in ("physical_activity", "obesity", "bmi_waist", "overall_risk") or section_id in (
            "physical_activity",
            "overall_risk",
        )
    if lifestyle_priority == "sleep":
        return metric_id in ("sleep", "oxidative_stress", "overall_risk") or section_id in (
            "sleep",
            "oxidative_stress",
            "overall_risk",
        )
    if lifestyle_priority == "nutrition":
        return metric_id in (
            "nutrition",
            "dyslipidemia",
            "type_2_diabetes",
            "obesity",
            "nafld",
            "overall_risk",
        ) or section_id in ("nutrition", "disease_risks", "disease_deep_dive", "overall_risk")
    if lifestyle_priority == "recovery":
        return metric_id in ("sleep", "oxidative_stress", "overall_risk") or section_id in (
            "sleep",
            "oxidative_stress",
            "overall_risk",
        )
    return False


def _action_role_for(
    profile: CompanyHealthProfile,
    lever_ids: list[InterventionId],
    *,
    section_id: SectionId,
    metric_id: MetricId,
    healthy: bool,
) -> str:
    if healthy or not lever_ids or lever_ids[0] == "maintain_wellness":
        return "maintain"
    one = profile.one_thing
    if one and lever_ids[0] == one.lever:
        if section_id == "overall_risk" or metric_id in one.target_metrics:
            return "org_primary"
    return "section_support"


def make_recommendation(
    profile: CompanyHealthProfile,
    lever_ids: list[InterventionId],
    *,
    section_id: SectionId,
    metric_id: MetricId,
    healthy: bool,
) -> InsightRecommendation:
    return InsightRecommendation(
        lever_ids=lever_ids,
        one_thing=profile.one_thing.lever if profile.one_thing else None,
        action_role=_action_role_for(
            profile, lever_ids, section_id=section_id, metric_id=metric_id, healthy=healthy
        ),
        lifestyle_priority=profile.lifestyle_priority,
    )


def levers_for_metric(
    profile: CompanyHealthProfile,
    metric_id: MetricId,
    healthy: bool,
    *,
    section_id: SectionId | None = None,
    used_levers: list[InterventionId] | None = None,
) -> list[InterventionId]:
    """Select organisational action levers for a section.

    Priority order (deterministic):
      1. Company one_thing (when this section should carry the org answer)
      2. Emergent graph priorities that include this metric
      3. Lifestyle-priority lever when aligned with the section/metric
      4. Cluster-support levers for disease context
      5. Metric-level default high (then medium) interventions

    Already-used levers are deferred so related dashboard sections vary.
    """
    if healthy:
        return ["maintain_wellness"]

    used_set = set(used_levers or [])
    candidates: list[InterventionId] = []

    one = profile.one_thing
    if section_id == "overall_risk":
        return ["target_high_risk", "scale_preventive_care"]

    if one and one.lever:
        carries_primary = metric_id in one.target_metrics
        if carries_primary:
            candidates.append(one.lever)

    for priority in profile.emergent_priorities:
        if metric_id in priority.members and priority.lever:
            candidates.append(priority.lever)

    lifestyle_lever = _LIFESTYLE_PRIORITY_LEVER.get(profile.lifestyle_priority or "")
    if lifestyle_lever and _lifestyle_aligns(metric_id, section_id, profile.lifestyle_priority):
        candidates.append(lifestyle_lever)

    if is_disease_metric(metric_id) or section_id in ("disease_risks", "disease_deep_dive"):
        candidates.extend(_CLUSTER_SUPPORT_LEVERS.get(profile.dominant_cluster or "mixed", []))

    defaults = get_default_levers(metric_id)
    candidates.extend(defaults["high"])
    candidates.extend(defaults["medium"])

    ranked = _unique(candidates)
    preferred = [lever for lever in ranked if lever not in used_set]
    pool = preferred if preferred else ranked
    return pool[:2]


def plan_for_metric_section(
    *,
    section_id: SectionId,
    metric_id: MetricId,
    profile: CompanyHealthProfile,
    kind: str = "distribution",
    extra_facts: dict[str, Any] | None = None,
    chart_finding: MetricFinding | None = None,
    used_levers: list[InterventionId] | None = None,
) -> InsightPlan:
    """Build an InsightPlan for a single-metric section.

    ``chart_finding``, when provided, is the single source of truth for
    observation facts — must be the same analyzed dataset the chart rendered,
    never recomputed.
    """
    finding = chart_finding if chart_finding is not None else profile.findings.get(metric_id)
    resolved_metric_id = chart_finding.metric_id if chart_finding is not None else metric_id
    score = profile.scores.get(resolved_metric_id)
    extras = (finding.extras or {}) if finding is not None else {}
    display_name = extras.get("displayName") or human_metric_name_lazy(resolved_metric_id)

    # Prefer explicit chart percent (e.g. highRiskPercent) when present
    if extras.get("highRiskPercent") is not None:
        chart_percent = float(extras["highRiskPercent"])
    else:
        chart_percent = float(finding.elevated_share) if finding is not None else 0.0

    if chart_finding is not None:
        severity_band = severity_from_prevalence(chart_percent)
    else:
        severity_band = score.severity_band if score is not None else profile.overall_severity

    healthy = chart_percent < 30
    mode = "positive" if healthy else "concern"
    effect_ids = effects_for_metric(profile, resolved_metric_id)

    facts: dict[str, Any] = {
        "display_name": display_name,
        "elevated_share": chart_percent,
        "healthy_share": (
            finding.healthy_share if finding is not None and finding.healthy_share is not None
            else max(0.0, 100 - chart_percent)
        ),
        "high_severity_share": finding.high_severity_share if finding is not None else 0,
        "dominant_label": (finding.dominant.label if finding is not None and finding.dominant else ""),
        "dominant_percent": (finding.dominant.percent if finding is not None and finding.dominant else 0),
        "opportunity_label": (finding.opportunity.label if finding is not None and finding.opportunity else ""),
        "opportunity_percent": (finding.opportunity.percent if finding is not None and finding.opportunity else 0),
        "burden_score": score.burden_score if score is not None else 0,
    }
    # Pass through existing analyzer extras that describe the denominator or
    # plotted mode. Does not change scoring, tone, or elevated_share.
    for key in (
        "male_has_data",
        "female_has_data",
        "population_label",
        "denominator_label",
        "graph_view",
    ):
        if key in extras and key not in facts:
            facts[key] = extras[key]
    if extra_facts:
        facts.update(extra_facts)

    plan_fields: dict[str, Any] = {
        "section_id": section_id,
        "mode": mode,
        "tone": tone_from_severity(severity_band, mode),
        "severity_band": severity_band,
        "observation": InsightObservation(
            kind=kind,
            metric_id=resolved_metric_id,
            facts=facts,
        ),
        "explanation": InsightExplanation(
            medical_frame_id=frame_id_for_metric(resolved_metric_id, healthy),
            effect_ids=effect_ids,
            cluster=profile.dominant_cluster,
        ),
        "recommendation": make_recommendation(
            profile,
            levers_for_metric(
                profile,
                resolved_metric_id,
                healthy,
                section_id=section_id,
                used_levers=used_levers,
            ),
            section_id=section_id,
            metric_id=resolved_metric_id,
            healthy=healthy,
        ),
    }

    return with_confidence(plan_fields, profile)


def human_metric_name_lazy(metric_id: MetricId) -> str:
    """Local import to avoid a reasoning -> generator -> reasoning import cycle."""
    from .generator import human_metric_name

    return human_metric_name(metric_id)


def _unique(items: list) -> list:
    seen: set = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


# ---------------------------------------------------------------------------
# section_strategies/overall_risk.py
# ---------------------------------------------------------------------------


def plan_overall_risk(profile: CompanyHealthProfile) -> InsightPlan:
    return plan_for_metric_section(
        section_id="overall_risk",
        metric_id="overall_risk",
        profile=profile,
        kind="distribution",
    )


# ---------------------------------------------------------------------------
# section_strategies/lifestyle.py
# ---------------------------------------------------------------------------


def plan_physical_activity(profile: CompanyHealthProfile) -> InsightPlan:
    return plan_for_metric_section(
        section_id="physical_activity",
        metric_id="physical_activity",
        profile=profile,
        kind="lifestyle",
    )


def plan_sleep(profile: CompanyHealthProfile) -> InsightPlan:
    return plan_for_metric_section(
        section_id="sleep",
        metric_id="sleep",
        profile=profile,
        kind="lifestyle",
    )


def plan_nutrition(profile: CompanyHealthProfile) -> InsightPlan:
    return plan_for_metric_section(
        section_id="nutrition",
        metric_id="nutrition",
        profile=profile,
        kind="lifestyle",
    )


def plan_oxidative(profile: CompanyHealthProfile) -> InsightPlan:
    return plan_for_metric_section(
        section_id="oxidative_stress",
        metric_id="oxidative_stress",
        profile=profile,
        kind="lifestyle",
    )


# ---------------------------------------------------------------------------
# section_strategies/disease.py
# ---------------------------------------------------------------------------


def plan_disease_risks(
    profile: CompanyHealthProfile,
    chart_finding: MetricFinding | None = None,
    used_levers: list[InterventionId] | None = None,
) -> InsightPlan:
    if chart_finding is not None:
        return plan_for_metric_section(
            section_id="disease_risks",
            metric_id=chart_finding.metric_id,
            profile=profile,
            kind="disease_lead",
            chart_finding=chart_finding,
            used_levers=used_levers,
        )

    lead = profile.chart_top_diseases[0] if profile.chart_top_diseases else None
    if lead is not None:
        finding = profile.findings.get(lead.metric_id)
        if finding is None:
            elevated_category = CategoryShare(
                id="elevated", label="Elevated", percent=lead.high_risk_percent, polarity="elevated"
            )
            chart_finding_from_list = MetricFinding(
                metric_id=lead.metric_id,
                categories=[elevated_category],
                dominant=elevated_category,
                healthy_share=max(0.0, 100 - lead.high_risk_percent),
                elevated_share=lead.high_risk_percent,
                high_severity_share=lead.high_risk_percent,
                opportunity=None,
                extras={"displayName": lead.name, "highRiskPercent": lead.high_risk_percent},
            )
        else:
            chart_finding_from_list = finding

        # Force chart percent even if a stale finding exists
        chart_finding_from_list.elevated_share = lead.high_risk_percent
        chart_finding_from_list.extras = {
            **(chart_finding_from_list.extras or {}),
            "displayName": lead.name,
            "highRiskPercent": lead.high_risk_percent,
        }

        return plan_for_metric_section(
            section_id="disease_risks",
            metric_id=lead.metric_id,
            profile=profile,
            kind="disease_lead",
            chart_finding=chart_finding_from_list,
            used_levers=used_levers,
        )

    # Last resort only when chart list is absent
    fallback: MetricId | None = next(
        (r.metric_id for r in profile.top_risks if is_disease_metric(r.metric_id)),
        profile.top_risks[0].metric_id if profile.top_risks else None,
    )

    if not fallback:
        return plan_for_metric_section(
            section_id="disease_risks",
            metric_id="overall_risk",
            profile=profile,
            kind="disease_lead",
            used_levers=used_levers,
        )

    return plan_for_metric_section(
        section_id="disease_risks",
        metric_id=fallback,
        profile=profile,
        kind="disease_lead",
        used_levers=used_levers,
    )


def plan_disease_deep_dive(
    profile: CompanyHealthProfile,
    active_metric_id: MetricId | None = None,
    chart_finding: MetricFinding | None = None,
    used_levers: list[InterventionId] | None = None,
) -> InsightPlan:
    if chart_finding is not None:
        gender_gap = chart_finding.gender_gap
        return plan_for_metric_section(
            section_id="disease_deep_dive",
            metric_id=chart_finding.metric_id,
            profile=profile,
            kind="distribution",
            chart_finding=chart_finding,
            used_levers=used_levers,
            extra_facts={
                "male_elevated": gender_gap.male_elevated if gender_gap else 0,
                "female_elevated": gender_gap.female_elevated if gender_gap else 0,
                "gender_delta": gender_gap.delta if gender_gap else 0,
            },
        )

    metric_id = (
        active_metric_id
        or (profile.chart_top_diseases[0].metric_id if profile.chart_top_diseases else None)
        or next((r.metric_id for r in profile.top_risks if is_disease_metric(r.metric_id)), None)
        or "type_2_diabetes"
    )

    finding = profile.findings.get(metric_id)
    gender_gap = finding.gender_gap if finding is not None else None
    return plan_for_metric_section(
        section_id="disease_deep_dive",
        metric_id=metric_id,
        profile=profile,
        kind="distribution",
        chart_finding=finding,
        used_levers=used_levers,
        extra_facts={
            "male_elevated": gender_gap.male_elevated if gender_gap else 0,
            "female_elevated": gender_gap.female_elevated if gender_gap else 0,
            "gender_delta": gender_gap.delta if gender_gap else 0,
        },
    )


# ---------------------------------------------------------------------------
# section_strategies/misc.py
# ---------------------------------------------------------------------------


def plan_positive_highlights(profile: CompanyHealthProfile) -> InsightPlan:
    finding = profile.findings.get("positive_wins")
    extras = (finding.extras or {}) if finding is not None else {}
    low_risk_count = int(extras.get("lowRiskCount", 0) or 0)
    profiles_count = int(extras.get("profilesCount", 0) or 0)
    habits_count = int(extras.get("habitsCount", 0) or 0)

    effect_ids = effects_for_metric(profile, "positive_wins")
    if any(p.effect_id == "workforce_resilience" for p in profile.emergent_priorities):
        effect_ids = [*effect_ids, "workforce_resilience"]

    plan_fields = {
        "section_id": "positive_highlights",
        "mode": "positive",
        "tone": "positive",
        "severity_band": "very_low",
        "observation": InsightObservation(
            kind="strength",
            metric_id="positive_wins",
            facts={
                "display_name": "Positive wins",
                "low_risk_count": low_risk_count,
                "profiles_count": profiles_count,
                "habits_count": habits_count,
                "elevated_share": 0,
                "healthy_share": finding.healthy_share if finding is not None else 0,
                "low_risk_names": str(extras.get("low_risk_names") or ""),
                "habit_labels": str(extras.get("habit_labels") or ""),
                "profile_names": str(extras.get("profile_names") or ""),
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id="positive_wins",
            effect_ids=effect_ids,
            cluster=profile.dominant_cluster,
        ),
        "recommendation": make_recommendation(
            profile,
            ["maintain_wellness"],
            section_id="positive_highlights",
            metric_id="positive_wins",
            healthy=True,
        ),
    }
    return with_confidence(plan_fields, profile)


def plan_participation(profile: CompanyHealthProfile) -> InsightPlan:
    finding = profile.findings.get("participation")
    extras = (finding.extras or {}) if finding is not None else {}

    plan_fields = {
        "section_id": "participation",
        "mode": "mixed",
        "tone": "neutral",
        "severity_band": "low",
        "observation": InsightObservation(
            kind="status",
            metric_id="participation",
            facts={
                "display_name": "Participation",
                "top_age_group": str(extras.get("topAgeGroup", "") or ""),
                "top_percent": float(extras.get("topPercent", 0) or 0),
                "top_enrolled": float(extras.get("topEnrolled", 0) or 0),
                "elevated_share": 0,
                "healthy_share": 100,
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id="participation",
            effect_ids=[],
        ),
        "recommendation": make_recommendation(
            profile,
            ["maintain_wellness"],
            section_id="participation",
            metric_id="participation",
            healthy=True,
        ),
    }
    return with_confidence(plan_fields, profile)


def plan_metabolic_age(
    profile: CompanyHealthProfile,
    used_levers: list[InterventionId] | None = None,
) -> InsightPlan:
    finding = profile.findings.get("metabolic_age")
    score = profile.scores.get("metabolic_age")
    extras = (finding.extras or {}) if finding is not None else {}
    severity_band = score.severity_band if score is not None else "moderate"
    healthy = (score.burden_score if score is not None else 50) < 30
    levers = (
        ["maintain_wellness"]
        if healthy
        else levers_for_metric(
            profile,
            "metabolic_age",
            False,
            section_id="metabolic_age",
            used_levers=used_levers,
        )
        or ["movement_programme", "nutrition_whole_food"]
    )

    plan_fields = {
        "section_id": "metabolic_age",
        "mode": "positive" if healthy else "concern",
        "tone": tone_from_severity(severity_band, "positive" if healthy else "concern"),
        "severity_band": severity_band,
        "observation": InsightObservation(
            kind="distribution",
            metric_id="metabolic_age",
            facts={
                "display_name": "Metabolic age",
                "elevated_share": (
                    finding.elevated_share
                    if finding is not None and finding.elevated_share is not None
                    else float(extras.get("highRiskPercent", 0) or 0)
                ),
                "healthy_share": finding.healthy_share if finding is not None else 0,
                "avg_gap_years": float(extras.get("avgGapYears", 0) or 0),
                "high_risk_percent": float(extras.get("highRiskPercent", 0) or 0),
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id="metabolic_age",
            effect_ids=[],
            cluster=profile.dominant_cluster,
        ),
        "recommendation": make_recommendation(
            profile,
            levers,
            section_id="metabolic_age",
            metric_id="metabolic_age",
            healthy=healthy,
        ),
    }
    return with_confidence(plan_fields, profile)


def plan_bmi_waist(
    profile: CompanyHealthProfile,
    used_levers: list[InterventionId] | None = None,
) -> InsightPlan:
    finding = profile.findings.get("bmi_waist")
    score = profile.scores.get("bmi_waist")
    extras = (finding.extras or {}) if finding is not None else {}
    burden_score = score.burden_score if score is not None else 0
    healthy = burden_score < 30
    severity_band = score.severity_band if score is not None else "moderate"
    levers = (
        ["maintain_wellness"]
        if healthy
        else levers_for_metric(
            profile,
            "bmi_waist",
            False,
            section_id="bmi_waist",
            used_levers=used_levers,
        )
        or ["weight_management", "movement_programme"]
    )

    plan_fields = {
        "section_id": "bmi_waist",
        "mode": "positive" if healthy else "concern",
        "tone": tone_from_severity(severity_band, "positive" if healthy else "concern"),
        "severity_band": severity_band,
        "observation": InsightObservation(
            kind="distribution",
            metric_id="bmi_waist",
            facts={
                "display_name": "BMI & waist",
                "elevated_share": finding.elevated_share if finding is not None else 0,
                "healthy_share": finding.healthy_share if finding is not None else 0,
                "above_ideal_waist_percent": float(extras.get("aboveIdealWaistPercent", 0) or 0),
                "avg_waist_inches": float(extras.get("avgWaistInches", 0) or 0),
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id="bmi_waist",
            effect_ids=[
                a.effect_id
                for a in profile.graph.activations
                if a.from_ == "bmi_waist" or a.to == "obesity"
            ],
            cluster=profile.dominant_cluster,
        ),
        "recommendation": make_recommendation(
            profile,
            levers,
            section_id="bmi_waist",
            metric_id="bmi_waist",
            healthy=healthy,
        ),
    }
    return with_confidence(plan_fields, profile)


# ---------------------------------------------------------------------------
# section_strategies/leadership.py
# ---------------------------------------------------------------------------


def plan_leadership_cards(profile: CompanyHealthProfile) -> list[InsightPlan]:
    return [
        _workforce_card(profile),
        _lifestyle_card(profile),
        _disease_focus_card(profile),
        _strategy_card(profile),
    ]


def _workforce_card(profile: CompanyHealthProfile) -> InsightPlan:
    finding = profile.findings.get("overall_risk")
    elevated = finding.elevated_share if finding is not None else profile.overall_burden
    healthy = profile.overall_severity in ("very_low", "low")

    plan_fields = {
        "section_id": "leadership",
        "mode": "leadership",
        "tone": tone_from_severity(profile.overall_severity, "positive" if healthy else "concern"),
        "severity_band": profile.overall_severity,
        "observation": InsightObservation(
            kind="cluster",
            metric_id="overall_risk",
            facts={
                "display_name": "Workforce health",
                "elevated_share": elevated,
                "healthy_share": finding.healthy_share if finding is not None else 0,
                "overall_burden": profile.overall_burden,
                "dominant_cluster": profile.dominant_cluster,
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id="overall_risk_healthy" if healthy else "overall_risk",
            effect_ids=[p.effect_id for p in profile.emergent_priorities[:2]],
            cluster=profile.dominant_cluster,
        ),
        "recommendation": InsightRecommendation(
            lever_ids=(
                ["maintain_wellness"] if healthy else ["target_high_risk", "scale_preventive_care"]
            ),
            one_thing=profile.one_thing.lever if profile.one_thing else None,
        ),
        "leadership": InsightPlanLeadership(
            card_id="workforce-health",
            title="Workforce Health Status",
            headline_key=(
                "healthy"
                if healthy
                else "emerging"
                if profile.overall_severity == "moderate"
                else "growing"
                if profile.overall_severity == "high"
                else "critical"
            ),
        ),
    }
    return with_confidence(plan_fields, profile)


def _lifestyle_card(profile: CompanyHealthProfile) -> InsightPlan:
    priority = profile.lifestyle_priority
    metric_id = (
        "sleep"
        if priority in ("sleep", "recovery")
        else "nutrition"
        if priority == "nutrition"
        else "physical_activity"
    )

    frame_id = (
        "recovery_strain"
        if priority == "recovery"
        else "workforce_resilience"
        if priority == "strong"
        else metric_id
    )

    finding = profile.findings.get(metric_id)
    tracked_effects = {"recovery_strain", "movement_priority", "cardio_nutrition", "workforce_resilience"}

    plan_fields = {
        "section_id": "leadership",
        "mode": "leadership",
        "tone": "positive" if priority == "strong" else "concern",
        "severity_band": (
            profile.scores[metric_id].severity_band
            if metric_id in profile.scores
            else profile.overall_severity
        ),
        "observation": InsightObservation(
            kind="lifestyle",
            metric_id=metric_id,
            facts={
                "display_name": "Lifestyle priority",
                "lifestyle_priority": priority,
                "elevated_share": finding.elevated_share if finding is not None else 0,
                "healthy_share": finding.healthy_share if finding is not None else 0,
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id=frame_id,
            effect_ids=[
                p.effect_id for p in profile.emergent_priorities if p.effect_id in tracked_effects
            ],
            cluster=profile.dominant_cluster,
        ),
        "recommendation": InsightRecommendation(
            lever_ids=(
                ["maintain_wellness"]
                if priority == "strong"
                else ["recovery_programme", "sleep_health"]
                if priority == "recovery"
                else ["nutrition_heart_healthy"]
                if priority == "nutrition"
                else ["sleep_health"]
                if priority == "sleep"
                else ["movement_programme"]
            ),
            one_thing=profile.one_thing.lever if profile.one_thing else None,
        ),
        "leadership": InsightPlanLeadership(
            card_id="lifestyle-priority",
            title="Lifestyle Priority",
            headline_key=priority,
        ),
    }
    return with_confidence(plan_fields, profile)


def _disease_focus_card(profile: CompanyHealthProfile) -> InsightPlan:
    cluster = profile.dominant_cluster
    top_disease = profile.top_risks[0].metric_id if profile.top_risks else "overall_risk"
    frame_id = (
        "cardio_cluster"
        if cluster == "cardiovascular"
        else "thyroid_health"
        if cluster == "hormonal"
        else "maintain"
        if cluster == "healthy"
        else "metabolic_cluster"
    )

    finding = profile.findings.get(top_disease)

    plan_fields = {
        "section_id": "leadership",
        "mode": "leadership",
        "tone": "positive" if cluster == "healthy" else "concern",
        "severity_band": profile.overall_severity,
        "observation": InsightObservation(
            kind="cluster",
            metric_id=top_disease,
            facts={
                "display_name": "Primary disease focus",
                "dominant_cluster": cluster,
                "elevated_share": finding.elevated_share if finding is not None else 0,
                "healthy_share": finding.healthy_share if finding is not None else 0,
                "top_disease": top_disease,
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id=frame_id,
            effect_ids=[p.effect_id for p in profile.emergent_priorities[:2]],
            cluster=cluster,
        ),
        "recommendation": InsightRecommendation(
            lever_ids=(
                ["cardiac_screening", "nutrition_heart_healthy"]
                if cluster == "cardiovascular"
                else ["thyroid_screening", "womens_health"]
                if cluster == "hormonal"
                else ["maintain_wellness"]
                if cluster == "healthy"
                else ["metabolic_screening", "nutrition_refined_carb", "movement_programme"]
            ),
            one_thing=profile.one_thing.lever if profile.one_thing else None,
        ),
        "leadership": InsightPlanLeadership(
            card_id="disease-focus",
            title="Primary Disease Focus",
            headline_key=cluster,
        ),
    }
    return with_confidence(plan_fields, profile)


def _strategy_card(profile: CompanyHealthProfile) -> InsightPlan:
    severity = profile.overall_severity
    lever: InterventionId = (
        "maintain_wellness"
        if severity in ("very_low", "low")
        else "target_high_risk"
        if severity == "moderate"
        else "scale_preventive_care"
    )

    levers: list[InterventionId] = [lever]
    if profile.one_thing:
        levers.append(profile.one_thing.lever)

    finding = profile.findings.get("overall_risk")

    plan_fields = {
        "section_id": "leadership",
        "mode": "leadership",
        "tone": "positive" if severity in ("very_low", "low") else "concern",
        "severity_band": severity,
        "observation": InsightObservation(
            kind="status",
            metric_id="overall_risk",
            facts={
                "display_name": "Strategic next step",
                "elevated_share": finding.elevated_share if finding is not None else profile.overall_burden,
                "healthy_share": finding.healthy_share if finding is not None else 0,
                "overall_burden": profile.overall_burden,
            },
        ),
        "explanation": InsightExplanation(
            medical_frame_id="maintain" if severity in ("very_low", "low") else "overall_risk",
            effect_ids=(
                [profile.one_thing.effect_id]
                if profile.one_thing and profile.one_thing.effect_id
                else []
            ),
            cluster=profile.dominant_cluster,
        ),
        "recommendation": InsightRecommendation(
            lever_ids=levers[:2],
            one_thing=profile.one_thing.lever if profile.one_thing else None,
        ),
        "leadership": InsightPlanLeadership(
            card_id="strategic-next-step",
            title="Strategic Next Step",
            headline_key=severity,
        ),
    }
    return with_confidence(plan_fields, profile)


# ---------------------------------------------------------------------------
# reason_about_section.py
# ---------------------------------------------------------------------------


@dataclass
class ReasonOptions:
    """Options controlling reasoning for a section."""

    active_metric_id: MetricId | None = None
    """Active disease for deep-dive tabs."""

    chart_finding: MetricFinding | None = None
    """
    Exact finding derived from the chart's rendered dataset.
    When set, observation facts MUST come from this object — no alternate
    recalculation.
    """

    used_levers: list[InterventionId] | None = None
    """Levers already recommended elsewhere on this report — prefer unused ones."""


def reason_about_section(
    section_id: SectionId,
    profile: CompanyHealthProfile,
    options: ReasonOptions | None = None,
) -> InsightPlan | list[InsightPlan]:
    options = options or ReasonOptions()
    chart_finding = options.chart_finding
    used = options.used_levers

    if section_id == "overall_risk":
        return plan_for_metric_section(
            section_id="overall_risk",
            metric_id="overall_risk",
            profile=profile,
            kind="distribution",
            chart_finding=chart_finding,
            used_levers=used,
        )
    if section_id == "disease_risks":
        return plan_disease_risks(profile, chart_finding, used_levers=used)
    if section_id == "disease_deep_dive":
        return plan_disease_deep_dive(
            profile, options.active_metric_id, chart_finding, used_levers=used
        )
    if section_id == "physical_activity":
        return plan_for_metric_section(
            section_id="physical_activity",
            metric_id="physical_activity",
            profile=profile,
            kind="lifestyle",
            chart_finding=chart_finding,
            used_levers=used,
        )
    if section_id == "sleep":
        return plan_for_metric_section(
            section_id="sleep",
            metric_id="sleep",
            profile=profile,
            kind="lifestyle",
            chart_finding=chart_finding,
            used_levers=used,
        )
    if section_id == "nutrition":
        return plan_for_metric_section(
            section_id="nutrition",
            metric_id="nutrition",
            profile=profile,
            kind="lifestyle",
            chart_finding=chart_finding,
            used_levers=used,
        )
    if section_id == "oxidative_stress":
        return plan_for_metric_section(
            section_id="oxidative_stress",
            metric_id="oxidative_stress",
            profile=profile,
            kind="lifestyle",
            chart_finding=chart_finding,
            used_levers=used,
        )
    if section_id == "positive_highlights":
        return plan_positive_highlights(profile)
    if section_id == "leadership":
        return plan_leadership_cards(profile)
    if section_id == "metabolic_age":
        return plan_metabolic_age(profile, used_levers=used)
    if section_id == "bmi_waist":
        return plan_bmi_waist(profile, used_levers=used)
    if section_id == "participation":
        if chart_finding:
            extras = chart_finding.extras or {}
            return plan_for_metric_section(
                section_id="participation",
                metric_id="participation",
                profile=profile,
                kind="status",
                chart_finding=chart_finding,
                used_levers=used,
                extra_facts={
                    "top_age_group": str(extras.get("topAgeGroup", "") or ""),
                    "top_percent": float(extras.get("topPercent", 0) or 0),
                    "top_enrolled": float(extras.get("topEnrolled", 0) or 0),
                },
            )
        return plan_participation(profile)

    return plan_for_metric_section(
        section_id=section_id,
        metric_id=chart_finding.metric_id if chart_finding else "overall_risk",
        profile=profile,
        chart_finding=chart_finding,
        used_levers=used,
    )


__all__ = [
    "ReasonOptions",
    "compute_insight_confidence",
    "empty_confidence",
    "reason_about_section",
    "with_confidence",
]
