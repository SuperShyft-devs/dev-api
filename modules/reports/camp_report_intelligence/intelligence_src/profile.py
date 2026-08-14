"""Flat staging merge of scoring + graph + priority + profile composition.

Merged without logic changes from:
  - intelligence/scoring/score_metric.py
  - intelligence/graph/evaluate_graph.py
  - intelligence/priority/calculate_priorities.py
  - intelligence/profile/select_one_thing.py
  - intelligence/profile/compose_company_profile.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .knowledge import (
    GRAPH_EDGE_MIN_ACTIVATION,
    GRAPH_EDGES,
    GRAPH_NODE_ACTIVATION_THRESHOLD,
    GRAPH_STRENGTH_ACTIVATION_THRESHOLD,
    SCORING_WEIGHTS,
    SEVERITY_WEIGHT,
    STRENGTH_LIMIT,
    TOP_RISK_LIMIT,
    get_modifiability,
    severity_from_prevalence,
)
from .models import (
    ChartTopDisease,
    CompanyHealthProfile,
    EmergentPriority,
    GraphActivation,
    GraphEvaluation,
    HealthCluster,
    InterventionId,
    LifestylePriority,
    MetricFinding,
    MetricId,
    MetricScore,
    OneThingPriority,
    ScoreComponents,
    ScoringWeights,
    Strength,
    TopRisk,
    js_round,
    round1,
    round3,
)

# ---------------------------------------------------------------------------
# scoring/score_metric.py
# ---------------------------------------------------------------------------


def _clamp01(n: float) -> float:
    return max(0.0, min(1.0, n))


def _clamp100(n: float) -> float:
    return max(0.0, min(100.0, n))


def score_metric(
    finding: MetricFinding,
    weights: Optional[ScoringWeights] = None,
    sample_size_hint: Optional[float] = None,
) -> MetricScore:
    w = weights or SCORING_WEIGHTS
    prevalence = finding.elevated_share
    if finding.elevated_share > 0:
        intensity = _clamp01(finding.high_severity_share / max(finding.elevated_share, 1)) * 100
    else:
        intensity = 0.0

    severity_band = severity_from_prevalence(prevalence)
    severity_component = SEVERITY_WEIGHT[severity_band] * 100
    modifiability = get_modifiability(finding.metric_id)

    sample_size = finding.sample_size if finding.sample_size is not None else sample_size_hint
    if sample_size is None:
        data_quality = 0.75
    elif sample_size >= 100:
        data_quality = 1.0
    elif sample_size >= 30:
        data_quality = 0.85
    elif sample_size >= 10:
        data_quality = 0.65
    else:
        data_quality = 0.45

    prevalence_n = _clamp01(prevalence / 100) * 100
    intensity_n = intensity
    raw = (
        w.prevalence * prevalence_n
        + w.intensity * intensity_n
        + w.severity * severity_component
    )

    mod_boost = 1 + w.modifiability_boost * (modifiability - 0.5) * 2
    quality_factor = w.data_quality_floor + (1 - w.data_quality_floor) * data_quality
    burden_score = _clamp100(raw * mod_boost * quality_factor)

    strength_mult = 1.0 if finding.metric_id == "positive_wins" else 0.9
    strength_score = _clamp100(finding.healthy_share * strength_mult)

    return MetricScore(
        metric_id=finding.metric_id,
        burden_score=round1(burden_score),
        strength_score=round1(strength_score),
        severity_band=severity_band,
        prevalence=prevalence,
        intensity=round1(intensity),
        modifiability=modifiability,
        data_quality=data_quality,
        components=ScoreComponents(
            prevalence=round1(prevalence_n),
            intensity=round1(intensity_n),
            severity=round1(severity_component),
        ),
    )


def score_all(
    findings: List[MetricFinding],
    weights: Optional[ScoringWeights] = None,
) -> Dict[MetricId, MetricScore]:
    out: Dict[MetricId, MetricScore] = {}
    for finding in findings:
        out[finding.metric_id] = score_metric(finding, weights)
    return out


# ---------------------------------------------------------------------------
# graph/evaluate_graph.py
# ---------------------------------------------------------------------------


def evaluate_graph(scores: Dict[MetricId, MetricScore]) -> GraphEvaluation:
    activations: List[GraphActivation] = []
    node_influence: Dict[MetricId, float] = {}

    for metric_id, score in scores.items():
        node_influence[metric_id] = score.burden_score

    for edge in GRAPH_EDGES:
        from_score = scores.get(edge.from_)
        to_score = scores.get(edge.to)
        if from_score is None or to_score is None:
            continue

        activation = 0.0

        if edge.type == "protects":
            from_strong = from_score.strength_score >= GRAPH_STRENGTH_ACTIVATION_THRESHOLD
            to_strong = to_score.strength_score >= GRAPH_STRENGTH_ACTIVATION_THRESHOLD
            if not from_strong or not to_strong:
                continue
            activation = edge.weight * ((from_score.strength_score / 100) * (to_score.strength_score / 100))
        else:
            from_active = from_score.burden_score >= GRAPH_NODE_ACTIVATION_THRESHOLD
            to_active = to_score.burden_score >= GRAPH_NODE_ACTIVATION_THRESHOLD
            if not from_active or not to_active:
                continue
            activation = edge.weight * ((from_score.burden_score / 100) * (to_score.burden_score / 100))

        if activation < GRAPH_EDGE_MIN_ACTIVATION:
            continue

        activations.append(
            GraphActivation(
                edge_id=edge.id,
                effect_id=edge.effect_id,
                activation=round3(activation),
                from_=edge.from_,
                to=edge.to,
                primary_lever=edge.primary_lever,
                contributing_scores={
                    edge.from_: from_score.burden_score,
                    edge.to: to_score.burden_score,
                },
            )
        )

        # One-hop influence boost only (no multi-hop propagation)
        node_influence[edge.from_] = node_influence.get(edge.from_, 0) + activation * 10
        node_influence[edge.to] = node_influence.get(edge.to, 0) + activation * 10

    emergent_priorities = _aggregate_priorities(activations)
    dominant_cluster = _resolve_dominant_cluster(scores, emergent_priorities)

    return GraphEvaluation(
        activations=sorted(activations, key=lambda a: a.activation, reverse=True),
        node_influence=node_influence,
        emergent_priorities=emergent_priorities,
        dominant_cluster=dominant_cluster,
    )


def _aggregate_priorities(activations: List[GraphActivation]) -> List[EmergentPriority]:
    by_effect: Dict[str, EmergentPriority] = {}

    for act in activations:
        existing = by_effect.get(act.effect_id)
        members: List[MetricId] = list(existing.members) if existing else []
        members_set = dict.fromkeys(members)  # preserve insertion order, like a JS Set
        members_set[act.from_] = None
        members_set[act.to] = None

        by_effect[act.effect_id] = EmergentPriority(
            effect_id=act.effect_id,
            score=round3((existing.score if existing else 0) + act.activation),
            lever=(existing.lever if existing else None) or act.primary_lever,
            members=list(members_set.keys()),
        )

    return sorted(by_effect.values(), key=lambda p: p.score, reverse=True)


_CLUSTER_MAP: Dict[MetricId, str] = {
    "type_2_diabetes": "metabolic",
    "obesity": "metabolic",
    "nafld": "metabolic",
    "metabolic_syndrome": "metabolic",
    "metabolic_age": "metabolic",
    "bmi_waist": "metabolic",
    "hypertension": "cardiovascular",
    "cardiac_health": "cardiovascular",
    "dyslipidemia": "cardiovascular",
    "pcos_pcod": "hormonal",
    "thyroid_health": "hormonal",
    "physical_activity": "lifestyle",
    "nutrition": "lifestyle",
    "sleep": "recovery",
    "oxidative_stress": "recovery",
}


def _resolve_dominant_cluster(
    scores: Dict[MetricId, MetricScore],
    priorities: List[EmergentPriority],
) -> HealthCluster:
    top = priorities[0] if priorities else None
    if top:
        if top.effect_id == "workforce_resilience":
            return "healthy"
        if top.effect_id == "recovery_strain":
            return "recovery"
        if top.effect_id in ("cardio_cluster", "cardio_nutrition"):
            return "cardiovascular"
        if top.effect_id in ("metabolic_cluster", "movement_priority"):
            return "metabolic"

    cluster_burden: Dict[str, float] = {
        "metabolic": 0,
        "cardiovascular": 0,
        "hormonal": 0,
        "lifestyle": 0,
        "recovery": 0,
    }

    for metric_id, score in scores.items():
        cluster = _CLUSTER_MAP.get(metric_id)
        if cluster:
            cluster_burden[cluster] += score.burden_score

    ranked = sorted(cluster_burden.items(), key=lambda kv: kv[1], reverse=True)
    best = ranked[0] if ranked else None
    second = ranked[1] if len(ranked) > 1 else None
    if not best or best[1] < 25:
        return "healthy"
    if second and best[1] - second[1] < 15:
        return "mixed"
    return best[0]  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# priority/calculate_priorities.py
# ---------------------------------------------------------------------------


@dataclass
class PriorityResult:
    scores: Dict[MetricId, MetricScore]
    graph: GraphEvaluation
    top_risks: List[TopRisk]
    strengths: List[Strength]


def calculate_priorities(
    findings: List[MetricFinding],
    weights: Optional[ScoringWeights] = None,
) -> PriorityResult:
    scores = score_all(findings, weights)
    graph = evaluate_graph(scores)
    scored = [s for s in scores.values() if s is not None]

    top_risks = [
        TopRisk(metric_id=s.metric_id, burden_score=s.burden_score)
        for s in sorted(scored, key=lambda s: s.burden_score, reverse=True)[:TOP_RISK_LIMIT]
    ]

    strengths = [
        Strength(metric_id=s.metric_id, strength_score=s.strength_score)
        for s in sorted(
            [s for s in scored if s.strength_score >= 55 and s.burden_score < 35],
            key=lambda s: s.strength_score,
            reverse=True,
        )[:STRENGTH_LIMIT]
    ]

    return PriorityResult(scores=scores, graph=graph, top_risks=top_risks, strengths=strengths)


# ---------------------------------------------------------------------------
# profile/select_one_thing.py
# ---------------------------------------------------------------------------


def select_one_thing(scores: Dict[MetricId, MetricScore], graph: GraphEvaluation) -> Optional[OneThingPriority]:
    """
    Select the single highest-leverage HR action from emergent graph priorities
    and top burden scores.
    """
    top_priority = graph.emergent_priorities[0] if graph.emergent_priorities else None
    if top_priority and top_priority.lever:
        return OneThingPriority(
            lever=top_priority.lever,
            target_metrics=top_priority.members,
            rationale_code=top_priority.effect_id,
            effect_id=top_priority.effect_id,
        )

    ranked = sorted([s for s in scores.values() if s is not None], key=lambda s: s.burden_score, reverse=True)

    top = ranked[0] if ranked else None
    if not top or top.burden_score < 25:
        return OneThingPriority(
            lever="maintain_wellness",
            target_metrics=[s.metric_id for s in ranked[:2]],
            rationale_code="maintain",
        )

    lever = _default_lever_for_metric(top.metric_id)
    return OneThingPriority(
        lever=lever,
        target_metrics=[top.metric_id],
        rationale_code=f"top_burden_{top.metric_id}",
    )


_DEFAULT_LEVER_MAP: Dict[MetricId, InterventionId] = {
    "physical_activity": "movement_programme",
    "sleep": "sleep_health",
    "nutrition": "nutrition_heart_healthy",
    "oxidative_stress": "recovery_programme",
    "obesity": "weight_management",
    "type_2_diabetes": "metabolic_screening",
    "dyslipidemia": "lipid_screening",
    "hypertension": "bp_screening",
    "cardiac_health": "cardiac_screening",
    "nafld": "liver_screening",
    "thyroid_health": "thyroid_screening",
    "pcos_pcod": "womens_health",
    "metabolic_syndrome": "metabolic_screening",
    "overall_risk": "target_high_risk",
}


def _default_lever_for_metric(metric_id: MetricId) -> InterventionId:
    return _DEFAULT_LEVER_MAP.get(metric_id, "scale_preventive_care")


# ---------------------------------------------------------------------------
# profile/compose_company_profile.py
# ---------------------------------------------------------------------------


@dataclass
class ComposeProfileOptions:
    chart_top_diseases: List[ChartTopDisease] = field(default_factory=list)


def compose_profile_from_findings(
    findings: List[MetricFinding],
    options: Optional[ComposeProfileOptions] = None,
) -> CompanyHealthProfile:
    """
    Internal: build profile from analyzer findings.
    Public callers should use compose_company_profile(dashboard_data) from the
    engine API (adapters/dashboard_slices.py).
    """
    options = options or ComposeProfileOptions()
    finding_map: Dict[MetricId, MetricFinding] = {}
    for f in findings:
        finding_map[f.metric_id] = f

    result = calculate_priorities(findings)
    scores, graph, top_risks, strengths = result.scores, result.graph, result.top_risks, result.strengths

    overall_burden = 0.0 if not top_risks else round1(sum(r.burden_score for r in top_risks) / len(top_risks))

    overall_finding = finding_map.get("overall_risk")
    overall_severity = severity_from_prevalence(
        overall_finding.elevated_share if overall_finding else overall_burden
    )

    one_thing = select_one_thing(scores, graph)
    lifestyle_priority = _resolve_lifestyle_priority(scores, graph)
    coverage = [f.metric_id for f in findings]
    profile_confidence = _compute_profile_confidence(coverage, scores)

    return CompanyHealthProfile(
        scores=scores,
        findings=finding_map,
        graph=graph,
        top_risks=top_risks,
        strengths=strengths,
        emergent_priorities=graph.emergent_priorities,
        one_thing=one_thing,
        overall_severity=overall_severity,
        overall_burden=overall_burden,
        dominant_cluster=graph.dominant_cluster,
        lifestyle_priority=lifestyle_priority,
        profile_confidence=profile_confidence,
        coverage=coverage,
        chart_top_diseases=options.chart_top_diseases,
    )


def _resolve_lifestyle_priority(scores: Dict[MetricId, MetricScore], graph) -> LifestylePriority:
    top_effect = graph.emergent_priorities[0].effect_id if graph.emergent_priorities else None
    if top_effect == "recovery_strain":
        return "recovery"
    if top_effect == "movement_priority":
        return "physical"
    if top_effect == "cardio_nutrition":
        return "nutrition"
    if top_effect == "workforce_resilience":
        return "strong"

    activity = scores["physical_activity"].burden_score if "physical_activity" in scores else 0
    sleep = scores["sleep"].burden_score if "sleep" in scores else 0
    nutrition = scores["nutrition"].burden_score if "nutrition" in scores else 0
    max_score = max(activity, sleep, nutrition)
    if max_score < 25:
        return "strong"
    if max_score == activity:
        return "physical"
    if max_score == sleep:
        return "sleep"
    if max_score == nutrition:
        return "nutrition"
    return "balanced"


_EXPECTED_COVERAGE: List[MetricId] = ["overall_risk", "physical_activity", "sleep", "nutrition", "oxidative_stress"]


def _compute_profile_confidence(coverage: List[MetricId], scores: Dict[MetricId, MetricScore]) -> float:
    covered = len([m for m in _EXPECTED_COVERAGE if m in coverage]) / len(_EXPECTED_COVERAGE)
    qualities = [s.data_quality for s in scores.values() if s is not None]
    avg_quality = 0.4 if not qualities else sum(qualities) / len(qualities)
    return js_round((0.55 * covered + 0.45 * avg_quality) * 100) / 100
