"""Camp Report Intelligence Engine — production single-file distribution.

Flattened from the working multi-file engine in ``modules/reports/intelligence/``.
This file is a packaging of that engine, not a rewrite. Intelligence logic,
thresholds, knowledge, scoring, reasoning, generation, and Camp Report assembly
are preserved.

Public API::

    generate_report_insights(report_json)
    enrich_camp_report_with_intelligence(report_json)
"""

from __future__ import annotations

import copy
import hashlib
import math
import re
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

# ============================================================
# DATA MODELS
# ============================================================

"""
Internal numeric helpers shared across the engine.

Python's built-in ``round()`` uses banker's rounding (round-half-to-even),
while JavaScript's ``Math.round`` always rounds halves toward +Infinity.
To keep scoring bit-identical to the TypeScript source, every rounding call
in this package must go through ``js_round``/``round1``/``round3`` below
instead of the Python builtin.
"""


import math
from dataclasses import dataclass, field, is_dataclass
from typing import Any, Dict, List, Literal, Optional, Union


def js_round(n: float) -> float:
    """Replicate JavaScript's ``Math.round`` (halves round toward +Infinity)."""
    return math.floor(n + 0.5)


def round1(n: float) -> float:
    """Round to 1 decimal place, matching ``Math.round(n * 10) / 10`` in TS."""
    return js_round(n * 10) / 10


def round3(n: float) -> float:
    """Round to 3 decimal places, matching ``Math.round(n * 1000) / 1000`` in TS."""
    return js_round(n * 1000) / 1000


"""Health Intelligence Engine — shared types. No React. No English generation.

Faithful port of src/intelligence/types.ts. Field names use snake_case (Python
convention) while every string *value* — metric ids, section ids, intervention
ids, severity bands, etc. — is kept byte-for-byte identical to the TypeScript
source so cross-language comparisons and fixtures line up.

``from`` is a Python keyword, so ``HealthGraphEdge.from`` / ``GraphActivation.from``
became ``from_`` here. That is the only structural deviation from types.ts.
"""

SeverityBand = Literal["very_low", "low", "moderate", "high", "very_high"]
InsightTone = Literal["concern", "positive", "neutral"]
ConfidenceBand = Literal["low", "moderate", "high"]
EdgeType = Literal["drives", "reinforces", "clusters_with", "protects", "indicates"]
HealthCluster = Literal[
    "metabolic",
    "cardiovascular",
    "hormonal",
    "lifestyle",
    "recovery",
    "mixed",
    "healthy",
]

MetricId = Literal[
    "overall_risk",
    "physical_activity",
    "sleep",
    "nutrition",
    "oxidative_stress",
    "metabolic_age",
    "bmi_waist",
    "positive_wins",
    "participation",
    "type_2_diabetes",
    "hypertension",
    "obesity",
    "pcos_pcod",
    "nafld",
    "cardiac_health",
    "thyroid_health",
    "dyslipidemia",
    "metabolic_syndrome",
]

SectionId = Literal[
    "overall_risk",
    "disease_risks",
    "disease_deep_dive",
    "physical_activity",
    "sleep",
    "nutrition",
    "oxidative_stress",
    "positive_highlights",
    "leadership",
    "metabolic_age",
    "bmi_waist",
    "participation",
    "gender_comparison",
    "abnormal_markers",
    "blood_heatmap",
    "interventions",
]

InterventionId = Literal[
    "metabolic_screening",
    "lipid_screening",
    "bp_screening",
    "thyroid_screening",
    "liver_screening",
    "cardiac_screening",
    "nutrition_refined_carb",
    "nutrition_heart_healthy",
    "nutrition_sodium",
    "nutrition_whole_food",
    "movement_programme",
    "weight_management",
    "sleep_health",
    "stress_management",
    "recovery_programme",
    "womens_health",
    "smoking_cessation",
    "alcohol_moderation",
    "maintain_wellness",
    "target_high_risk",
    "scale_preventive_care",
    "clinical_review",
]

CategoryPolarity = Literal["healthy", "elevated", "neutral"]

LifestyleDriver = Literal["nutrition", "activity", "sleep", "stress", "weight", "alcohol", "smoking"]

LifestylePriority = Literal["physical", "sleep", "nutrition", "recovery", "balanced", "strong"]


@dataclass
class CategoryShare:
    id: str
    label: str
    percent: float
    polarity: CategoryPolarity
    count: Optional[float] = None


@dataclass
class GenderGap:
    male_elevated: float
    female_elevated: float
    delta: float


@dataclass
class MetricFinding:
    metric_id: MetricId
    categories: List[CategoryShare]
    dominant: Optional[CategoryShare]
    healthy_share: float
    elevated_share: float
    high_severity_share: float
    opportunity: Optional[CategoryShare]
    gender_gap: Optional[GenderGap] = None
    sample_size: Optional[float] = None
    """Extra analyzer facts for composers (counts, names, etc.)"""
    extras: Optional[Dict[str, Any]] = None


@dataclass
class ScoreComponents:
    prevalence: float
    intensity: float
    severity: float


@dataclass
class MetricScore:
    metric_id: MetricId
    """0 = excellent, 100 = critical burden"""
    burden_score: float
    """0 = no strength, 100 = strong protective signal"""
    strength_score: float
    severity_band: SeverityBand
    prevalence: float
    intensity: float
    modifiability: float
    data_quality: float
    components: ScoreComponents


@dataclass
class HealthGraphEdge:
    id: str
    from_: MetricId
    to: MetricId
    type: EdgeType
    weight: float
    min_confidence: ConfidenceBand
    effect_id: str
    primary_lever: Optional[InterventionId] = None


@dataclass
class GraphActivation:
    edge_id: str
    effect_id: str
    activation: float
    from_: MetricId
    to: MetricId
    contributing_scores: Dict[MetricId, float]
    primary_lever: Optional[InterventionId] = None


@dataclass
class EmergentPriority:
    effect_id: str
    score: float
    members: List[MetricId]
    lever: Optional[InterventionId] = None


@dataclass
class GraphEvaluation:
    activations: List[GraphActivation]
    node_influence: Dict[MetricId, float]
    emergent_priorities: List[EmergentPriority]
    dominant_cluster: HealthCluster


@dataclass
class OneThingPriority:
    lever: InterventionId
    target_metrics: List[MetricId]
    rationale_code: str
    effect_id: Optional[str] = None


@dataclass
class TopRisk:
    metric_id: MetricId
    burden_score: float


@dataclass
class Strength:
    metric_id: MetricId
    strength_score: float


@dataclass
class ChartTopDisease:
    """Exact top-disease ranking as rendered by the Top Disease Risk chart."""

    metric_id: MetricId
    name: str
    high_risk_percent: float


@dataclass
class CompanyHealthProfile:
    scores: Dict[MetricId, MetricScore]
    findings: Dict[MetricId, MetricFinding]
    graph: GraphEvaluation
    top_risks: List[TopRisk]
    strengths: List[Strength]
    emergent_priorities: List[EmergentPriority]
    one_thing: Optional[OneThingPriority]
    overall_severity: SeverityBand
    overall_burden: float
    dominant_cluster: HealthCluster
    lifestyle_priority: LifestylePriority
    profile_confidence: float
    coverage: List[MetricId]
    """
    Single source of truth for the Top Disease Risk chart.
    Narrative for disease_risks / leadership disease focus must use this list —
    never a recomputed ranking from deep-dive distributions.
    """
    chart_top_diseases: List[ChartTopDisease] = field(default_factory=list)


@dataclass
class ConfidenceFactors:
    data_quality: float
    signal_strength: float
    graph_support: float
    knowledge_coverage: float


@dataclass
class InsightConfidence:
    score: float
    band: ConfidenceBand
    factors: ConfidenceFactors
    notes: List[str] = field(default_factory=list)


@dataclass
class InsightObservation:
    kind: Literal["distribution", "disease_lead", "lifestyle", "strength", "cluster", "status"]
    metric_id: MetricId
    facts: Dict[str, Any]


@dataclass
class InsightExplanation:
    medical_frame_id: str
    effect_ids: List[str]
    cluster: Optional[HealthCluster] = None


@dataclass
class InsightRecommendation:
    lever_ids: List[InterventionId]
    one_thing: Optional[InterventionId] = None
    """Company one_thing lever (may differ from lever_ids[0] when this section supports it)."""
    action_role: Literal["org_primary", "section_support", "maintain"] = "section_support"
    """
    org_primary — answers the company-level 'single most appropriate action'.
    section_support — complementary action for this metric/section given the profile.
    maintain — reinforce existing strengths / monitoring.
    """
    lifestyle_priority: Optional[LifestylePriority] = None


@dataclass
class InsightPlanLeadership:
    card_id: str
    title: str
    headline_key: str


@dataclass
class InsightPlan:
    section_id: SectionId
    mode: Literal["concern", "positive", "mixed", "leadership"]
    tone: InsightTone
    severity_band: SeverityBand
    observation: InsightObservation
    explanation: InsightExplanation
    recommendation: InsightRecommendation
    confidence: InsightConfidence
    """Leadership-only extras"""
    leadership: Optional[InsightPlanLeadership] = None


@dataclass
class StructuredInsight:
    section_id: SectionId
    tone: InsightTone
    severity_band: SeverityBand
    confidence: InsightConfidence
    observation: str
    explanation: str
    recommendation: str
    headline: Optional[str] = None
    effect_ids: Optional[List[str]] = None
    lever_ids: Optional[List[InterventionId]] = None
    related_metrics: Optional[List[MetricId]] = None


@dataclass
class ChartNarrative:
    tone: InsightTone
    text: str
    structured: StructuredInsight
    confidence: InsightConfidence


@dataclass
class LeadershipTakeawayCard:
    id: str
    title: str
    headline: str
    body: str
    confidence: InsightConfidence
    structured: StructuredInsight


@dataclass
class ScoringWeights:
    prevalence: float
    intensity: float
    severity: float
    modifiability_boost: float
    data_quality_floor: float


@dataclass
class DiseaseInterventions:
    high: List[InterventionId]
    medium: List[InterventionId]


@dataclass
class DiseaseKnowledge:
    id: MetricId
    display_name: str
    cluster: Literal["metabolic", "cardiovascular", "hormonal"]
    clinical_focus: str
    workplace_relevance: str
    lifestyle_drivers: List[LifestyleDriver]
    related_metrics: List[MetricId]
    biomarkers: List[str]
    interventions: DiseaseInterventions
    modifiability: float
    medical_frame_id: str


@dataclass
class LifestyleKnowledge:
    id: MetricId
    display_name: str
    cluster: HealthCluster
    clinical_focus: str
    poor_labels: List[str]
    healthy_labels: List[str]
    interventions: DiseaseInterventions
    modifiability: float
    medical_frame_id: str
    high_severity_labels: Optional[List[str]] = None


@dataclass
class InterventionKnowledge:
    id: InterventionId
    phrase: str
    category: str


"""
Health Intelligence Engine — dashboard data inputs.
Framework-agnostic DTOs. No FastAPI. No UI.

Faithful port of src/intelligence/input.ts. Every dataclass also accepts plain
dicts via ``from_dict`` (and ``DashboardInput.from_dict``/``ensure`` at the top
level) so callers can pass either raw JSON-ish dicts (camelCase, matching the
original TypeScript payload shape) or already-typed dataclasses.
"""

RiskLevel = Literal["Healthy", "Increased", "High", "Very High"]

DiseaseCode = Literal[
    "metabolic_syndrome",
    "type_2_diabetes",
    "hypertension",
    "obesity",
    "pcos_pcod",
    "nafld",
    "cardiac_health",
    "thyroid_health",
    "dyslipidemia",
    "oxidative_stress",
]

OverallRiskBand = Literal["Optimal", "Low risk", "Increased Risk", "High risk"]

LifestyleGenderView = Literal["both", "male", "female"]


def _pick(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _list(items: Any, factory) -> Optional[List[Any]]:
    if items is None:
        return None
    return [factory(i) if isinstance(i, dict) else i for i in items]


@dataclass
class DiseaseDefinition:
    code: DiseaseCode
    name: str

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DiseaseDefinition":
        return DiseaseDefinition(code=_pick(d, "code"), name=_pick(d, "name"))


@dataclass
class ParticipationByAge:
    age_group: str
    enrolled: float
    percent: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "ParticipationByAge":
        return ParticipationByAge(
            age_group=_pick(d, "ageGroup", "age_group"),
            enrolled=_pick(d, "enrolled"),
            percent=_pick(d, "percent"),
        )


@dataclass
class TopHighRiskDisease:
    name: str
    high_risk_percent: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TopHighRiskDisease":
        return TopHighRiskDisease(
            name=_pick(d, "name"),
            high_risk_percent=_pick(d, "highRiskPercent", "high_risk_percent"),
        )


@dataclass
class CompanyAverageScores:
    nutrition: float
    fitness: float
    lifestyle: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "CompanyAverageScores":
        return CompanyAverageScores(
            nutrition=_pick(d, "nutrition"),
            fitness=_pick(d, "fitness"),
            lifestyle=_pick(d, "lifestyle"),
        )


@dataclass
class OverallRiskScoreBucket:
    band: OverallRiskBand
    percent: float
    count: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "OverallRiskScoreBucket":
        return OverallRiskScoreBucket(
            band=_pick(d, "band"),
            percent=_pick(d, "percent"),
            count=_pick(d, "count"),
        )


@dataclass
class DistributionSlice:
    label: str
    percent: float
    count: Optional[float] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DistributionSlice":
        return DistributionSlice(
            label=_pick(d, "label"),
            percent=_pick(d, "percent"),
            count=_pick(d, "count"),
        )


@dataclass
class GenderDistributionPair:
    male: List[DistributionSlice]
    female: List[DistributionSlice]

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "GenderDistributionPair":
        return GenderDistributionPair(
            male=_list(_pick(d, "male") or [], DistributionSlice.from_dict) or [],
            female=_list(_pick(d, "female") or [], DistributionSlice.from_dict) or [],
        )


@dataclass
class RiskDistributionBucket:
    level: RiskLevel
    segments: Dict[str, float]
    counts: Optional[Dict[str, float]] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "RiskDistributionBucket":
        return RiskDistributionBucket(
            level=_pick(d, "level"),
            segments=_pick(d, "segments") or {},
            counts=_pick(d, "counts"),
        )


@dataclass
class DiseaseRiskData:
    disease: DiseaseDefinition
    buckets: List[RiskDistributionBucket]
    overall_status: RiskLevel

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DiseaseRiskData":
        disease = _pick(d, "disease")
        buckets = _pick(d, "buckets") or []
        return DiseaseRiskData(
            disease=DiseaseDefinition.from_dict(disease) if isinstance(disease, dict) else disease,
            buckets=[RiskDistributionBucket.from_dict(b) if isinstance(b, dict) else b for b in buckets],
            overall_status=_pick(d, "overallStatus", "overall_status"),
        )


@dataclass
class OxidativeStressByDept:
    department: str
    low: float
    moderate: float
    high: float
    very_high: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "OxidativeStressByDept":
        return OxidativeStressByDept(
            department=_pick(d, "department"),
            low=_pick(d, "low") or 0,
            moderate=_pick(d, "moderate") or 0,
            high=_pick(d, "high") or 0,
            very_high=_pick(d, "veryHigh", "very_high") or 0,
        )


@dataclass
class MetabolicAgeBucket:
    label: str
    count: float
    percent: float
    is_high_risk: bool

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "MetabolicAgeBucket":
        return MetabolicAgeBucket(
            label=_pick(d, "label"),
            count=_pick(d, "count") or 0,
            percent=_pick(d, "percent") or 0,
            is_high_risk=bool(_pick(d, "isHighRisk", "is_high_risk")),
        )


@dataclass
class MetabolicAgeSummary:
    buckets: List[MetabolicAgeBucket]
    avg_gap_years: float
    high_risk_percent: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "MetabolicAgeSummary":
        return MetabolicAgeSummary(
            buckets=[MetabolicAgeBucket.from_dict(b) if isinstance(b, dict) else b for b in (_pick(d, "buckets") or [])],
            avg_gap_years=_pick(d, "avgGapYears", "avg_gap_years") or 0,
            high_risk_percent=_pick(d, "highRiskPercent", "high_risk_percent") or 0,
        )


@dataclass
class PositiveWinDisease:
    code: str
    name: str
    risk_status: str

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "PositiveWinDisease":
        return PositiveWinDisease(
            code=_pick(d, "code"),
            name=_pick(d, "name"),
            risk_status=_pick(d, "riskStatus", "risk_status"),
        )


@dataclass
class HealthyHabit:
    habit_label: str

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "HealthyHabit":
        return HealthyHabit(habit_label=_pick(d, "habitLabel", "habit_label"))


@dataclass
class PositiveWins:
    low_risk: List[PositiveWinDisease]
    healthy_habits: List[HealthyHabit]
    healthy_profiles: List[str]

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "PositiveWins":
        return PositiveWins(
            low_risk=[
                PositiveWinDisease.from_dict(x) if isinstance(x, dict) else x
                for x in (_pick(d, "lowRisk", "low_risk") or [])
            ],
            healthy_habits=[
                HealthyHabit.from_dict(x) if isinstance(x, dict) else x
                for x in (_pick(d, "healthyHabits", "healthy_habits") or [])
            ],
            healthy_profiles=list(_pick(d, "healthyProfiles", "healthy_profiles") or []),
        )


@dataclass
class NutritionMacroStat:
    name: str
    within_ideal_percent: float
    above_ideal_percent: float
    below_ideal_percent: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "NutritionMacroStat":
        return NutritionMacroStat(
            name=_pick(d, "name"),
            within_ideal_percent=_pick(d, "withinIdealPercent", "within_ideal_percent") or 0,
            above_ideal_percent=_pick(d, "aboveIdealPercent", "above_ideal_percent") or 0,
            below_ideal_percent=_pick(d, "belowIdealPercent", "below_ideal_percent") or 0,
        )


@dataclass
class NutritionSummary:
    avg_score: float
    risk_band: str
    macros: List[NutritionMacroStat]

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "NutritionSummary":
        return NutritionSummary(
            avg_score=_pick(d, "avgScore", "avg_score") or 0,
            risk_band=_pick(d, "riskBand", "risk_band"),
            macros=[
                NutritionMacroStat.from_dict(m) if isinstance(m, dict) else m
                for m in (_pick(d, "macros") or [])
            ],
        )


@dataclass
class BmiBucket:
    label: str
    percent: float

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BmiBucket":
        return BmiBucket(label=_pick(d, "label"), percent=_pick(d, "percent") or 0)


@dataclass
class BmiWaistSummary:
    bmi_distribution: List[BmiBucket]
    avg_waist_inches: float
    above_ideal_waist_percent: float
    insight_tag: str

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BmiWaistSummary":
        return BmiWaistSummary(
            bmi_distribution=[
                BmiBucket.from_dict(b) if isinstance(b, dict) else b
                for b in (_pick(d, "bmiDistribution", "bmi_distribution") or [])
            ],
            avg_waist_inches=_pick(d, "avgWaistInches", "avg_waist_inches") or 0,
            above_ideal_waist_percent=_pick(d, "aboveIdealWaistPercent", "above_ideal_waist_percent") or 0,
            insight_tag=_pick(d, "insightTag", "insight_tag") or "",
        )


@dataclass
class GenderWeights:
    male: Optional[float] = None
    female: Optional[float] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "GenderWeights":
        return GenderWeights(male=_pick(d, "male"), female=_pick(d, "female"))


@dataclass
class DashboardInput:
    """
    Structured dashboard payload accepted by compose_company_profile().
    All fields optional — missing sections are simply omitted from the profile.
    """

    overall_risk_score: Optional[List[OverallRiskScoreBucket]] = None
    physical_activity: Optional[GenderDistributionPair] = None
    sleep: Optional[GenderDistributionPair] = None
    diseases: Optional[List[DiseaseRiskData]] = None
    """Exact list rendered by Top Disease Risk chart — authoritative for that section."""
    top_high_risk_diseases: Optional[List[TopHighRiskDisease]] = None
    oxidative_stress: Optional[List[OxidativeStressByDept]] = None
    oxidative_headcounts: Optional[Dict[str, float]] = None
    company_scores: Optional[CompanyAverageScores] = None
    nutrition: Optional[NutritionSummary] = None
    positive_wins: Optional[PositiveWins] = None
    metabolic_age: Optional[MetabolicAgeSummary] = None
    bmi_waist: Optional[BmiWaistSummary] = None
    participation_by_age: Optional[List[ParticipationByAge]] = None
    gender_weights: Optional[GenderWeights] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DashboardInput":
        overall_risk_score = _pick(d, "overallRiskScore", "overall_risk_score")
        physical_activity = _pick(d, "physicalActivity", "physical_activity")
        sleep = _pick(d, "sleep")
        diseases = _pick(d, "diseases")
        top_high_risk_diseases = _pick(d, "topHighRiskDiseases", "top_high_risk_diseases")
        oxidative_stress = _pick(d, "oxidativeStress", "oxidative_stress")
        company_scores = _pick(d, "companyScores", "company_scores")
        nutrition = _pick(d, "nutrition")
        positive_wins = _pick(d, "positiveWins", "positive_wins")
        metabolic_age = _pick(d, "metabolicAge", "metabolic_age")
        bmi_waist = _pick(d, "bmiWaist", "bmi_waist")
        participation_by_age = _pick(d, "participationByAge", "participation_by_age")
        gender_weights = _pick(d, "genderWeights", "gender_weights")

        return DashboardInput(
            overall_risk_score=(
                [OverallRiskScoreBucket.from_dict(b) if isinstance(b, dict) else b for b in overall_risk_score]
                if overall_risk_score is not None
                else None
            ),
            physical_activity=(
                GenderDistributionPair.from_dict(physical_activity)
                if isinstance(physical_activity, dict)
                else physical_activity
            ),
            sleep=GenderDistributionPair.from_dict(sleep) if isinstance(sleep, dict) else sleep,
            diseases=(
                [DiseaseRiskData.from_dict(x) if isinstance(x, dict) else x for x in diseases]
                if diseases is not None
                else None
            ),
            top_high_risk_diseases=(
                [TopHighRiskDisease.from_dict(x) if isinstance(x, dict) else x for x in top_high_risk_diseases]
                if top_high_risk_diseases is not None
                else None
            ),
            oxidative_stress=(
                [OxidativeStressByDept.from_dict(x) if isinstance(x, dict) else x for x in oxidative_stress]
                if oxidative_stress is not None
                else None
            ),
            oxidative_headcounts=_pick(d, "oxidativeHeadcounts", "oxidative_headcounts"),
            company_scores=(
                CompanyAverageScores.from_dict(company_scores)
                if isinstance(company_scores, dict)
                else company_scores
            ),
            nutrition=NutritionSummary.from_dict(nutrition) if isinstance(nutrition, dict) else nutrition,
            positive_wins=(
                PositiveWins.from_dict(positive_wins) if isinstance(positive_wins, dict) else positive_wins
            ),
            metabolic_age=(
                MetabolicAgeSummary.from_dict(metabolic_age) if isinstance(metabolic_age, dict) else metabolic_age
            ),
            bmi_waist=BmiWaistSummary.from_dict(bmi_waist) if isinstance(bmi_waist, dict) else bmi_waist,
            participation_by_age=(
                [ParticipationByAge.from_dict(x) if isinstance(x, dict) else x for x in participation_by_age]
                if participation_by_age is not None
                else None
            ),
            gender_weights=(
                GenderWeights.from_dict(gender_weights) if isinstance(gender_weights, dict) else gender_weights
            ),
        )

    @staticmethod
    def ensure(data: Union["DashboardInput", Dict[str, Any], None]) -> "DashboardInput":
        """Accept either a DashboardInput or a raw dict (camelCase or snake_case)."""
        if data is None:
            return DashboardInput()
        if is_dataclass(data) and not isinstance(data, dict):
            return data  # type: ignore[return-value]
        return DashboardInput.from_dict(data)


# @deprecated Prefer DashboardInput
DashboardSlices = DashboardInput

# ============================================================
# KNOWLEDGE BASE
# ============================================================

"""Shared severity bands — single source of truth.

Faithful port of src/intelligence/knowledge/severity.ts.
"""


import hashlib

from typing import Dict, List, Literal, Optional, Sequence, Tuple


SEVERITY_THRESHOLDS: List[Tuple[float, SeverityBand]] = [
    (10, "very_low"),
    (20, "low"),
    (40, "moderate"),
    (60, "high"),
    (float("inf"), "very_high"),
]

SEVERITY_WEIGHT: dict = {
    "very_low": 0.1,
    "low": 0.3,
    "moderate": 0.55,
    "high": 0.8,
    "very_high": 1,
}


def severity_from_prevalence(prevalence: float) -> SeverityBand:
    pct = max(0.0, min(100.0, prevalence))
    for max_value, band in SEVERITY_THRESHOLDS:
        if pct <= max_value:
            return band
    return "very_high"


def tone_from_severity(
    band: SeverityBand,
    mode: Literal["concern", "positive", "mixed", "leadership"],
) -> Literal["concern", "positive", "neutral"]:
    if mode == "positive":
        return "positive"
    if mode == "mixed":
        return "neutral"
    if band in ("very_low", "low"):
        return "positive"
    return "concern"


# Configurable scoring weights — never hardcode inside scoring functions.
SCORING_WEIGHTS = ScoringWeights(
    prevalence=0.5,
    intensity=0.25,
    severity=0.25,
    modifiability_boost=0.15,
    data_quality_floor=0.5,
)

# Node activation thresholds for graph evaluation (burden 0-100).
GRAPH_NODE_ACTIVATION_THRESHOLD = 30
GRAPH_STRENGTH_ACTIVATION_THRESHOLD = 55
GRAPH_EDGE_MIN_ACTIVATION = 0.25

CONFIDENCE_BANDS = {
    "high": 0.75,
    "moderate": 0.5,
}

TOP_RISK_LIMIT = 5
STRENGTH_LIMIT = 5


"""Faithful port of src/intelligence/knowledge/diseases.ts (including diseaseMetricFromName)."""

DISEASE_KNOWLEDGE: Dict[str, DiseaseKnowledge] = {
    "type_2_diabetes": DiseaseKnowledge(
        id="type_2_diabetes",
        display_name="Type 2 Diabetes",
        cluster="metabolic",
        clinical_focus=(
            "Rising diabetes risk suggests worsening blood sugar control across the workforce and a greater likelihood "
            "of developing long-term metabolic and cardiovascular complications."
        ),
        workplace_relevance="Strong predictor of future healthcare burden, productivity impact, and chronic disease progression.",
        lifestyle_drivers=["weight", "activity", "nutrition", "sleep", "stress"],
        related_metrics=["obesity", "physical_activity", "nutrition", "sleep", "nafld", "dyslipidemia"],
        biomarkers=["HbA1c", "Fasting Blood Sugar", "Fasting Insulin", "Triglycerides", "HDL", "ALT"],
        interventions=DiseaseInterventions(
            high=["metabolic_screening", "nutrition_refined_carb", "movement_programme"],
            medium=["sleep_health", "stress_management", "nutrition_whole_food"],
        ),
        modifiability=0.85,
        medical_frame_id="type_2_diabetes",
    ),

    "hypertension": DiseaseKnowledge(
        id="hypertension",
        display_name="Hypertension",
        cluster="cardiovascular",
        clinical_focus=(
            "Increasing blood-pressure risk suggests greater strain on cardiovascular health and a higher likelihood "
            "of future heart disease, stroke, and kidney-related complications."
        ),
        workplace_relevance="Often develops without noticeable symptoms, making regular screening essential for early detection and prevention.",
        lifestyle_drivers=["nutrition", "stress", "weight", "activity", "sleep", "alcohol", "smoking"],
        related_metrics=["cardiac_health", "dyslipidemia", "obesity", "sleep", "physical_activity"],
        biomarkers=["Creatinine", "eGFR", "Fasting Blood Sugar", "LDL", "HDL", "Triglycerides"],
        interventions=DiseaseInterventions(
            high=["bp_screening", "nutrition_sodium", "stress_management"],
            medium=["movement_programme", "sleep_health", "alcohol_moderation"],
        ),
        modifiability=0.8,
        medical_frame_id="hypertension",
    ),

    "obesity": DiseaseKnowledge(
        id="obesity",
        display_name="Obesity",
        cluster="metabolic",
        clinical_focus=(
            "Higher obesity levels indicate increasing weight-related health risk across the workforce, with potential "
            "effects on diabetes, heart health, liver health, and overall metabolic function."
        ),
        workplace_relevance="Major contributor to several chronic conditions and an important focus for preventive health programmes.",
        lifestyle_drivers=["nutrition", "activity", "sleep", "stress"],
        related_metrics=["physical_activity", "nutrition", "type_2_diabetes", "nafld", "dyslipidemia"],
        biomarkers=["Fasting Blood Sugar", "HbA1c", "Triglycerides", "HDL", "ALT"],
        interventions=DiseaseInterventions(
            high=["weight_management", "movement_programme", "nutrition_whole_food"],
            medium=["sleep_health", "nutrition_refined_carb"],
        ),
        modifiability=0.9,
        medical_frame_id="obesity",
    ),

    "pcos_pcod": DiseaseKnowledge(
        id="pcos_pcod",
        display_name="PCOS/PCOD",
        cluster="hormonal",
        clinical_focus=(
            "Higher PCOS-related risk highlights the connection between hormonal health, insulin resistance, and "
            "metabolic wellbeing among female employees."
        ),
        workplace_relevance="Common women's-health condition associated with metabolic changes and an increased long-term diabetes risk.",
        lifestyle_drivers=["nutrition", "activity", "weight", "stress", "sleep"],
        related_metrics=["obesity", "type_2_diabetes", "nutrition", "physical_activity"],
        biomarkers=["Fasting Insulin", "Fasting Blood Sugar", "HbA1c", "Triglycerides", "Vitamin D", "TSH"],
        interventions=DiseaseInterventions(
            high=["womens_health", "nutrition_refined_carb", "movement_programme"],
            medium=["stress_management", "sleep_health"],
        ),
        modifiability=0.7,
        medical_frame_id="pcos_pcod",
    ),

    "nafld": DiseaseKnowledge(
        id="nafld",
        display_name="NAFLD",
        cluster="metabolic",
        clinical_focus=(
            "Increasing fatty-liver risk suggests changes in metabolic health that may be associated with excess weight, "
            "poor glucose control, and abnormal lipid levels."
        ),
        workplace_relevance="Early metabolic health indicator that often progresses silently until more advanced stages.",
        lifestyle_drivers=["weight", "nutrition", "activity", "alcohol"],
        related_metrics=["obesity", "type_2_diabetes", "nutrition", "dyslipidemia"],
        biomarkers=["ALT", "AST", "GGT", "Fasting Blood Sugar", "HbA1c", "Triglycerides"],
        interventions=DiseaseInterventions(
            high=["liver_screening", "nutrition_refined_carb", "weight_management"],
            medium=["alcohol_moderation", "movement_programme"],
        ),
        modifiability=0.85,
        medical_frame_id="nafld",
    ),

    "cardiac_health": DiseaseKnowledge(
        id="cardiac_health",
        display_name="Cardiac Health",
        cluster="cardiovascular",
        clinical_focus=(
            "Rising cardiac-risk indicators suggest that multiple factors affecting heart health may be occurring "
            "together, increasing the likelihood of future cardiovascular complications."
        ),
        workplace_relevance="Key preventive health priority because cardiovascular disease remains a leading cause of serious illness and healthcare costs.",
        lifestyle_drivers=["nutrition", "activity", "stress", "sleep", "weight", "smoking"],
        related_metrics=["dyslipidemia", "hypertension", "type_2_diabetes", "oxidative_stress", "obesity"],
        biomarkers=["LDL", "HDL", "Triglycerides", "hs-CRP", "Fasting Blood Sugar", "HbA1c"],
        interventions=DiseaseInterventions(
            high=["cardiac_screening", "nutrition_heart_healthy", "smoking_cessation"],
            medium=["movement_programme", "stress_management", "sleep_health"],
        ),
        modifiability=0.75,
        medical_frame_id="cardiac_health",
    ),

    "thyroid_health": DiseaseKnowledge(
        id="thyroid_health",
        display_name="Thyroid Health",
        cluster="hormonal",
        clinical_focus=(
            "Higher thyroid-related risk may indicate changes in thyroid function that can influence energy levels, "
            "metabolism, weight regulation, and overall wellbeing."
        ),
        workplace_relevance="Common endocrine condition that can contribute to fatigue, reduced concentration, and lower daily performance if left unmanaged.",
        lifestyle_drivers=["stress"],
        related_metrics=["dyslipidemia", "sleep"],
        biomarkers=["TSH", "Free T4", "Free T3", "LDL", "Vitamin D", "Vitamin B12"],
        interventions=DiseaseInterventions(
            high=["thyroid_screening", "clinical_review"],
            medium=["womens_health", "stress_management"],
        ),
        modifiability=0.45,
        medical_frame_id="thyroid_health",
    ),

    "dyslipidemia": DiseaseKnowledge(
        id="dyslipidemia",
        display_name="Dyslipidemia",
        cluster="cardiovascular",
        clinical_focus=(
            "Increasing dyslipidemia risk indicates less favourable cholesterol and triglyceride patterns, which can "
            "contribute to the gradual development of cardiovascular disease."
        ),
        workplace_relevance="Well-established cardiovascular risk factor that responds effectively to early screening and lifestyle improvement.",
        lifestyle_drivers=["nutrition", "activity", "weight", "alcohol", "smoking"],
        related_metrics=["cardiac_health", "nutrition", "obesity", "physical_activity", "type_2_diabetes"],
        biomarkers=["LDL", "HDL", "Triglycerides", "Total Cholesterol", "Non-HDL Cholesterol"],
        interventions=DiseaseInterventions(
            high=["lipid_screening", "nutrition_heart_healthy", "movement_programme"],
            medium=["weight_management", "alcohol_moderation", "smoking_cessation"],
        ),
        modifiability=0.85,
        medical_frame_id="dyslipidemia",
    ),

    "metabolic_syndrome": DiseaseKnowledge(
        id="metabolic_syndrome",
        display_name="Metabolic Syndrome",
        cluster="metabolic",
        clinical_focus=(
            "Higher metabolic-syndrome risk suggests that weight, blood sugar, blood pressure, and lipid-related "
            "abnormalities are occurring together, increasing the likelihood of future chronic disease."
        ),
        workplace_relevance="Comprehensive indicator of metabolic health that reflects the combined burden of multiple lifestyle-related risk factors.",
        lifestyle_drivers=["weight", "activity", "nutrition", "stress", "sleep"],
        related_metrics=[
            "obesity",
            "type_2_diabetes",
            "dyslipidemia",
            "hypertension",
            "nafld",
            "physical_activity",
            "nutrition",
        ],
        biomarkers=["Fasting Blood Sugar", "HbA1c", "Triglycerides", "HDL", "hs-CRP"],
        interventions=DiseaseInterventions(
            high=["metabolic_screening", "nutrition_refined_carb", "movement_programme"],
            medium=["weight_management", "sleep_health", "stress_management"],
        ),
        modifiability=0.9,
        medical_frame_id="metabolic_syndrome",
    ),
}

def get_disease_knowledge(id: str) -> Optional[DiseaseKnowledge]:
    return DISEASE_KNOWLEDGE.get(id)


def is_disease_metric(id: MetricId) -> bool:
    return id in DISEASE_KNOWLEDGE


def disease_metric_from_name(name: str) -> Optional[MetricId]:
    """Map display names from dashboard API to metric ids."""
    normalized = name.strip().lower()
    for disease in DISEASE_KNOWLEDGE.values():
        if disease.display_name.lower() == normalized:
            return disease.id
    aliases: Dict[str, MetricId] = {
        "type 2 diabetes": "type_2_diabetes",
        "diabetes": "type_2_diabetes",
        "pcos/pcod": "pcos_pcod",
        "pcos": "pcos_pcod",
        "pcod": "pcos_pcod",
        "fatty liver": "nafld",
        "cardiac health": "cardiac_health",
        "thyroid health": "thyroid_health",
        "metabolic syndrome": "metabolic_syndrome",
    }
    return aliases.get(normalized)


"""Faithful port of src/intelligence/knowledge/lifestyle.ts."""

LIFESTYLE_KNOWLEDGE: Dict[str, LifestyleKnowledge] = {
    "physical_activity": LifestyleKnowledge(
        id="physical_activity",
        display_name="Physical Activity",
        cluster="lifestyle",
        clinical_focus=(
            "Low physical activity increases future metabolic, cardiovascular, and obesity risk across the "
            "workforce."
        ),
        poor_labels=["Less than 30mins", "Rarely or Never"],
        healthy_labels=["More than 60 mins", "30-60mins"],
        high_severity_labels=["Rarely or Never"],
        interventions=DiseaseInterventions(high=["movement_programme"], medium=["weight_management"]),
        modifiability=0.95,
        medical_frame_id="physical_activity",
    ),
    "sleep": LifestyleKnowledge(
        id="sleep",
        display_name="Sleep",
        cluster="recovery",
        clinical_focus="Insufficient or irregular sleep impairs recovery, cognitive performance, and metabolic regulation.",
        poor_labels=["Less than 5", "5-7", "More than 9"],
        healthy_labels=["7-9"],
        high_severity_labels=["Less than 5"],
        interventions=DiseaseInterventions(high=["sleep_health"], medium=["stress_management", "recovery_programme"]),
        modifiability=0.8,
        medical_frame_id="sleep",
    ),
    "nutrition": LifestyleKnowledge(
        id="nutrition",
        display_name="Nutrition",
        cluster="lifestyle",
        clinical_focus=(
            "Nutrition patterns are a primary modifiable lever for lipids, glucose regulation, and cardiovascular "
            "risk."
        ),
        poor_labels=[],
        healthy_labels=[],
        interventions=DiseaseInterventions(
            high=["nutrition_heart_healthy", "nutrition_whole_food"],
            medium=["nutrition_refined_carb", "nutrition_sodium"],
        ),
        modifiability=0.9,
        medical_frame_id="nutrition",
    ),
    "oxidative_stress": LifestyleKnowledge(
        id="oxidative_stress",
        display_name="Oxidative Stress",
        cluster="recovery",
        clinical_focus=(
            "Elevated oxidative stress indicates cellular strain linked to poor recovery, fatigue, and chronic "
            "disease risk."
        ),
        poor_labels=["High", "Very High", "high", "veryHigh"],
        healthy_labels=["Low", "low"],
        high_severity_labels=["Very High", "veryHigh"],
        interventions=DiseaseInterventions(
            high=["recovery_programme", "nutrition_whole_food"],
            medium=["sleep_health", "movement_programme", "stress_management"],
        ),
        modifiability=0.75,
        medical_frame_id="oxidative_stress",
    ),
    "overall_risk": LifestyleKnowledge(
        id="overall_risk",
        display_name="Overall Risk",
        cluster="metabolic",
        clinical_focus="Overall risk distribution summarises workforce metabolic and lifestyle vulnerability.",
        poor_labels=["Increased Risk", "High risk"],
        healthy_labels=["Optimal", "Low risk"],
        high_severity_labels=["High risk"],
        interventions=DiseaseInterventions(
            high=["target_high_risk", "scale_preventive_care"], medium=["maintain_wellness"]
        ),
        modifiability=0.7,
        medical_frame_id="overall_risk",
    ),
    "metabolic_age": LifestyleKnowledge(
        id="metabolic_age",
        display_name="Metabolic Age",
        cluster="metabolic",
        clinical_focus="Elevated metabolic age gaps indicate accelerated biological ageing relative to chronological age.",
        poor_labels=[],
        healthy_labels=[],
        interventions=DiseaseInterventions(
            high=["movement_programme", "nutrition_whole_food"], medium=["sleep_health", "weight_management"]
        ),
        modifiability=0.8,
        medical_frame_id="metabolic_age",
    ),
    "bmi_waist": LifestyleKnowledge(
        id="bmi_waist",
        display_name="BMI & Waist",
        cluster="metabolic",
        clinical_focus="Central adiposity and elevated BMI are upstream drivers of metabolic and cardiac disease.",
        poor_labels=[],
        healthy_labels=[],
        interventions=DiseaseInterventions(
            high=["weight_management", "movement_programme"], medium=["nutrition_whole_food"]
        ),
        modifiability=0.9,
        medical_frame_id="bmi_waist",
    ),
}


def get_lifestyle_knowledge(id: str) -> Optional[LifestyleKnowledge]:
    return LIFESTYLE_KNOWLEDGE.get(id)


def get_modifiability(metric_id: MetricId) -> float:
    knowledge = LIFESTYLE_KNOWLEDGE.get(metric_id)
    if knowledge is not None:
        return knowledge.modifiability
    # disease lookup deferred to knowledge index to avoid circular imports in consumers
    return 0.7


"""Faithful port of src/intelligence/knowledge/interventions.ts."""

INTERVENTIONS: Dict[InterventionId, InterventionKnowledge] = {
    "metabolic_screening": InterventionKnowledge(
        id="metabolic_screening",
        phrase="Include annual metabolic screening (HbA1c and fasting blood glucose) for at-risk employees",
        category="screening",
    ),
    "lipid_screening": InterventionKnowledge(
        id="lipid_screening",
        phrase="Incorporate lipid profile screening into routine preventive health assessments",
        category="screening",
    ),
    "bp_screening": InterventionKnowledge(
        id="bp_screening",
        phrase="Conduct regular blood pressure screening to support early detection",
        category="screening",
    ),
    "thyroid_screening": InterventionKnowledge(
        id="thyroid_screening",
        phrase="Include thyroid function testing (TSH) as part of routine health evaluations",
        category="screening",
    ),
    "liver_screening": InterventionKnowledge(
        id="liver_screening",
        phrase="Include liver function assessment during routine metabolic health screening",
        category="screening",
    ),
    "cardiac_screening": InterventionKnowledge(
        id="cardiac_screening",
        phrase="Perform comprehensive cardiovascular risk assessment for high-risk groups",
        category="screening",
    ),
    "nutrition_refined_carb": InterventionKnowledge(
        id="nutrition_refined_carb",
        phrase="Encourage dietary patterns that reduce refined carbohydrate intake",
        category="nutrition",
    ),
    "nutrition_heart_healthy": InterventionKnowledge(
        id="nutrition_heart_healthy",
        phrase="Promote heart-healthy eating habits rich in whole grains, fruits, and vegetables",
        category="nutrition",
    ),
    "nutrition_sodium": InterventionKnowledge(
        id="nutrition_sodium",
        phrase="Promote lower sodium intake through nutrition education and healthier food choices",
        category="nutrition",
    ),
    "nutrition_whole_food": InterventionKnowledge(
        id="nutrition_whole_food",
        phrase="Encourage balanced meals based on whole foods and appropriate portion sizes",
        category="nutrition",
    ),
    "movement_programme": InterventionKnowledge(
        id="movement_programme",
        phrase="Promote regular physical activity through daily movement and active breaks",
        category="activity",
    ),
    "weight_management": InterventionKnowledge(
        id="weight_management",
        phrase="Support healthy weight management through nutrition, activity, and lifestyle coaching",
        category="weight",
    ),
    "sleep_health": InterventionKnowledge(
        id="sleep_health",
        phrase="Promote healthy sleep habits to improve recovery, wellbeing, and overall health",
        category="sleep",
    ),
    "stress_management": InterventionKnowledge(
        id="stress_management",
        phrase="Provide stress management resources and encourage healthy work-life balance",
        category="stress",
    ),
    "recovery_programme": InterventionKnowledge(
        id="recovery_programme",
        phrase="Support recovery through programmes focused on sleep, stress reduction, and healthy daily habits",
        category="recovery",
    ),
    "womens_health": InterventionKnowledge(
        id="womens_health",
        phrase="Strengthen women's health awareness through education, screening, and confidential support services",
        category="womens_health",
    ),
    "smoking_cessation": InterventionKnowledge(
        id="smoking_cessation",
        phrase="Provide evidence-based smoking cessation support and counselling",
        category="lifestyle",
    ),
    "alcohol_moderation": InterventionKnowledge(
        id="alcohol_moderation",
        phrase="Promote responsible alcohol consumption through education and counselling",
        category="lifestyle",
    ),
    "maintain_wellness": InterventionKnowledge(
        id="maintain_wellness",
        phrase="Continue preventive wellness initiatives to maintain current health outcomes",
        category="maintain",
    ),
    "target_high_risk": InterventionKnowledge(
        id="target_high_risk",
        phrase="Provide targeted health coaching and regular follow-up for employees at higher health risk",
        category="strategy",
    ),
    "scale_preventive_care": InterventionKnowledge(
        id="scale_preventive_care",
        phrase="Expand preventive health programmes through integrated clinical and lifestyle support",
        category="strategy",
    ),
    "clinical_review": InterventionKnowledge(
        id="clinical_review",
        phrase="Recommend clinical evaluation for employees with persistent or significant health concerns",
        category="clinical",
    ),
}


def intervention_phrase(id: InterventionId) -> str:
    knowledge = INTERVENTIONS.get(id)
    return knowledge.phrase if knowledge else "Prioritize targeted preventive interventions"


"""Concise medical frames — one line of clinical meaning per id.

Faithful port of src/intelligence/knowledge/medicalFrames.ts.
"""

MEDICAL_FRAMES: Dict[str, str] = {
    "overall_risk": "Overall risk distribution reflects the workforce's current metabolic health and future lifestyle-related disease risk.",
    "overall_risk_healthy": "A predominantly healthy risk profile reflects good overall health and effective preventive practices.",

    "type_2_diabetes": (
        "Rising diabetes risk suggests worsening blood sugar control and a greater likelihood of long-term metabolic complications."
    ),

    "hypertension": (
        "Increasing blood-pressure risk suggests a higher likelihood of cardiovascular disease, stroke, and related health complications."
    ),

    "obesity": (
        "Higher obesity levels indicate increasing weight-related health risk and a greater chance of developing chronic metabolic conditions."
    ),

    "pcos_pcod": (
        "Higher PCOS indicators highlight the need for greater attention to hormonal health and metabolic wellbeing among female employees."
    ),

    "nafld": (
        "Increasing fatty-liver risk may reflect early metabolic changes associated with excess weight and poor glucose regulation."
    ),

    "cardiac_health": (
        "Rising cardiac-risk indicators suggest multiple cardiovascular risk factors may be developing together across the workforce."
    ),

    "thyroid_health": (
        "Higher thyroid-related risk may contribute to fatigue, reduced energy, and changes in metabolic function."
    ),

    "dyslipidemia": (
        "Increasing dyslipidemia reflects unhealthy cholesterol patterns that can raise long-term cardiovascular risk."
    ),

    "metabolic_syndrome": (
        "Higher metabolic-syndrome prevalence suggests several metabolic risk factors are occurring together, increasing future chronic disease risk."
    ),

    "physical_activity": (
        "Low physical activity is associated with poorer metabolic health and a higher risk of obesity, diabetes, and cardiovascular disease."
    ),

    "physical_activity_healthy": (
        "Regular physical activity supports healthy metabolism, cardiovascular fitness, and overall wellbeing."
    ),

    "sleep": (
        "Poor or irregular sleep can affect recovery, cognitive performance, hormone balance, and metabolic health."
    ),

    "sleep_healthy": (
        "Healthy sleep habits support physical recovery, mental performance, and long-term wellbeing."
    ),

    "nutrition": (
        "Healthy nutrition plays an important role in maintaining blood sugar, cholesterol levels, and cardiovascular health."
    ),

    "nutrition_healthy": (
        "Balanced eating habits help maintain healthy metabolism and reduce cardiovascular risk."
    ),

    "oxidative_stress": (
        "Higher oxidative stress reflects increased cellular damage that may contribute to fatigue, inflammation, and chronic disease."
    ),

    "oxidative_stress_healthy": (
        "Lower oxidative stress supports healthy cellular function, recovery, and overall wellbeing."
    ),

    "recovery_strain": (
        "Poor sleep together with elevated oxidative stress suggests reduced recovery and increased physical strain."
    ),

    "cardio_nutrition": (
        "Dietary patterns contributing to unhealthy lipid levels may increase overall cardiovascular risk."
    ),

    "movement_priority": (
        "Low physical activity together with excess weight highlights the importance of increasing daily movement."
    ),

    "metabolic_cluster": (
        "Several metabolic risk factors are present together, suggesting a broader pattern rather than isolated health concerns."
    ),

    "cardio_cluster": (
        "Blood pressure, lipid abnormalities, and cardiac indicators together suggest an increased cardiovascular health priority."
    ),

    "workforce_resilience": (
        "Healthy physical activity and sleep habits provide a strong foundation for long-term health and recovery."
    ),

    "positive_wins": (
        "Healthy risk profiles across several areas highlight strengths that should be maintained through ongoing preventive care."
    ),

    "metabolic_age": (
        "A higher metabolic age than chronological age may indicate declining metabolic health and increased future disease risk."
    ),

    "bmi_waist": (
        "Higher BMI and waist circumference are associated with increased metabolic and cardiovascular health risk."
    ),

    "participation": (
        "Participation levels reflect the reach and engagement of the workforce health assessment."
    ),

    "maintain": (
        "Current health patterns support continuing existing preventive initiatives while maintaining regular monitoring."
    ),

    "disease_generic": (
        "Higher disease risk highlights the importance of preventive screening, healthy lifestyle practices, and appropriate follow-up."
    ),
}

def medical_frame(id: str) -> str:
    return MEDICAL_FRAMES.get(id, MEDICAL_FRAMES["disease_generic"])


def frame_id_for_metric(metric_id: MetricId, healthy: bool) -> str:
    if healthy:
        healthy_key = f"{metric_id}_healthy"
        if healthy_key in MEDICAL_FRAMES:
            return healthy_key
    return metric_id if metric_id in MEDICAL_FRAMES else "disease_generic"


"""
Health Relationship Graph — static edge definitions.
Runtime evaluates one-hop activations against MetricScores.
Extracted and compressed from the Medical Knowledge Layer cross-links.

Faithful port of src/intelligence/knowledge/graphEdges.ts — ALL 26 edges.
"""

GRAPH_EDGES: List[HealthGraphEdge] = [
    # Recovery
    HealthGraphEdge(
        id="sleep_oxidative_recovery",
        from_="sleep",
        to="oxidative_stress",
        type="reinforces",
        weight=0.9,
        min_confidence="high",
        primary_lever="recovery_programme",
        effect_id="recovery_strain",
    ),
    HealthGraphEdge(
        id="oxidative_sleep_recovery",
        from_="oxidative_stress",
        to="sleep",
        type="reinforces",
        weight=0.85,
        min_confidence="high",
        primary_lever="recovery_programme",
        effect_id="recovery_strain",
    ),
    # Movement / obesity
    HealthGraphEdge(
        id="activity_obesity_movement",
        from_="physical_activity",
        to="obesity",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="movement_priority",
    ),
    HealthGraphEdge(
        id="obesity_activity_movement",
        from_="obesity",
        to="physical_activity",
        type="drives",
        weight=0.9,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="movement_priority",
    ),
    HealthGraphEdge(
        id="activity_diabetes",
        from_="physical_activity",
        to="type_2_diabetes",
        type="drives",
        weight=0.85,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="movement_priority",
    ),
    # Cardio-nutrition
    HealthGraphEdge(
        id="nutrition_dyslipidemia",
        from_="nutrition",
        to="dyslipidemia",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="nutrition_heart_healthy",
        effect_id="cardio_nutrition",
    ),
    HealthGraphEdge(
        id="dyslipidemia_nutrition",
        from_="dyslipidemia",
        to="nutrition",
        type="reinforces",
        weight=0.9,
        min_confidence="high",
        primary_lever="nutrition_heart_healthy",
        effect_id="cardio_nutrition",
    ),
    HealthGraphEdge(
        id="nutrition_diabetes",
        from_="nutrition",
        to="type_2_diabetes",
        type="drives",
        weight=0.85,
        min_confidence="high",
        primary_lever="nutrition_refined_carb",
        effect_id="cardio_nutrition",
    ),
    # Metabolic cluster
    HealthGraphEdge(
        id="obesity_diabetes",
        from_="obesity",
        to="type_2_diabetes",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="weight_management",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="obesity_nafld",
        from_="obesity",
        to="nafld",
        type="drives",
        weight=0.9,
        min_confidence="high",
        primary_lever="weight_management",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="diabetes_dyslipidemia",
        from_="type_2_diabetes",
        to="dyslipidemia",
        type="clusters_with",
        weight=0.85,
        min_confidence="high",
        primary_lever="metabolic_screening",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="mets_obesity",
        from_="metabolic_syndrome",
        to="obesity",
        type="clusters_with",
        weight=0.95,
        min_confidence="high",
        primary_lever="movement_programme",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="mets_diabetes",
        from_="metabolic_syndrome",
        to="type_2_diabetes",
        type="clusters_with",
        weight=0.95,
        min_confidence="high",
        primary_lever="metabolic_screening",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="nafld_diabetes",
        from_="nafld",
        to="type_2_diabetes",
        type="reinforces",
        weight=0.8,
        min_confidence="high",
        primary_lever="nutrition_refined_carb",
        effect_id="metabolic_cluster",
    ),
    # Cardio cluster
    HealthGraphEdge(
        id="dyslipidemia_cardiac",
        from_="dyslipidemia",
        to="cardiac_health",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="lipid_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="hypertension_cardiac",
        from_="hypertension",
        to="cardiac_health",
        type="drives",
        weight=0.95,
        min_confidence="high",
        primary_lever="bp_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="hypertension_dyslipidemia",
        from_="hypertension",
        to="dyslipidemia",
        type="clusters_with",
        weight=0.85,
        min_confidence="high",
        primary_lever="cardiac_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="diabetes_cardiac",
        from_="type_2_diabetes",
        to="cardiac_health",
        type="drives",
        weight=0.9,
        min_confidence="high",
        primary_lever="cardiac_screening",
        effect_id="cardio_cluster",
    ),
    HealthGraphEdge(
        id="mets_hypertension",
        from_="metabolic_syndrome",
        to="hypertension",
        type="clusters_with",
        weight=0.85,
        min_confidence="high",
        primary_lever="scale_preventive_care",
        effect_id="cardio_cluster",
    ),
    # Sleep / hypertension
    HealthGraphEdge(
        id="sleep_hypertension",
        from_="sleep",
        to="hypertension",
        type="reinforces",
        weight=0.7,
        min_confidence="moderate",
        primary_lever="sleep_health",
        effect_id="recovery_strain",
    ),
    # Protective
    HealthGraphEdge(
        id="activity_sleep_resilience",
        from_="physical_activity",
        to="sleep",
        type="protects",
        weight=0.8,
        min_confidence="moderate",
        primary_lever="maintain_wellness",
        effect_id="workforce_resilience",
    ),
    HealthGraphEdge(
        id="sleep_activity_resilience",
        from_="sleep",
        to="physical_activity",
        type="protects",
        weight=0.8,
        min_confidence="moderate",
        primary_lever="maintain_wellness",
        effect_id="workforce_resilience",
    ),
    # Oxidative / cardiac
    HealthGraphEdge(
        id="oxidative_cardiac",
        from_="oxidative_stress",
        to="cardiac_health",
        type="indicates",
        weight=0.65,
        min_confidence="moderate",
        primary_lever="recovery_programme",
        effect_id="recovery_strain",
    ),
    # PCOS metabolic
    HealthGraphEdge(
        id="pcos_diabetes",
        from_="pcos_pcod",
        to="type_2_diabetes",
        type="drives",
        weight=0.8,
        min_confidence="high",
        primary_lever="nutrition_refined_carb",
        effect_id="metabolic_cluster",
    ),
    HealthGraphEdge(
        id="pcos_obesity",
        from_="pcos_pcod",
        to="obesity",
        type="reinforces",
        weight=0.75,
        min_confidence="high",
        primary_lever="womens_health",
        effect_id="metabolic_cluster",
    ),
]


"""Runtime guardrails — organisational wellness insights only.

Faithful port of src/intelligence/knowledge/guardrails.ts.
"""

GUARDRAILS: Dict[str, bool] = {
    "noPrescriptionMedications": True,
    "noIndividualDiagnosis": True,
    "noTreatmentPlans": True,
    "clinicalReviewIsGenericOnly": True,
}

DISCLAIMER = "Insights are organisational wellness signals, not individual medical diagnoses or treatment advice."


def get_modifiability(metric_id: MetricId) -> float:
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return lifestyle.modifiability
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return disease.modifiability
    return 0.7


_DISPLAY_NAME_FALLBACKS: Dict[str, str] = {
    "overall_risk": "Overall risk",
    "physical_activity": "Physical activity",
    "sleep": "Sleep",
    "nutrition": "Nutrition",
    "oxidative_stress": "Oxidative stress",
    "metabolic_age": "Metabolic age",
    "bmi_waist": "BMI and waist",
    "positive_wins": "Positive health wins",
    "participation": "Participation",
}


def get_display_name(metric_id: MetricId) -> str:
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return disease.display_name
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return lifestyle.display_name
    # Never return snake_case ids to callers
    return _DISPLAY_NAME_FALLBACKS.get(metric_id, "This health indicator")


class _LeverBundle(dict):
    """Dict that also supports attribute access (``.high``/``.medium``), so
    callers can use either ``get_default_levers(id)["high"]`` or
    ``get_default_levers(id).high`` — mirrors TS's plain ``{ high, medium }``
    object, which supports both styles from calling code perspectives."""

    def __getattr__(self, item: str):
        try:
            return self[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc


def get_default_levers(metric_id: MetricId) -> Dict[str, list]:
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return _LeverBundle(high=disease.interventions.high, medium=disease.interventions.medium)
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return _LeverBundle(high=lifestyle.interventions.high, medium=lifestyle.interventions.medium)
    return _LeverBundle(high=["target_high_risk"], medium=["maintain_wellness"])


# ===========================================================================
# Narrative phrase libraries (generation assets) — from medical_knowledge_layer.md
# Added for the narrative content-quality upgrade. Reasoning/scoring untouched.
# ===========================================================================

"""Curated narrative phrase libraries — ported verbatim from
medical_knowledge_layer.md (sections 10-16). These are *generation assets*:
the reasoning engine selects from them deterministically; it never invents new
clinical facts. Every phrase here already exists, and is owned by, the Medical
Knowledge Layer. Interpretation clauses are stored clause-shaped (no leading
ellipsis, no trailing period) so they join after an observation with a comma,
exactly like SHORT_WHY."""


DISEASE_INTERPRETATION_PHRASES: Dict[str, List[str]] = {
"type_2_diabetes": [
        "showing early signs of declining metabolic health across the workforce",
        "suggesting reduced insulin sensitivity in a growing number of employees",
        "highlighting an increased likelihood of future cardiovascular complications",
        "reflecting lifestyle patterns that may gradually impair blood sugar control",
        "reinforcing the value of early preventive measures before disease develops",
        "suggesting that untreated metabolic risk may increase future healthcare needs",
        "highlighting the importance of improving overall metabolic health",
        "consistent with an early stage where lifestyle changes can still make a significant impact",
        "emphasising the close relationship between physical activity and glucose regulation",
        "indicating that healthy weight management and balanced nutrition remain the most effective preventive strategies",
],
"hypertension": [
    "showing a gradual increase in cardiovascular and stroke risk",
    "suggesting persistent blood pressure changes across the workforce",
    "highlighting the close relationship between stress levels and blood pressure",
    "reflecting lifestyle patterns that may contribute to long-term cardiovascular disease",
    "reinforcing the importance of regular blood pressure screening",
    "suggesting that prolonged work-related stress may influence vascular health",
    "highlighting the need to strengthen cardiovascular health initiatives",
    "consistent with an early stage where timely intervention can improve outcomes",
    "emphasising the importance of maintaining healthy weight, activity, and sodium intake",
    "suggesting that uncontrolled blood pressure may gradually affect kidney function",
],

"obesity": [
    "showing increasing weight-related health concerns across the workforce",
    "suggesting a gradual rise in metabolic risk across multiple health areas",
    "highlighting the relationship between physical activity and healthy body composition",
    "reflecting lifestyle habits that may contribute to long-term health complications",
    "reinforcing the importance of healthy weight management as a preventive priority",
    "suggesting a greater likelihood of future diabetes and cardiovascular disease",
    "highlighting body weight as an important indicator of overall metabolic health",
    "consistent with a condition that responds well to sustained lifestyle improvements",
    "emphasising nutrition and regular physical activity as key preventive measures",
    "suggesting excess body weight may also influence liver, joint, and overall metabolic health",
],

"pcos_pcod": [
    "showing the close relationship between hormonal balance and metabolic health",
    "suggesting insulin resistance may contribute to hormonal changes in affected women",
    "highlighting the increased long-term risk of diabetes and cardiovascular disease",
    "reflecting a common condition that often benefits from early lifestyle management",
    "reinforcing the importance of dedicated women's health awareness and support",
    "suggesting nutrition and regular physical activity play an important role in management",
    "highlighting the need for greater focus on women's metabolic wellbeing",
    "consistent with a condition that often improves through sustained lifestyle changes",
    "emphasising the value of confidential and accessible women's health services",
    "suggesting that untreated metabolic changes may increase future health risks",
],

"nafld": [
    "showing early metabolic changes that may affect liver health",
    "suggesting insulin resistance may be contributing to fatty liver changes",
    "highlighting the increased likelihood of future diabetes and cardiovascular disease",
    "reflecting a stage of liver disease where lifestyle changes can still be highly effective",
    "reinforcing the importance of early identification and intervention",
    "suggesting excess body weight and poor dietary habits are important contributing factors",
    "highlighting liver health as an important part of overall metabolic wellbeing",
    "consistent with a condition that often improves through healthy lifestyle modifications",
    "emphasising balanced nutrition as a key component of liver health",
    "suggesting that untreated fatty liver disease may gradually progress over time",
],

"cardiac_health": [
    "showing increasing cardiovascular risk across multiple health indicators",
    "suggesting several contributing factors may be increasing the likelihood of future cardiac events",
    "highlighting the long-term risk of heart attack and stroke",
    "reflecting the combined influence of cholesterol, blood pressure, weight, and lifestyle",
    "reinforcing the importance of comprehensive cardiovascular prevention",
    "suggesting future healthcare needs may increase if cardiovascular risk remains unmanaged",
    "highlighting heart health as a major focus for preventive care",
    "consistent with a condition that responds well to early lifestyle improvement",
    "emphasising nutrition, physical activity, and routine screening as key preventive measures",
    "suggesting that addressing multiple cardiovascular risk factors together provides greater long-term benefit",
],

"thyroid_health": [
    "showing changes that may contribute to fatigue and reduced energy levels",
    "suggesting thyroid dysfunction may be affecting overall wellbeing",
    "highlighting a condition that can be identified through routine screening",
    "reflecting a common endocrine disorder that may influence metabolism",
    "reinforcing the importance of early thyroid function assessment",
    "suggesting women may experience a higher burden of thyroid-related conditions",
    "highlighting the value of including thyroid health in routine wellness programmes",
    "consistent with a medical condition that often responds well to appropriate treatment",
    "emphasising routine thyroid screening for employees with persistent symptoms",
    "suggesting thyroid dysfunction may also influence cholesterol levels and metabolic health",
],

"dyslipidemia": [
    "showing increasing cholesterol-related cardiovascular risk",
    "suggesting unhealthy lipid levels may be becoming more common across the workforce",
    "highlighting the long-term risk of heart attack and stroke",
    "reflecting dietary and lifestyle habits that influence cholesterol balance",
    "reinforcing the importance of routine lipid screening and prevention",
    "suggesting nutrition remains one of the most effective ways to improve lipid health",
    "highlighting cardiovascular health as an important preventive priority",
    "consistent with an early stage where lifestyle improvement can produce measurable benefits",
    "emphasising elevated triglycerides as an important metabolic health indicator",
    "suggesting that improving cholesterol levels can significantly reduce cardiovascular risk",
],

"metabolic_syndrome": [
    "showing several metabolic risk factors occurring together across the workforce",
    "suggesting multiple health risks are developing simultaneously rather than independently",
    "highlighting a substantially higher likelihood of future diabetes and cardiovascular disease",
    "reflecting the combined effects of body weight, blood sugar, blood pressure, and lipid imbalance",
    "reinforcing the importance of comprehensive lifestyle-based prevention",
    "suggesting overall metabolic health may be gradually declining",
    "highlighting metabolic health as a key priority for long-term disease prevention",
    "consistent with a condition that responds well to coordinated lifestyle improvements",
    "emphasising nutrition and physical activity as the foundation of risk reduction",
    "suggesting that managing all metabolic risk factors together leads to better long-term health outcomes",
],
}

DISEASE_POSITIVE_NARRATIVES: Dict[str, List[str]] = {
    "type_2_diabetes": [
        "Blood sugar regulation remains stable across most of the workforce.",
        "Metabolic health appears well maintained, with a low overall diabetes risk.",
        "Current findings suggest healthy glucose regulation among most employees.",
        "Blood sugar markers are generally within expected healthy ranges.",
        "Healthy lifestyle habits appear to be supporting metabolic wellbeing.",
        "The workforce shows a favourable metabolic health profile.",
        "Diabetes risk remains low across the assessed population.",
        "Early metabolic indicators reflect good preventive health practices.",
        "Balanced nutrition and regular activity appear to support healthy glucose control.",
        "Current metabolic health provides a strong foundation for long-term wellbeing.",
    ],

    "hypertension": [
        "Blood pressure remains well managed across most of the workforce.",
        "Cardiovascular health indicators are generally within healthy ranges.",
        "Current findings suggest a low overall risk of hypertension.",
        "Healthy lifestyle habits appear to support normal blood pressure levels.",
        "Hypertension is not a major health concern within this cohort.",
        "The workforce demonstrates good overall cardiovascular health.",
        "Blood pressure measurements are reassuring across the assessed population.",
        "Preventive health practices appear to support healthy vascular function.",
        "Heart health remains a positive aspect of the workforce profile.",
        "Blood pressure risk remains consistently low across the cohort.",
    ],

    "obesity": [
        "Body weight remains within healthy ranges for most employees.",
        "Healthy body composition reflects positive lifestyle habits.",
        "Current findings indicate a low overall burden of obesity.",
        "Weight-related health indicators are favourable across the workforce.",
        "Regular physical activity and balanced nutrition appear to support healthy body weight.",
        "The workforce demonstrates good overall physical health.",
        "Obesity is not a major concern within this cohort.",
        "Body composition measurements are reassuring across the assessed population.",
        "Healthy weight management is a positive feature of the workforce profile.",
        "Weight-related health risk remains consistently low.",
    ],

    "pcos_pcod": [
        "Hormonal and metabolic health appears stable among female employees.",
        "PCOS-related risk remains low within the assessed female population.",
        "Women's metabolic health indicators are generally reassuring.",
        "Insulin sensitivity appears favourable across most female employees.",
        "Healthy lifestyle patterns may be supporting hormonal wellbeing.",
        "Women's health remains a positive aspect of the workforce profile.",
        "PCOS-related concerns are limited within this cohort.",
        "Female metabolic markers are largely within expected healthy ranges.",
        "Current findings support good hormonal health among most women assessed.",
        "Overall metabolic wellbeing among female employees appears well maintained.",
    ],

    "nafld": [
        "Liver health appears healthy across most of the workforce.",
        "Fatty liver risk remains low based on current findings.",
        "Current metabolic patterns appear supportive of healthy liver function.",
        "Liver-related biomarkers are generally within expected ranges.",
        "Balanced nutrition and physical activity appear to support liver health.",
        "The workforce demonstrates good overall liver and metabolic health.",
        "Fatty liver disease is not a major concern within this cohort.",
        "Liver function markers are reassuring across the assessed population.",
        "Healthy metabolic habits appear to be protecting liver function.",
        "Current liver health provides a positive foundation for long-term wellbeing.",
    ],

    "cardiac_health": [
        "Heart health remains favourable across most of the workforce.",
        "Cardiovascular risk indicators are generally within healthy ranges.",
        "Current findings suggest a low overall risk of heart disease.",
        "Healthy lifestyle habits appear to support cardiovascular wellbeing.",
        "Cardiac health is not a major concern within this cohort.",
        "The workforce demonstrates good overall cardiovascular fitness.",
        "Heart-health markers are reassuring across the assessed population.",
        "Preventive health practices appear to support long-term cardiovascular health.",
        "Cardiovascular wellbeing remains a positive feature of the workforce profile.",
        "Heart disease risk remains consistently low across the cohort.",
    ],

    "thyroid_health": [
        "Thyroid function appears stable across most of the workforce.",
        "Current findings suggest a low overall risk of thyroid disorders.",
        "Thyroid-related biomarkers are generally within healthy ranges.",
        "Endocrine health appears well maintained across the assessed population.",
        "Thyroid-related concerns are limited within this cohort.",
        "Routine screening suggests healthy thyroid function in most employees.",
        "Energy-related symptoms linked to thyroid disease are likely to be uncommon.",
        "Thyroid health remains reassuring across the workforce.",
        "Overall endocrine health represents a positive aspect of the workforce profile.",
        "Current thyroid status supports healthy metabolic function.",
    ],

    "dyslipidemia": [
        "Cholesterol levels remain healthy across most of the workforce.",
        "Lipid markers are generally within recommended ranges.",
        "Current findings suggest a low overall risk of dyslipidemia.",
        "Healthy nutrition and activity patterns appear to support lipid balance.",
        "Abnormal cholesterol levels are not a major concern within this cohort.",
        "The workforce demonstrates good overall lipid health.",
        "Cholesterol and triglyceride measurements are reassuring.",
        "Preventive lifestyle habits appear to support healthy lipid levels.",
        "Lipid health remains a positive aspect of the workforce profile.",
        "Blood lipid risk remains consistently low across the assessed population.",
    ],

    "metabolic_syndrome": [
        "Overall metabolic health appears favourable across the workforce.",
        "Combined metabolic risk remains low based on current findings.",
        "The workforce demonstrates healthy patterns across multiple metabolic indicators.",
        "Blood sugar, blood pressure, and lipid markers are generally well controlled.",
        "Healthy nutrition and regular physical activity appear to support metabolic wellbeing.",
        "Metabolic syndrome is not a major concern within this cohort.",
        "Current metabolic markers are reassuring across the assessed population.",
        "Preventive health practices appear to be supporting long-term metabolic health.",
        "Overall metabolic wellbeing remains a positive feature of the workforce profile.",
        "Current findings suggest a strong foundation for maintaining long-term metabolic health.",
    ],
}

DISEASE_LEADERSHIP_TAKEAWAYS: Dict[str, List[str]] = {
    "type_2_diabetes": [
        "If resources are limited, prioritising healthier nutrition and regular physical activity will have the greatest impact on reducing future diabetes risk.",
        "Improving metabolic health today can reduce future healthcare costs and long-term disease burden.",
        "The early stages of diabetes provide an important opportunity where timely intervention can prevent disease progression.",
        "Routine metabolic screening combined with targeted lifestyle support offers long-term health and economic benefits.",
        "Addressing body weight, nutrition, and physical activity together is more effective than focusing on a single factor.",
    ],

    "hypertension": [
        "If resources are limited, reducing sodium intake, improving physical activity, and managing stress should be prioritised.",
        "Routine blood pressure screening allows early detection before complications develop.",
        "Workplace stress and unhealthy lifestyle habits can both contribute to rising blood pressure and should be addressed together.",
        "Healthy weight management and regular exercise remain two of the most effective strategies for reducing hypertension risk.",
        "Early preventive action helps reduce the future burden of cardiovascular and kidney disease.",
    ],

    "obesity": [
        "If resources are limited, investing in healthier eating habits and regular physical activity will provide the greatest long-term benefit.",
        "Reducing obesity can improve several related health conditions, including diabetes, heart disease, and fatty liver disease.",
        "Maintaining a healthy body weight benefits multiple aspects of metabolic and cardiovascular health.",
        "Sustainable improvements are more likely when healthy food choices and opportunities for physical activity are easily accessible.",
        "Consistent daily movement is generally more effective than short-term intensive programmes for maintaining long-term weight control.",
    ],

    "pcos_pcod": [
        "If resources are limited, nutrition programmes that improve insulin sensitivity can provide meaningful benefits for women with PCOS.",
        "Increasing awareness and access to women's health services can improve early identification and long-term management.",
        "Improving insulin resistance may also reduce the future risk of diabetes in affected women.",
        "Inclusive nutrition and physical activity programmes can support both hormonal and metabolic health.",
        "Confidential and accessible women's health support encourages earlier engagement and better health outcomes.",
    ],

    "nafld": [
        "If resources are limited, reducing excess sugar intake and supporting healthy weight loss should be prioritised.",
        "Early identification of fatty liver disease creates an opportunity to prevent future metabolic complications.",
        "Fatty liver disease often reflects broader metabolic health concerns that should be addressed comprehensively.",
        "Even modest and sustained weight loss can significantly improve liver health.",
        "Routine liver health screening helps identify individuals who may benefit from early intervention.",
    ],

    "cardiac_health": [
        "If resources are limited, improving nutrition, physical activity, and cholesterol management will provide the greatest reduction in cardiovascular risk.",
        "Cardiovascular health should remain a priority because it has a major impact on long-term health outcomes and healthcare costs.",
        "Managing body weight, blood pressure, cholesterol, and blood sugar together provides greater benefit than addressing each factor separately.",
        "Routine cardiovascular screening supports earlier identification of employees at increased risk.",
        "A comprehensive prevention strategy is more effective than focusing on individual cardiovascular risk factors in isolation.",
    ],

    "thyroid_health": [
        "If resources are limited, incorporating thyroid function testing into routine health assessments can improve early detection.",
        "Thyroid disorders are often identified through screening rather than lifestyle assessment alone.",
        "Recognising thyroid dysfunction early can help distinguish medical conditions from fatigue or work-related stress.",
        "Because thyroid disorders are more common in women, awareness programmes can complement broader women's health initiatives.",
        "Timely diagnosis and treatment can significantly improve energy levels, wellbeing, and daily functioning.",
    ],

    "dyslipidemia": [
        "If resources are limited, promoting heart-healthy nutrition should be a key strategy for improving lipid health.",
        "Managing cholesterol levels provides a measurable way to reduce long-term cardiovascular risk.",
        "Improvements in nutrition and physical activity can lead to meaningful changes in lipid profiles over time.",
        "Raised triglycerides should be addressed alongside cholesterol as part of overall cardiovascular risk reduction.",
        "Combining routine lipid screening with lifestyle support enables earlier intervention and better long-term outcomes.",
    ],

    "metabolic_syndrome": [
        "If resources are limited, integrated programmes that improve nutrition, physical activity, and healthy weight will benefit multiple metabolic risk factors simultaneously.",
        "Metabolic syndrome provides a comprehensive picture of overall metabolic health rather than a single disease.",
        "Addressing risk factors early helps prevent progression to diabetes, cardiovascular disease, and other chronic conditions.",
        "The same core lifestyle interventions improve blood sugar, blood pressure, cholesterol, and body weight together.",
        "A coordinated prevention strategy focused on metabolic health can deliver broad and sustainable health benefits across the workforce.",
    ],
}

DISEASE_SEVERITY_LANGUAGE: Dict[str, Dict[str, str]] = {
    "type_2_diabetes": {
        "very_low": "currently a low health concern",
        "low": "should continue to be monitored through routine metabolic screening",
        "moderate": "requires targeted lifestyle interventions to reduce future diabetes risk",
        "high": "should become a key focus of preventive health programmes",
        "very_high": "requires immediate action to reduce the growing burden of diabetes and its complications",
    },

    "hypertension": {
        "very_low": "currently a low cardiovascular concern",
        "low": "should be monitored through regular blood pressure assessments",
        "moderate": "requires targeted action to improve blood pressure control",
        "high": "should become a priority for cardiovascular risk reduction",
        "very_high": "requires immediate intervention to reduce the risk of cardiovascular and kidney complications",
    },

    "obesity": {
        "very_low": "currently a low weight-related health concern",
        "low": "should continue to be monitored through routine health assessments",
        "moderate": "requires focused support for healthy weight management",
        "high": "should become a major focus of workplace health initiatives",
        "very_high": "requires immediate action as it contributes to multiple chronic health conditions",
    },

    "pcos_pcod": {
        "very_low": "currently a low concern within the female workforce",
        "low": "should remain part of routine women's health awareness",
        "moderate": "requires targeted education and preventive support for affected women",
        "high": "should become an important women's health priority",
        "very_high": "requires dedicated clinical support and early intervention programmes",
    },

    "nafld": {
        "very_low": "currently a low liver health concern",
        "low": "should continue to be monitored during routine metabolic assessments",
        "moderate": "requires targeted interventions to improve liver and metabolic health",
        "high": "should become an important component of metabolic disease prevention",
        "very_high": "requires immediate action to prevent progression of liver and metabolic disease",
    },

    "cardiac_health": {
        "very_low": "currently a low cardiovascular concern",
        "low": "should continue to be monitored through preventive screening",
        "moderate": "requires focused action to reduce cardiovascular risk",
        "high": "should become a leading priority within workforce health programmes",
        "very_high": "requires immediate and comprehensive action to reduce the risk of serious cardiovascular events",
    },

    "thyroid_health": {
        "very_low": "currently a low endocrine health concern",
        "low": "should remain part of routine thyroid screening",
        "moderate": "requires increased awareness and timely thyroid function assessment",
        "high": "should prompt regular screening and appropriate clinical evaluation",
        "very_high": "requires immediate clinical assessment and follow-up for affected employees",
    },

    "dyslipidemia": {
        "very_low": "currently a low lipid-related health concern",
        "low": "should continue to be monitored through routine lipid assessments",
        "moderate": "requires targeted interventions to improve cholesterol and triglyceride levels",
        "high": "should become an important focus for cardiovascular disease prevention",
        "very_high": "requires immediate action to reduce long-term cardiovascular risk",
    },

    "metabolic_syndrome": {
        "very_low": "currently a low overall metabolic concern",
        "low": "should continue to be monitored through regular metabolic assessments",
        "moderate": "requires coordinated lifestyle interventions addressing multiple risk factors",
        "high": "should become a major priority for preventive health programmes",
        "very_high": "requires immediate, coordinated action to reduce the combined burden of metabolic disease",
    },
}

def select_variant(variants: Sequence[str], *key_parts: object) -> str:
    """Deterministically pick one phrase from `variants` using a stable hash of
    `key_parts`. Stable across processes/runs (uses hashlib, never the salted
    builtin hash()). Empty/one-element pools return safely. The same key always
    yields the same phrase — selection depends only on the caller-supplied key
    (metric, severity, section, profile token, role)."""
    pool = [v for v in variants if v and v.strip()]
    if not pool:
        return ""
    if len(pool) == 1:
        return pool[0]
    key = "|".join("" if p is None else str(p) for p in key_parts)
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return pool[int(digest, 16) % len(pool)]

# Same organisational action, varied medically-authored phrasing. The action
# (and therefore the reasoning/lever) is identical across a lever's variants —
# only the surface verb/wording changes, so no recommendation ever shifts to a
# different intervention. Verbs drawn from the Medical Knowledge Layer register.
LEVER_ACTION_VARIANTS: Dict[str, List[str]] = {
    "metabolic_screening": [
        "Include annual metabolic screening for employees at increased risk",
        "Offer routine HbA1c and fasting blood glucose screening as part of preventive care",
        "Integrate annual metabolic assessments into regular health check-ups",
    ],

    "lipid_screening": [
        "Include routine lipid profile testing alongside heart-health initiatives",
        "Incorporate lipid screening into regular preventive health assessments",
        "Make lipid profile evaluation a routine component of cardiovascular prevention",
    ],

    "bp_screening": [
        "Encourage regular blood pressure screening with appropriate follow-up",
        "Include routine blood pressure assessment during preventive health checks",
        "Support early identification through regular blood pressure monitoring",
    ],

    "thyroid_screening": [
        "Include thyroid function testing in routine preventive health assessments",
        "Offer thyroid screening for employees with symptoms or elevated risk",
        "Incorporate TSH testing into standard health check programmes",
    ],

    "liver_screening": [
        "Include liver function tests during routine metabolic health assessments",
        "Monitor liver health alongside metabolic screening programmes",
        "Incorporate liver function evaluation into preventive health check-ups",
    ],

    "cardiac_screening": [
        "Provide comprehensive cardiovascular screening for higher-risk employees",
        "Include cardiovascular risk assessment in routine preventive health programmes",
        "Strengthen preventive care through regular heart health screening",
    ],

    "nutrition_refined_carb": [
        "Encourage reduced intake of refined carbohydrates through nutrition education",
        "Promote healthier food choices by limiting refined carbohydrate consumption",
        "Support balanced eating habits that reduce refined carbohydrate intake",
    ],

    "nutrition_heart_healthy": [
        "Promote heart-healthy eating habits across the workforce",
        "Encourage balanced dietary patterns that support cardiovascular health",
        "Increase access to nutrition programmes focused on heart health",
    ],

    "nutrition_sodium": [
        "Promote lower sodium intake through workplace nutrition education",
        "Encourage healthier food choices that reduce excess salt consumption",
        "Support sodium reduction through awareness campaigns and healthy catering options",
    ],

    "nutrition_whole_food": [
        "Encourage balanced meals based on whole foods and appropriate portion sizes",
        "Promote eating patterns centred on minimally processed foods",
        "Support healthier food choices through whole-food nutrition programmes",
    ],

    "movement_programme": [
        "Encourage regular physical activity through structured movement programmes",
        "Promote active work routines with daily movement opportunities",
        "Support regular movement by incorporating active breaks into the workday",
    ],

    "weight_management": [
        "Provide structured weight management support for employees who may benefit",
        "Encourage healthy weight management through personalised lifestyle programmes",
        "Support long-term weight management with nutrition and physical activity guidance",
    ],

    "sleep_health": [
        "Promote healthy sleep habits to support recovery and overall wellbeing",
        "Encourage good sleep practices through workplace wellbeing initiatives",
        "Increase awareness of the importance of sleep for long-term health",
    ],

    "stress_management": [
        "Provide practical stress management resources and wellbeing support",
        "Encourage healthy stress management through workplace wellbeing programmes",
        "Support mental wellbeing by promoting stress reduction strategies",
    ],

    "recovery_programme": [
        "Develop recovery programmes focused on sleep, stress management, and healthy daily habits",
        "Support employee recovery through integrated wellbeing initiatives",
        "Promote recovery by combining healthy sleep, stress reduction, and restorative practices",
    ],

    "womens_health": [
        "Strengthen women's health programmes through education and preventive screening",
        "Improve access to confidential women's health support and clinical guidance",
        "Promote greater awareness of women's health through dedicated wellbeing initiatives",
    ],

    "smoking_cessation": [
        "Provide evidence-based smoking cessation support for employees",
        "Offer accessible programmes to help employees quit smoking",
        "Support tobacco cessation through counselling and workplace resources",
    ],

    "alcohol_moderation": [
        "Promote responsible alcohol consumption through health education",
        "Provide guidance on reducing alcohol-related health risks",
        "Encourage healthier drinking habits through workplace awareness programmes",
    ],

    "maintain_wellness": [
        "Continue existing preventive health initiatives and regular monitoring",
        "Maintain current wellbeing programmes that are supporting positive health outcomes",
        "Sustain healthy workplace practices through ongoing preventive care",
    ],

    "target_high_risk": [
        "Provide targeted health coaching for employees identified as higher risk",
        "Prioritise follow-up support for employees with elevated health risk",
        "Deliver focused preventive interventions for higher-risk groups",
    ],

    "scale_preventive_care": [
        "Expand preventive health programmes across screening, nutrition, and physical activity",
        "Strengthen organisation-wide preventive care through integrated wellness initiatives",
        "Broaden preventive services by combining clinical screening with lifestyle support",
    ],

    "clinical_review": [
        "Recommend clinical evaluation for employees with persistent abnormal findings",
        "Encourage medical review when health indicators require further assessment",
        "Support timely clinical follow-up for employees with concerning health markers",
    ],
}

# Synonymous, unquantified lead-ins for concern observations.
ELEVATED_SHARE_LEADS: List[str] = [
    "A considerable proportion of employees",
    "A notable proportion of employees",
    "A significant number of employees",
    "A meaningful proportion of employees",
    "A substantial share of employees",

]


# --- accessors (safe fallbacks; never raise on unknown ids) -------------------

def disease_severity_language(disease_id: str, band: str) -> Optional[str]:
    """Disease-specific severity phrase for a band, or None to fall back to the
    engine's generic language. Reasoning is unaffected — this is wording only."""
    return DISEASE_SEVERITY_LANGUAGE.get(disease_id, {}).get(band)


def disease_interpretation_clauses(disease_id: str) -> List[str]:
    return DISEASE_INTERPRETATION_PHRASES.get(disease_id, [])


def disease_positive_narratives(disease_id: str) -> List[str]:
    return DISEASE_POSITIVE_NARRATIVES.get(disease_id, [])


def disease_leadership_takeaways(disease_id: str) -> List[str]:
    return DISEASE_LEADERSHIP_TAKEAWAYS.get(disease_id, [])


def lever_action_variants(lever: str) -> List[str]:
    return LEVER_ACTION_VARIANTS.get(lever, [])

# ============================================================
# GRAPH-FACT HELPERS
# ============================================================

"""Graph-derived extras for MetricFinding.

Helpers only. Does not decide tone, mode, or observation wording.
Analyzer shares (elevated/healthy/dominant/opportunity) remain the
intelligence inputs; extras are supporting facts from plotted data.
"""


from typing import Any, Iterable, Optional, Sequence


PA_LT30 = "Less than 30mins"
PA_MID = "30-60mins"
PA_HIGH = "More than 60 mins"
PA_RARELY = "Rarely or Never"
PA_POOR = (PA_LT30, PA_RARELY)

SLEEP_LT5 = "Less than 5"
SLEEP_5_7 = "5-7"
SLEEP_7_9 = "7-9"
SLEEP_GT9 = "More than 9"


def slice_percent(slices: Sequence[DistributionSlice] | None, label: str) -> float:
    if not slices:
        return 0.0
    for item in slices:
        if item.label == label:
            return round1(float(item.percent or 0))
    return 0.0


def dominant_slice(slices: Sequence[DistributionSlice] | None) -> tuple[str, float]:
    if not slices:
        return "", 0.0
    top = max(slices, key=lambda s: float(s.percent or 0))
    return top.label, round1(float(top.percent or 0))


def count_total(slices: Sequence[DistributionSlice] | None) -> float:
    if not slices:
        return 0.0
    return float(sum(s.count or 0 for s in slices))


def combined_count_percent(
    male: Sequence[DistributionSlice] | None,
    female: Sequence[DistributionSlice] | None,
    labels: Iterable[str],
) -> Optional[float]:
    """Share of labelled categories across both genders, using responder counts.

    Returns None when counts are missing so callers do not invent a blend.
    """
    wanted = set(labels)
    male_n = count_total(male)
    female_n = count_total(female)
    total = male_n + female_n
    if total <= 0:
        return None
    male_hit = sum((s.count or 0) for s in (male or []) if s.label in wanted)
    female_hit = sum((s.count or 0) for s in (female or []) if s.label in wanted)
    return round1(100.0 * (male_hit + female_hit) / total)


def join_names(values: Iterable[str]) -> str:
    names = [str(v).strip() for v in values if str(v).strip()]
    if not names:
        return ""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def merge_extras(finding: MetricFinding, extra: dict[str, Any]) -> MetricFinding:
    finding.extras = {**(finding.extras or {}), **extra}
    return finding


def lifestyle_graph_extras(
    metric_id: str,
    *,
    view: str,
    male,
    female,
    merged: Sequence[DistributionSlice],
) -> dict[str, Any]:
    extras: dict[str, Any] = {"graph_view": view}
    if view == "both":
        m_label, m_pct = dominant_slice(male)
        f_label, f_pct = dominant_slice(female)
        extras["male_dominant_label"] = m_label
        extras["male_dominant_percent"] = m_pct
        extras["female_dominant_label"] = f_label
        extras["female_dominant_percent"] = f_pct
        extras["male_sample"] = count_total(male)
        extras["female_sample"] = count_total(female)
    else:
        label, pct = dominant_slice(merged)
        extras["view_dominant_label"] = label
        extras["view_dominant_percent"] = pct

    if metric_id == "physical_activity":
        if view == "both":
            extras["male_lt30_percent"] = slice_percent(male, PA_LT30)
            extras["male_rarely_percent"] = slice_percent(male, PA_RARELY)
            extras["male_mid_percent"] = slice_percent(male, PA_MID)
            extras["male_high_percent"] = slice_percent(male, PA_HIGH)
            extras["female_lt30_percent"] = slice_percent(female, PA_LT30)
            extras["female_rarely_percent"] = slice_percent(female, PA_RARELY)
            extras["female_mid_percent"] = slice_percent(female, PA_MID)
            extras["female_high_percent"] = slice_percent(female, PA_HIGH)
            combined = combined_count_percent(male, female, PA_POOR)
            if combined is not None:
                extras["combined_low_activity"] = combined
                extras["combined_is_derived"] = True
        else:
            extras["lt30_percent"] = slice_percent(merged, PA_LT30)
            extras["rarely_percent"] = slice_percent(merged, PA_RARELY)
            extras["mid_percent"] = slice_percent(merged, PA_MID)
            extras["high_activity_percent"] = slice_percent(merged, PA_HIGH)

    if metric_id == "sleep":
        if view == "both":
            extras["male_lt5_percent"] = slice_percent(male, SLEEP_LT5)
            extras["male_5_7_percent"] = slice_percent(male, SLEEP_5_7)
            extras["male_7_9_percent"] = slice_percent(male, SLEEP_7_9)
            extras["male_gt9_percent"] = slice_percent(male, SLEEP_GT9)
            extras["female_lt5_percent"] = slice_percent(female, SLEEP_LT5)
            extras["female_5_7_percent"] = slice_percent(female, SLEEP_5_7)
            extras["female_7_9_percent"] = slice_percent(female, SLEEP_7_9)
            extras["female_gt9_percent"] = slice_percent(female, SLEEP_GT9)
        else:
            extras["sleep_lt5_percent"] = slice_percent(merged, SLEEP_LT5)
            extras["sleep_5_7_percent"] = slice_percent(merged, SLEEP_5_7)
            extras["sleep_7_9_percent"] = slice_percent(merged, SLEEP_7_9)
            extras["sleep_gt9_percent"] = slice_percent(merged, SLEEP_GT9)
    return extras


def participation_graph_extras(by_age: list[ParticipationByAge]) -> dict[str, Any]:
    if not by_age:
        return {}
    ranked = sorted(by_age, key=lambda r: (r.percent, r.enrolled), reverse=True)
    top = ranked[0]
    empty = [r.age_group for r in by_age if (r.enrolled or 0) <= 0 and (r.percent or 0) <= 0]
    enrolled = [r for r in by_age if (r.enrolled or 0) > 0]
    extras: dict[str, Any] = {
        "topAgeGroup": top.age_group,
        "topPercent": round1(top.percent),
        "topEnrolled": top.enrolled,
        "empty_age_groups": join_names(empty),
    }
    if enrolled:
        bottom = min(enrolled, key=lambda r: (r.percent, r.enrolled))
        if bottom.age_group != top.age_group:
            extras["bottomAgeGroup"] = bottom.age_group
            extras["bottomPercent"] = round1(bottom.percent)
            extras["bottomEnrolled"] = bottom.enrolled
    return extras


def overall_risk_graph_extras(categories: list[CategoryShare]) -> dict[str, Any]:
    by_label = {c.label: round1(c.percent) for c in categories}
    increased = by_label.get("Increased Risk", 0.0)
    high = by_label.get("High risk", 0.0)
    return {
        "optimal_percent": by_label.get("Optimal", 0.0),
        "low_risk_percent": by_label.get("Low risk", 0.0),
        "increased_percent": increased,
        "high_risk_percent": high,
        "elevated_combined": round1(increased + high),
        "elevated_definition": "increased_plus_high",
    }


def oxidative_graph_extras(categories: list[CategoryShare]) -> dict[str, Any]:
    by_label = {c.label: round1(c.percent) for c in categories}
    high = by_label.get("High", 0.0)
    very_high = by_label.get("Very High", 0.0)
    low = by_label.get("Low", 0.0)
    moderate = by_label.get("Moderate", 0.0)
    return {
        "low_percent": low,
        "moderate_percent": moderate,
        "high_percent": high,
        "very_high_percent": very_high,
        "high_plus_very_high": round1(high + very_high),
        "non_elevated_percent": round1(low + moderate),
        "elevated_definition": "high_plus_very_high",
    }

# ============================================================
# ANALYZER
# ============================================================

"""Flat staging merge of analyzers + adapters.

Merged without logic changes from:
  - intelligence/analyzers/distribution_analyzer.py
  - intelligence/analyzers/overall_risk.py
  - intelligence/analyzers/lifestyle_distribution.py
  - intelligence/analyzers/disease_risk.py
  - intelligence/analyzers/oxidative_stress.py
  - intelligence/analyzers/nutrition.py
  - intelligence/analyzers/positive_wins.py
  - intelligence/analyzers/misc.py
  - intelligence/adapters/findings.py
  - intelligence/adapters/from_camp_report.py
  - intelligence/adapters/dashboard_slices.py

``compose_company_profile`` / ``rebuild_profile_with_finding`` import profile
lazily inside the function body to avoid analyzer ↔ profile import cycles.
"""


import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple, Union


# ---------------------------------------------------------------------------
# analyzers/distribution_analyzer.py
# ---------------------------------------------------------------------------


@dataclass
class DistributionInputCategory:
    label: str
    percent: float
    id: Optional[str] = None
    count: Optional[float] = None


def polarity_for_label(label: str, healthy_labels: List[str], elevated_labels: List[str]) -> CategoryPolarity:
    if label in healthy_labels:
        return "healthy"
    if label in elevated_labels:
        return "elevated"
    return "neutral"


def sum_by_polarity(categories: List[CategoryShare], polarity: CategoryPolarity) -> float:
    return sum(c.percent for c in categories if c.polarity == polarity)


def average(values: List[float]) -> float:
    if not values:
        return 0
    return sum(values) / len(values)


def analyze_distribution(
    metric_id: MetricId,
    categories: List[DistributionInputCategory],
    healthy_labels: List[str],
    elevated_labels: List[str],
    high_severity_labels: Optional[List[str]] = None,
    sample_size: Optional[float] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> MetricFinding:
    high_severity_labels = high_severity_labels or []
    built: List[CategoryShare] = [
        CategoryShare(
            id=c.id if c.id is not None else c.label,
            label=c.label,
            percent=c.percent,
            count=c.count,
            polarity=polarity_for_label(c.label, healthy_labels, elevated_labels),
        )
        for c in categories
    ]

    dominant = None
    if built:
        dominant = sorted(built, key=lambda c: c.percent, reverse=True)[0]

    healthy_share = sum_by_polarity(built, "healthy")
    elevated_share = sum_by_polarity(built, "elevated")
    high_severity_share = sum(c.percent for c in built if c.label in high_severity_labels)

    elevated_only = [c for c in built if c.polarity == "elevated"]
    opportunity = sorted(elevated_only, key=lambda c: c.percent, reverse=True)[0] if elevated_only else None

    return MetricFinding(
        metric_id=metric_id,
        categories=built,
        dominant=dominant,
        healthy_share=round1(healthy_share),
        elevated_share=round1(elevated_share),
        high_severity_share=round1(high_severity_share),
        opportunity=opportunity,
        sample_size=sample_size,
        extras=extras,
    )


# ---------------------------------------------------------------------------
# analyzers/overall_risk.py
# ---------------------------------------------------------------------------


def analyze_overall_risk(buckets: List[OverallRiskScoreBucket]) -> MetricFinding:
    knowledge = get_lifestyle_knowledge("overall_risk")
    sample_size = sum(b.count for b in buckets)
    finding = analyze_distribution(
        metric_id="overall_risk",
        categories=[DistributionInputCategory(label=b.band, percent=b.percent, count=b.count) for b in buckets],
        healthy_labels=knowledge.healthy_labels,
        elevated_labels=knowledge.poor_labels,
        high_severity_labels=knowledge.high_severity_labels,
        sample_size=sample_size if sample_size > 0 else None,
        extras={"displayName": "Overall risk"},
    )
    return merge_extras(finding, overall_risk_graph_extras(finding.categories))


# ---------------------------------------------------------------------------
# analyzers/lifestyle_distribution.py
# ---------------------------------------------------------------------------

LifestyleMetricId = Literal["physical_activity", "sleep"]


def _weight(gender_weights: object, key: str) -> float:
    """Accept a dict or GenderWeights dataclass; missing/None means 1 (TS ``?? 1``)."""
    if gender_weights is None:
        return 1
    value = (
        gender_weights.get(key)
        if isinstance(gender_weights, dict)
        else getattr(gender_weights, key, None)
    )
    return 1 if value is None else float(value)


def _merge_gender_slices(
    data: GenderDistributionPair,
    view: LifestyleGenderView,
    gender_weights: Optional[Dict[str, float]] = None,
) -> List[DistributionSlice]:
    if view == "male":
        return data.male
    if view == "female":
        return data.female

    labels = list(dict.fromkeys([s.label for s in data.male] + [s.label for s in data.female]))
    male_has_counts = any(s.count is not None for s in data.male)
    female_has_counts = any(s.count is not None for s in data.female)
    use_counts = male_has_counts or female_has_counts
    male_w = _weight(gender_weights, "male")
    female_w = _weight(gender_weights, "female")
    total_w = (male_w + female_w) or 1
    grand_count = 0.0
    if use_counts:
        grand_count = float(
            sum((s.count or 0) for s in data.male) + sum((s.count or 0) for s in data.female)
        )

    merged: List[DistributionSlice] = []
    for label in labels:
        male_slice = next((s for s in data.male if s.label == label), None)
        female_slice = next((s for s in data.female if s.label == label), None)
        male_pct = male_slice.percent if male_slice else 0
        female_pct = female_slice.percent if female_slice else 0
        male_count = male_slice.count if male_slice else None
        female_count = female_slice.count if female_slice else None
        count = (male_count or 0) + (female_count or 0) if (male_count is not None or female_count is not None) else None
        if use_counts and grand_count > 0:
            percent = round1(100.0 * float(count or 0) / grand_count)
        else:
            percent = round1((male_pct * male_w + female_pct * female_w) / total_w)
        merged.append(
            DistributionSlice(
                label=label,
                percent=percent,
                count=count,
            )
        )
    return merged


def _elevated_for_side(slices: List[DistributionSlice], poor_labels: List[str]) -> float:
    return sum(s.percent for s in slices if s.label in poor_labels)


def analyze_lifestyle_distribution(
    metric_id: LifestyleMetricId,
    data: GenderDistributionPair,
    view: Optional[LifestyleGenderView] = None,
    gender_weights: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    knowledge = get_lifestyle_knowledge(metric_id)
    view = view or "both"
    merged = _merge_gender_slices(data, view, gender_weights)
    sample_size = sum(s.count or 0 for s in merged)

    finding = analyze_distribution(
        metric_id=metric_id,
        categories=[DistributionInputCategory(label=s.label, percent=s.percent, count=s.count) for s in merged],
        healthy_labels=knowledge.healthy_labels,
        elevated_labels=knowledge.poor_labels,
        high_severity_labels=knowledge.high_severity_labels,
        sample_size=sample_size if sample_size > 0 else None,
        extras={"displayName": knowledge.display_name},
    )
    merge_extras(
        finding,
        lifestyle_graph_extras(
            metric_id,
            view=view,
            male=data.male,
            female=data.female,
            merged=merged,
        ),
    )

    male_elevated = _elevated_for_side(data.male, knowledge.poor_labels)
    female_elevated = _elevated_for_side(data.female, knowledge.poor_labels)
    if data.male and data.female:
        finding.gender_gap = GenderGap(
            male_elevated=round1(male_elevated),
            female_elevated=round1(female_elevated),
            delta=round1(abs(male_elevated - female_elevated)),
        )

    return finding


def analyze_physical_activity(
    data: GenderDistributionPair,
    view: Optional[LifestyleGenderView] = None,
    gender_weights: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    return analyze_lifestyle_distribution("physical_activity", data, view, gender_weights)


def analyze_sleep(
    data: GenderDistributionPair,
    view: Optional[LifestyleGenderView] = None,
    gender_weights: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    return analyze_lifestyle_distribution("sleep", data, view, gender_weights)


def compute_poor_percent(
    data: GenderDistributionPair,
    metric_id: MetricId,
    view: LifestyleGenderView = "both",
    gender_weights: Optional[Dict[str, float]] = None,
) -> float:
    """Weighted poor percent helper retained for adapters/tests."""
    finding = analyze_lifestyle_distribution(metric_id, data, view, gender_weights)  # type: ignore[arg-type]
    return finding.elevated_share


def average_gender_elevated(data: GenderDistributionPair, poor_labels: List[str]) -> Optional[float]:
    sides = [side for side in (data.male, data.female) if side]
    if not sides:
        return None
    return average([_elevated_for_side(side, poor_labels) for side in sides])


# ---------------------------------------------------------------------------
# analyzers/disease_risk.py
# ---------------------------------------------------------------------------

ELEVATED_LEVELS: List[RiskLevel] = ["Increased", "High", "Very High"]
HIGH_SEVERITY: List[RiskLevel] = ["High", "Very High"]


def _segment_average(segments: dict) -> float:
    return average(list(segments.values()))


def _gender_band_sum(
    disease: DiseaseRiskData,
    gender: str,
    levels: List[str],
) -> Optional[float]:
    wanted = set(levels)
    total = 0.0
    found = False
    for bucket in disease.buckets:
        if bucket.level not in wanted:
            continue
        value = bucket.segments.get(gender)
        if isinstance(value, (int, float)):
            total += float(value)
            found = True
    return round1(total) if found else None


def _gender_has_plotted_data(disease: DiseaseRiskData, gender: str) -> bool:
    for bucket in disease.buckets:
        count = (bucket.counts or {}).get(gender)
        percent = bucket.segments.get(gender)
        if (isinstance(count, (int, float)) and float(count) > 0) or (
            isinstance(percent, (int, float)) and float(percent) > 0
        ):
            return True
    return False


def _dominant_from_segments(disease: DiseaseRiskData, gender: str) -> tuple[str, float]:
    best_label = ""
    best_pct = -1.0
    for bucket in disease.buckets:
        value = bucket.segments.get(gender)
        if not isinstance(value, (int, float)):
            continue
        if float(value) > best_pct:
            best_pct = float(value)
            best_label = bucket.level
    return best_label, round1(best_pct) if best_pct >= 0 else 0.0


def _count_weighted_disease_bands(disease: DiseaseRiskData) -> dict[str, Any]:
    """Workforce band shares from plotted counts when present, else unweighted percents."""
    levels = [bucket.level for bucket in disease.buckets]
    counts_by_level: dict[str, float] = {level: 0.0 for level in levels}
    used_counts = False
    total_count = 0.0
    for bucket in disease.buckets:
        if bucket.counts:
            used_counts = True
            level_count = float(sum(float(v) for v in bucket.counts.values()))
            counts_by_level[bucket.level] = level_count
            total_count += level_count

    percents: dict[str, float] = {}
    if used_counts and total_count > 0:
        for level in levels:
            percents[level] = round1(100.0 * counts_by_level[level] / total_count)
    else:
        for bucket in disease.buckets:
            percents[bucket.level] = round1(_segment_average(bucket.segments))

    return {
        "percents": percents,
        "used_counts": used_counts and total_count > 0,
        "male_dominant": _dominant_from_segments(disease, "Male"),
        "female_dominant": _dominant_from_segments(disease, "Female"),
    }


def analyze_disease_risk_data(disease: DiseaseRiskData) -> MetricFinding:
    metric_id: MetricId = disease_metric_from_name(disease.disease.name) or disease.disease.code
    knowledge = get_disease_knowledge(metric_id)

    weighted = _count_weighted_disease_bands(disease)
    categories = [
        DistributionInputCategory(label=bucket.level, percent=weighted["percents"].get(bucket.level, 0.0))
        for bucket in disease.buckets
    ]

    finding = analyze_distribution(
        metric_id=metric_id,
        categories=categories,
        healthy_labels=["Healthy"],
        elevated_labels=ELEVATED_LEVELS,
        high_severity_labels=HIGH_SEVERITY,
        extras={
            "displayName": knowledge.display_name if knowledge else disease.disease.name,
            "overallStatus": disease.overall_status,
        },
    )

    male_hvh = _gender_band_sum(disease, "Male", HIGH_SEVERITY)
    female_hvh = _gender_band_sum(disease, "Female", HIGH_SEVERITY)
    if male_hvh is not None and female_hvh is not None:
        finding.gender_gap = GenderGap(
            male_elevated=male_hvh,
            female_elevated=female_hvh,
            delta=round1(abs(male_hvh - female_hvh)),
        )

    merge_extras(finding, {
        "healthy_percent": weighted["percents"].get("Healthy", 0.0),
        "increased_percent": weighted["percents"].get("Increased", 0.0),
        "high_percent": weighted["percents"].get("High", 0.0),
        "very_high_percent": weighted["percents"].get("Very High", 0.0),
        "high_plus_very_high": round1(
            weighted["percents"].get("High", 0.0) + weighted["percents"].get("Very High", 0.0)
        ),
        "elevated_definition": "high_plus_very_high",
        "weighted_by_counts": weighted["used_counts"],
        "male_dominant_label": weighted["male_dominant"][0],
        "male_dominant_percent": weighted["male_dominant"][1],
        "female_dominant_label": weighted["female_dominant"][0],
        "female_dominant_percent": weighted["female_dominant"][1],
        "male_increased_percent": _gender_band_sum(disease, "Male", ["Increased"]) or 0.0,
        "female_increased_percent": _gender_band_sum(disease, "Female", ["Increased"]) or 0.0,
        "male_high_plus_very_high": male_hvh or 0.0,
        "female_high_plus_very_high": female_hvh or 0.0,
        "male_has_data": _gender_has_plotted_data(disease, "Male"),
        "female_has_data": _gender_has_plotted_data(disease, "Female"),
    })
    return finding


def analyze_top_disease(disease: TopHighRiskDisease) -> MetricFinding:
    metric_id: MetricId = disease_metric_from_name(disease.name) or "metabolic_syndrome"
    knowledge = get_disease_knowledge(metric_id)
    elevated = disease.high_risk_percent
    return analyze_distribution(
        metric_id=metric_id,
        categories=[
            DistributionInputCategory(label="Healthy", percent=round1(max(0.0, 100 - elevated))),
            DistributionInputCategory(label="Elevated", percent=round1(elevated)),
        ],
        healthy_labels=["Healthy"],
        elevated_labels=["Elevated"],
        high_severity_labels=["Elevated"],
        extras={
            "displayName": knowledge.display_name if knowledge else disease.name,
            "highRiskPercent": elevated,
            "high_plus_very_high": round1(elevated),
            "elevated_definition": "high_plus_very_high",
        },
    )


def analyze_disease_set(diseases: List[DiseaseRiskData]) -> List[MetricFinding]:
    return [analyze_disease_risk_data(d) for d in diseases]


# ---------------------------------------------------------------------------
# analyzers/oxidative_stress.py
# ---------------------------------------------------------------------------


def analyze_oxidative_stress(
    rows: List[OxidativeStressByDept],
    headcounts: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    knowledge = get_lifestyle_knowledge("oxidative_stress")

    if not rows:
        return analyze_distribution(
            metric_id="oxidative_stress",
            categories=[],
            healthy_labels=knowledge.healthy_labels,
            elevated_labels=["High", "Very High"],
            high_severity_labels=["Very High"],
        )

    low = moderate = high = very_high = weight_sum = 0.0

    for row in rows:
        w = (headcounts or {}).get(row.department, 1)
        weight_sum += w
        low += row.low * w
        moderate += row.moderate * w
        high += row.high * w
        very_high += row.very_high * w

    denom = weight_sum or 1
    finding = analyze_distribution(
        metric_id="oxidative_stress",
        categories=[
            DistributionInputCategory(label="Low", percent=round1(low / denom)),
            DistributionInputCategory(label="Moderate", percent=round1(moderate / denom)),
            DistributionInputCategory(label="High", percent=round1(high / denom)),
            DistributionInputCategory(label="Very High", percent=round1(very_high / denom)),
        ],
        healthy_labels=["Low"],
        elevated_labels=["High", "Very High"],
        high_severity_labels=["Very High"],
        extras={"departmentCount": len(rows), "displayName": "Oxidative stress"},
    )
    return merge_extras(finding, oxidative_graph_extras(finding.categories))


def analyze_oxidative_row(row: OxidativeStressByDept) -> MetricFinding:
    return analyze_oxidative_stress([row])


# ---------------------------------------------------------------------------
# analyzers/nutrition.py
# ---------------------------------------------------------------------------


def analyze_nutrition_from_score(scores: CompanyAverageScores) -> MetricFinding:
    """
    Nutrition from company average score (0-100, higher = healthier).
    Burden is inverted from the score.
    """
    nutrition_score = scores.nutrition
    poor_share = round1(max(0.0, min(100.0, 100 - nutrition_score)))
    healthy_share = round1(max(0.0, min(100.0, nutrition_score)))

    return analyze_distribution(
        metric_id="nutrition",
        categories=[
            DistributionInputCategory(label="Within healthy range", percent=healthy_share),
            DistributionInputCategory(label="Below healthy range", percent=poor_share),
        ],
        healthy_labels=["Within healthy range"],
        elevated_labels=["Below healthy range"],
        high_severity_labels=["Below healthy range"] if nutrition_score < 40 else [],
        extras={
            "nutritionScore": nutrition_score,
            "fitnessScore": scores.fitness,
            "lifestyleScore": scores.lifestyle,
        },
    )


def analyze_nutrition_summary(summary: NutritionSummary) -> MetricFinding:
    avg = summary.avg_score
    poor_share = round1(max(0.0, min(100.0, 100 - avg)))
    fibre = next((m for m in summary.macros if "fibre" in m.name.lower()), None)
    return analyze_distribution(
        metric_id="nutrition",
        categories=[
            DistributionInputCategory(label="Within ideal", percent=round1(avg)),
            DistributionInputCategory(label="Outside ideal", percent=poor_share),
        ],
        healthy_labels=["Within ideal"],
        elevated_labels=["Outside ideal"],
        extras={
            "nutritionScore": avg,
            "riskBand": summary.risk_band,
            "fibreWithinIdeal": fibre.within_ideal_percent if fibre else 0,
        },
    )


# ---------------------------------------------------------------------------
# analyzers/positive_wins.py
# ---------------------------------------------------------------------------


def analyze_positive_wins(data: PositiveWins) -> MetricFinding:
    low_risk_count = len(data.low_risk)
    habits_count = len(data.healthy_habits)
    profiles_count = len(data.healthy_profiles)
    total_signals = low_risk_count + habits_count + profiles_count
    has_signals = 100.0 if total_signals else 0.0

    low_risk_names = join_names(item.name for item in data.low_risk)
    habit_labels = join_names(item.habit_label for item in data.healthy_habits)
    profile_names = join_names(data.healthy_profiles)

    return analyze_distribution(
        metric_id="positive_wins",
        categories=[
            DistributionInputCategory(label="Strength signals", percent=has_signals),
        ],
        healthy_labels=["Strength signals"],
        elevated_labels=[],
        extras={
            "lowRiskCount": low_risk_count,
            "habitsCount": habits_count,
            "profilesCount": profiles_count,
            "totalSignals": total_signals,
            "low_risk_names": low_risk_names,
            "habit_labels": habit_labels,
            "profile_names": profile_names,
        },
    )


# ---------------------------------------------------------------------------
# analyzers/misc.py
# ---------------------------------------------------------------------------

_NORMAL_RE = re.compile(r"normal|healthy|ideal", re.IGNORECASE)
_OVERWEIGHT_RE = re.compile(r"overweight|obese|obesity", re.IGNORECASE)


def analyze_metabolic_age(data: MetabolicAgeSummary) -> MetricFinding:
    return analyze_distribution(
        metric_id="metabolic_age",
        categories=[
            DistributionInputCategory(label=b.label, percent=b.percent, count=b.count) for b in data.buckets
        ],
        healthy_labels=[b.label for b in data.buckets if not b.is_high_risk],
        elevated_labels=[b.label for b in data.buckets if b.is_high_risk],
        extras={
            "avgGapYears": data.avg_gap_years,
            "highRiskPercent": data.high_risk_percent,
        },
    )


def analyze_bmi_waist(data: BmiWaistSummary) -> MetricFinding:
    elevated = data.above_ideal_waist_percent
    return analyze_distribution(
        metric_id="bmi_waist",
        categories=[
            *[DistributionInputCategory(label=b.label, percent=b.percent) for b in data.bmi_distribution],
            DistributionInputCategory(label="Above ideal waist", percent=elevated),
        ],
        healthy_labels=[b.label for b in data.bmi_distribution if _NORMAL_RE.search(b.label)],
        elevated_labels=[
            *[b.label for b in data.bmi_distribution if _OVERWEIGHT_RE.search(b.label)],
            "Above ideal waist",
        ],
        extras={
            "avgWaistInches": data.avg_waist_inches,
            "aboveIdealWaistPercent": elevated,
            "insightTag": data.insight_tag,
        },
    )


def analyze_participation(by_age: List[ParticipationByAge]) -> MetricFinding:
    if not by_age:
        return analyze_distribution(
            metric_id="participation",
            categories=[],
            healthy_labels=[],
            elevated_labels=[],
        )
    extras = participation_graph_extras(by_age)
    return analyze_distribution(
        metric_id="participation",
        categories=[
            DistributionInputCategory(label=r.age_group, percent=r.percent, count=r.enrolled) for r in by_age
        ],
        healthy_labels=[],
        elevated_labels=[],
        extras=extras,
    )


def analyze_company_scores_as_fitness(fitness: float) -> MetricFinding:
    poor = round1(max(0.0, 100 - fitness))
    return analyze_distribution(
        metric_id="physical_activity",
        categories=[
            DistributionInputCategory(label="Adequate fitness", percent=round1(fitness)),
            DistributionInputCategory(label="Fitness gap", percent=poor),
        ],
        healthy_labels=["Adequate fitness"],
        elevated_labels=["Fitness gap"],
        extras={"fitnessScore": fitness},
    )


# ---------------------------------------------------------------------------
# adapters/findings.py
# ---------------------------------------------------------------------------


def finding_from_overall_risk(buckets: List[OverallRiskScoreBucket]) -> MetricFinding:
    return analyze_overall_risk(buckets)


def finding_from_physical_activity(
    data: GenderDistributionPair,
    view: LifestyleGenderView = "both",
    gender_weights: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    return analyze_physical_activity(data, view, gender_weights)


def finding_from_sleep(
    data: GenderDistributionPair,
    view: LifestyleGenderView = "both",
    gender_weights: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    return analyze_sleep(data, view, gender_weights)


def finding_from_top_disease(disease: TopHighRiskDisease) -> MetricFinding:
    return analyze_top_disease(disease)


def finding_from_disease(disease: DiseaseRiskData) -> MetricFinding:
    return analyze_disease_risk_data(disease)


def finding_from_oxidative(
    rows: List[OxidativeStressByDept],
    headcounts: Optional[Dict[str, float]] = None,
) -> MetricFinding:
    return analyze_oxidative_stress(rows, headcounts)


def finding_from_oxidative_row(row: OxidativeStressByDept) -> MetricFinding:
    return analyze_oxidative_row(row)


def finding_from_positive_wins(data: PositiveWins) -> MetricFinding:
    return analyze_positive_wins(data)


def finding_from_company_scores(scores: CompanyAverageScores) -> MetricFinding:
    return analyze_nutrition_from_score(scores)


def finding_from_nutrition(summary: NutritionSummary) -> MetricFinding:
    return analyze_nutrition_summary(summary)


def finding_from_metabolic_age(data: MetabolicAgeSummary) -> MetricFinding:
    return analyze_metabolic_age(data)


def finding_from_bmi_waist(data: BmiWaistSummary) -> MetricFinding:
    return analyze_bmi_waist(data)


def finding_from_participation(by_age: List[ParticipationByAge]) -> MetricFinding:
    return analyze_participation(by_age)


# ---------------------------------------------------------------------------
# adapters/from_camp_report.py
# ---------------------------------------------------------------------------

OVERALL_RISK_GROUP_LABELS = {
    "optimal": "Optimal",
    "low_risk": "Low risk",
    "increased_risk": "Increased Risk",
    "high_risk": "High risk",
}

PHYSICAL_ACTIVITY_GROUP_LABELS = {
    "less_than_30mins": "Less than 30mins",
    "30_60_mins": "30-60mins",
    "more_than_60_mins": "More than 60 mins",
    "rarely_or_never": "Rarely or Never",
}

SLEEP_GROUP_LABELS = {
    "less_than_5hrs": "Less than 5",
    "between_5_7_hrs": "5-7",
    "between_7_9_hrs": "7-9",
    "more_than_9hrs": "More than 9",
}

RISK_LEVEL_GROUPS = {
    "healthy": "Healthy",
    "increased": "Increased",
    "high": "High",
    "very_high": "Very High",
    "veryhigh": "Very High",
    "very high": "Very High",
}

DEEP_DIVE_EXCLUDED_CODES = frozenset({"metabolic_syndrome"})

SECTION_KEYS = {
    "overall_risk": "overall_risk_score",
    "physical_activity": "distribution_by_physical_activity_frequency",
    "sleep": "distribution_by_sleeping_hours",
    "oxidative_stress": "distribution_by_oxidative_stress",
    "diseases": "distribution_by_gender_by_metabolic_syndrome",
    "positive_wins": "positive_wins",
    "company_scores": "company_average_scores",
    "participation": "participation_by_age",
    "kpis": "kpis",
}


def _section_data(report: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
    section = report.get(key)
    if section is None:
        return None
    if isinstance(section, dict) and "data" in section and isinstance(section["data"], dict):
        return section["data"]
    if isinstance(section, dict):
        return section
    return None


def _disease_display_name(code: str) -> str:
    knowledge = get_disease_knowledge(code)  # type: ignore[arg-type]
    if knowledge is not None:
        return knowledge.display_name
    metric = disease_metric_from_name(code.replace("_", " "))
    if metric:
        k2 = get_disease_knowledge(metric)
        if k2:
            return k2.display_name
    return code.replace("_", " ").title()


def _disease_definition(code: str) -> DiseaseDefinition:
    return DiseaseDefinition(code=code, name=_disease_display_name(code))  # type: ignore[arg-type]


def _gender_headcount(side: Dict[str, Any]) -> int:
    return int(sum(side.get("count") or []))


def _workforce_elevated_percent(item: Dict[str, Any]) -> float:
    male = item.get("male") or {}
    female = item.get("female") or {}
    male_total = _gender_headcount(male)
    female_total = _gender_headcount(female)
    total = male_total + female_total
    if total == 0:
        return 0.0
    male_elevated = float(male.get("elevated_percent") or 0)
    female_elevated = float(female.get("elevated_percent") or 0)
    return round(((male_elevated * male_total + female_elevated * female_total) / total) * 10) / 10


def _risk_level_from_group(group: str) -> Optional[str]:
    return RISK_LEVEL_GROUPS.get(group) or RISK_LEVEL_GROUPS.get(group.lower())


def _map_gender_side(side: Dict[str, Any], label_map: Dict[str, str]) -> List[DistributionSlice]:
    groups = side.get("group") or []
    percents = side.get("percent") or []
    counts = side.get("count") or []
    out: List[DistributionSlice] = []
    for i, key in enumerate(groups):
        if key == "unmapped":
            continue
        out.append(
            DistributionSlice(
                label=label_map.get(key, str(key).replace("_", " ")),
                percent=float(percents[i]) if i < len(percents) else 0.0,
                count=float(counts[i]) if i < len(counts) else None,
            )
        )
    return out


def _map_gender_pair(api: Dict[str, Any], label_map: Dict[str, str]) -> GenderDistributionPair:
    return GenderDistributionPair(
        male=_map_gender_side(api.get("male") or {}, label_map),
        female=_map_gender_side(api.get("female") or {}, label_map),
    )


def _map_overall_risk(data: Dict[str, Any]) -> List[OverallRiskScoreBucket]:
    groups = data.get("group") or []
    percents = data.get("percent") or []
    counts = data.get("count") or []
    buckets: List[OverallRiskScoreBucket] = []
    for i, group in enumerate(groups):
        band = OVERALL_RISK_GROUP_LABELS.get(group, group)
        buckets.append(
            OverallRiskScoreBucket(
                band=band,  # type: ignore[arg-type]
                percent=float(percents[i]) if i < len(percents) else 0.0,
                count=float(counts[i]) if i < len(counts) else 0.0,
            )
        )
    return buckets


def _map_oxidative(data: Dict[str, Any]) -> Tuple[List[OxidativeStressByDept], Optional[Dict[str, float]]]:
    field_map = {
        "low": "low",
        "moderate": "moderate",
        "high": "high",
        "very_high": "very_high",
        "veryhigh": "very_high",
        "very high": "very_high",
    }
    row = {"department": "Company-wide", "low": 0.0, "moderate": 0.0, "high": 0.0, "very_high": 0.0}
    groups = data.get("group") or []
    percents = data.get("percent") or []
    for i, group in enumerate(groups):
        field = field_map.get(group) or field_map.get(str(group).lower())
        if field:
            row[field] = float(percents[i]) if i < len(percents) else 0.0
    total = data.get("total_employees")
    headcounts = {"Company-wide": float(total)} if total is not None else None
    return [OxidativeStressByDept(**row)], headcounts


def _map_disease_item(item: Dict[str, Any]) -> DiseaseRiskData:
    code = str(item.get("code") or "")
    disease = _disease_definition(code)
    male = item.get("male") or {}
    female = item.get("female") or {}
    levels = ["Healthy", "Increased", "High", "Very High"]
    buckets: List[RiskDistributionBucket] = []
    for level in levels:
        male_idx = next(
            (i for i, g in enumerate(male.get("group") or []) if _risk_level_from_group(g) == level),
            -1,
        )
        female_idx = next(
            (i for i, g in enumerate(female.get("group") or []) if _risk_level_from_group(g) == level),
            -1,
        )
        male_pct = float((male.get("percent") or [0])[male_idx]) if male_idx >= 0 else 0.0
        female_pct = float((female.get("percent") or [0])[female_idx]) if female_idx >= 0 else 0.0
        male_count = float((male.get("count") or [0])[male_idx]) if male_idx >= 0 else 0.0
        female_count = float((female.get("count") or [0])[female_idx]) if female_idx >= 0 else 0.0
        buckets.append(
            RiskDistributionBucket(
                level=level,  # type: ignore[arg-type]
                segments={"Male": male_pct, "Female": female_pct},
                counts={"Male": male_count, "Female": female_count},
            )
        )

    healthy = next((b for b in buckets if b.level == "Healthy"), None)
    healthy_sum = (
        (sum(healthy.segments.values()) / max(len(healthy.segments), 1)) if healthy else 0.0
    )
    if healthy_sum >= 75:
        overall = "Healthy"
    elif healthy_sum >= 55:
        overall = "Increased"
    elif healthy_sum >= 40:
        overall = "High"
    else:
        overall = "Very High"

    return DiseaseRiskData(disease=disease, buckets=buckets, overall_status=overall)  # type: ignore[arg-type]


def _map_diseases(data: Dict[str, Any]) -> Tuple[List[TopHighRiskDisease], List[DiseaseRiskData]]:
    items = list(data.get("diseases") or [])
    ranked = sorted(items, key=_workforce_elevated_percent, reverse=True)
    top = [
        TopHighRiskDisease(
            name=_disease_definition(str(item.get("code") or "")).name,
            high_risk_percent=_workforce_elevated_percent(item),
        )
        for item in ranked[:3]
    ]
    diseases = [
        _map_disease_item(item)
        for item in items
        if str(item.get("code") or "") not in DEEP_DIVE_EXCLUDED_CODES
    ]
    return top, diseases


def _map_participation(data: Dict[str, Any]) -> List[ParticipationByAge]:
    age_groups = data.get("age_group") or data.get("ageGroup") or []
    enrolled = data.get("enrolled") or []
    percents = data.get("percent") or []
    return [
        ParticipationByAge(
            age_group=str(age_groups[i]),
            enrolled=float(enrolled[i]) if i < len(enrolled) else 0.0,
            percent=float(percents[i]) if i < len(percents) else 0.0,
        )
        for i in range(len(age_groups))
    ]


def _map_company_scores(data: Dict[str, Any]) -> CompanyAverageScores:
    def score_of(key: str) -> float:
        block = data.get(key)
        if isinstance(block, dict):
            return float(block.get("score") or 0)
        return float(block or 0)

    return CompanyAverageScores(
        nutrition=score_of("nutrition"),
        fitness=score_of("fitness"),
        lifestyle=score_of("lifestyle"),
    )


def _map_positive_wins(data: Dict[str, Any]) -> PositiveWins:
    return PositiveWins.from_dict(data)


def _map_kpis_weights(data: Dict[str, Any]) -> GenderWeights:
    return GenderWeights(
        male=float(data["male_enrolled"]) if data.get("male_enrolled") is not None else None,
        female=float(data["female_enrolled"]) if data.get("female_enrolled") is not None else None,
    )


def dashboard_input_from_camp_report(report: Dict[str, Any]) -> DashboardInput:
    """Build engine input from a stored camp report JSON (section_key → payload)."""
    overall = _section_data(report, SECTION_KEYS["overall_risk"])
    physical = _section_data(report, SECTION_KEYS["physical_activity"])
    sleep = _section_data(report, SECTION_KEYS["sleep"])
    oxidative = _section_data(report, SECTION_KEYS["oxidative_stress"])
    diseases_section = _section_data(report, SECTION_KEYS["diseases"])
    positive = _section_data(report, SECTION_KEYS["positive_wins"])
    scores = _section_data(report, SECTION_KEYS["company_scores"])
    participation = _section_data(report, SECTION_KEYS["participation"])
    kpis = _section_data(report, SECTION_KEYS["kpis"])

    top_diseases = None
    diseases = None
    if diseases_section:
        top_diseases, diseases = _map_diseases(diseases_section)

    oxidative_rows = None
    oxidative_headcounts = None
    if oxidative:
        oxidative_rows, oxidative_headcounts = _map_oxidative(oxidative)

    return DashboardInput(
        overall_risk_score=_map_overall_risk(overall) if overall else None,
        physical_activity=_map_gender_pair(physical, PHYSICAL_ACTIVITY_GROUP_LABELS) if physical else None,
        sleep=_map_gender_pair(sleep, SLEEP_GROUP_LABELS) if sleep else None,
        diseases=diseases,
        top_high_risk_diseases=top_diseases,
        oxidative_stress=oxidative_rows,
        oxidative_headcounts=oxidative_headcounts,
        company_scores=_map_company_scores(scores) if scores else None,
        positive_wins=_map_positive_wins(positive) if positive else None,
        participation_by_age=_map_participation(participation) if participation else None,
        gender_weights=_map_kpis_weights(kpis) if kpis else None,
    )


# ---------------------------------------------------------------------------
# adapters/dashboard_slices.py
# ---------------------------------------------------------------------------

DashboardInputLike = Union[DashboardInput, Dict, None]


def collect_findings(data: DashboardInputLike) -> List[MetricFinding]:
    data = DashboardInput.ensure(data)
    findings: List[MetricFinding] = []
    covered_diseases: set = set()

    if data.overall_risk_score:
        findings.append(analyze_overall_risk(data.overall_risk_score))
    if data.physical_activity:
        findings.append(analyze_physical_activity(data.physical_activity, "both", data.gender_weights))
    if data.sleep:
        findings.append(analyze_sleep(data.sleep, "both", data.gender_weights))

    # Top Disease Risk chart values win over deep-dive recomputation.
    if data.top_high_risk_diseases:
        for d in data.top_high_risk_diseases:
            finding = analyze_top_disease(d)
            findings.append(finding)
            covered_diseases.add(finding.metric_id)

    # Deep-dive matrices fill remaining diseases only (for graph / deep-dive tabs).
    if data.diseases:
        for disease in data.diseases:
            metric_id: MetricId = disease_metric_from_name(disease.disease.name) or disease.disease.code
            if metric_id in covered_diseases:
                continue
            findings.append(analyze_disease_risk_data(disease))
            covered_diseases.add(metric_id)

    if data.oxidative_stress:
        findings.append(analyze_oxidative_stress(data.oxidative_stress, data.oxidative_headcounts))
    if data.nutrition:
        findings.append(analyze_nutrition_summary(data.nutrition))
    elif data.company_scores:
        findings.append(analyze_nutrition_from_score(data.company_scores))
    if data.positive_wins:
        findings.append(analyze_positive_wins(data.positive_wins))
    if data.metabolic_age:
        findings.append(analyze_metabolic_age(data.metabolic_age))
    if data.bmi_waist:
        findings.append(analyze_bmi_waist(data.bmi_waist))
    if data.participation_by_age:
        findings.append(analyze_participation(data.participation_by_age))

    return _dedupe_findings(findings)


def chart_top_diseases_from_input(data: DashboardInputLike) -> List[ChartTopDisease]:
    data = DashboardInput.ensure(data)
    if not data.top_high_risk_diseases:
        return []
    result: List[ChartTopDisease] = []
    for d in data.top_high_risk_diseases:
        metric_id: MetricId = disease_metric_from_name(d.name) or "metabolic_syndrome"
        result.append(ChartTopDisease(metric_id=metric_id, name=d.name, high_risk_percent=d.high_risk_percent))
    return result


# @deprecated Prefer chart_top_diseases_from_input
chart_top_diseases_from_slices = chart_top_diseases_from_input


def compose_company_profile(data: DashboardInputLike) -> CompanyHealthProfile:
    """
    Build the company health profile from structured dashboard data.
    This is the primary public entry for profile composition.
    """
    # Lazy import: analyzer ↔ profile cycle if profile ever imports analyzer.

    ensured = DashboardInput.ensure(data)
    return compose_profile_from_findings(
        collect_findings(ensured),
        ComposeProfileOptions(chart_top_diseases=chart_top_diseases_from_input(ensured)),
    )


# @deprecated Prefer compose_company_profile(data)
build_profile_from_slices = compose_company_profile


def rebuild_profile_with_finding(base: CompanyHealthProfile, finding: MetricFinding) -> CompanyHealthProfile:
    """
    Merge a section-local finding into an existing profile's finding set
    and rebuild scores/graph so local chart data is always reflected.
    """

    findings = [f for f in base.findings.values() if f is not None]
    without = [f for f in findings if f.metric_id != finding.metric_id]
    return compose_profile_from_findings(
        [*without, finding],
        ComposeProfileOptions(chart_top_diseases=base.chart_top_diseases),
    )


def _dedupe_findings(findings: List[MetricFinding]) -> List[MetricFinding]:
    by_metric: Dict[MetricId, MetricFinding] = {}
    for f in findings:
        by_metric[f.metric_id] = f
    return list(by_metric.values())

# ============================================================
# PROFILE & SCORING
# ============================================================

"""Flat staging merge of scoring + graph + priority + profile composition.

Merged without logic changes from:
  - intelligence/scoring/score_metric.py
  - intelligence/graph/evaluate_graph.py
  - intelligence/priority/calculate_priorities.py
  - intelligence/profile/select_one_thing.py
  - intelligence/profile/compose_company_profile.py
"""


from dataclasses import dataclass, field
from typing import Dict, List, Optional


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

# ============================================================
# REASONING
# ============================================================

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


from dataclasses import dataclass
from typing import Any


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

# ============================================================
# INSIGHT GENERATION
# ============================================================

"""Insight generation — display helpers, structured narrative, leadership, public API.

Flat merge (no business-logic changes) of:
  - intelligence/insight/display.py
  - intelligence/insight/generate_structured_insight.py
  - intelligence/insight/generate_leadership.py
  - intelligence/compose/compose_insight.py
  - intelligence/generate.py

``format_chart_footer`` / ``format_leadership_cards`` are lazy-imported from
``.formatter`` inside the public generate_* functions to avoid an import cycle.
"""


import re
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# insight/display.py
# ---------------------------------------------------------------------------

# Human labels only — never return snake_case metric ids.
FALLBACK_LABELS: dict[str, str] = {
    "overall_risk": "Overall risk",
    "physical_activity": "Physical activity",
    "sleep": "Sleep",
    "nutrition": "Nutrition",
    "oxidative_stress": "Oxidative stress",
    "metabolic_age": "Metabolic age",
    "bmi_waist": "BMI and waist",
    "positive_wins": "Positive health wins",
    "participation": "Participation",
    "type_2_diabetes": "Type 2 Diabetes",
    "hypertension": "Hypertension",
    "obesity": "Obesity",
    "pcos_pcod": "PCOS/PCOD",
    "nafld": "NAFLD",
    "cardiac_health": "Cardiac health",
    "thyroid_health": "Thyroid health",
    "dyslipidemia": "Dyslipidemia",
    "metabolic_syndrome": "Metabolic syndrome",
}

CLUSTER_LABELS: dict[str, str] = {
    "metabolic": "metabolic",
    "cardiovascular": "cardiovascular",
    "hormonal": "hormonal",
    "lifestyle": "lifestyle",
    "recovery": "recovery",
    "mixed": "mixed",
    "healthy": "healthy",
}

# Patterns that must never reach the UI.
_INTERNAL_ID_PATTERN = re.compile(
    r"\b(overall_risk|physical_activity|oxidative_stress|metabolic_age|bmi_waist|"
    r"positive_wins|type_2_diabetes|pcos_pcod|cardiac_health|thyroid_health|"
    r"metabolic_syndrome|disease_deep_dive|disease_risks|recovery_strain|"
    r"cardio_nutrition|movement_priority|metabolic_cluster|cardio_cluster|"
    r"workforce_resilience|medicalFrameId|metricId|effectId)\b",
    re.IGNORECASE,
)

_SNAKE_CASE_KEY_PATTERN = re.compile(r"\b[a-z]+(?:_[a-z]+)+\s*:\s*")
_BAD_PAREN_JOIN_PATTERN = re.compile(r"\)\.\s+([a-z])")
_MULTI_SPACE_PATTERN = re.compile(r"\s{2,}")
_SPACE_BEFORE_PUNCT_PATTERN = re.compile(r"\s+([,.;:!?])")
_REPEATED_PUNCT_PATTERN = re.compile(r"([.!?]){2,}")


def human_metric_name(metric_id: str) -> str:
    disease = get_disease_knowledge(metric_id)
    if disease is not None:
        return disease.display_name
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None:
        return lifestyle.display_name
    return FALLBACK_LABELS.get(metric_id, "This health indicator")


def human_cluster_name(cluster: str | None) -> str:
    if not cluster:
        return "overall"
    return CLUSTER_LABELS.get(cluster, "overall")


def sanitize_insight_text(text: str) -> str:
    """Strip any leaked internal identifiers from user-facing text."""

    def _replace_id(match: re.Match[str]) -> str:
        key = match.group(0).lower()
        return human_metric_name(key)

    out = _INTERNAL_ID_PATTERN.sub(_replace_id, text)
    # Remove "Label:" prefixes that look like code keys (snake_case before colon)
    out = _SNAKE_CASE_KEY_PATTERN.sub("", out)
    # Fix common join artifacts: "). indicating" -> "), indicating"
    out = _BAD_PAREN_JOIN_PATTERN.sub(r"), \1", out)
    # Fix punct splices from joining already-terminated phrases
    out = re.sub(r"\.\s*,", ".", out)
    out = re.sub(r",\s*\.", ".", out)
    out = re.sub(r"\.\s*\.", ".", out)
    out = _MULTI_SPACE_PATTERN.sub(" ", out)
    out = _SPACE_BEFORE_PUNCT_PATTERN.sub(r"\1", out)
    out = _REPEATED_PUNCT_PATTERN.sub(r"\1", out)
    return out.strip()


def count_words(text: str) -> int:
    return len(text.split())


def limit_words(text: str, max_words: int) -> str:
    """Soft trim to max_words while keeping sentence boundaries when possible."""
    words = text.split()
    if len(words) <= max_words:
        return text.strip()

    truncated = " ".join(words[:max_words])
    last_sentence = re.match(r"^(.+[.!?])\s", truncated)
    if last_sentence and count_words(last_sentence.group(1)) >= int(max_words * 0.6):
        return last_sentence.group(1)
    return truncated if re.search(r"[.!?]$", truncated) else f"{truncated}."


# ---------------------------------------------------------------------------
# insight/generate_structured_insight.py
# ---------------------------------------------------------------------------

CONCERN_MAX_WORDS = 50
POSITIVE_MAX_WORDS = 50
NEUTRAL_MAX_WORDS = 50

INSUFFICIENT_DATA_MESSAGE = (
    "Insufficient data is currently available to generate a meaningful organisational insight."
)


def _ensure_single_ending(text: str, ending: str) -> str:
    """Append ``ending`` once — strip any existing trailing sentence punctuation first."""
    trimmed = (text or "").strip()
    if not trimmed:
        return ""
    bare = re.sub(r"[.!,;:]+$", "", trimmed).strip()
    if not bare:
        return ""
    return f"{bare}{ending}"


# Participial / prepositional openings intended to follow a comma — must stay
# lowercase. Capitalised forms ("Increasing blood-pressure risk…") are full
# sentences and must not be treated as dangling clauses.
_CLAUSE_OPENERS = re.compile(
    r"^(indicating|increasing|impairing|supporting|reflecting|signalling|signaling|"
    r"highlighting|pointing|raising|showing|making|forming|warranting|reinforcing|"
    r"helping|suggesting|contributing|influencing|which |an |a |with )"
)

_CLAUSE_TO_SENTENCE: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^indicating\s+", re.I), "This indicates "),
    (re.compile(r"^suggesting\s+", re.I), "This suggests "),
    (re.compile(r"^highlighting\s+", re.I), "This highlights "),
    (re.compile(r"^reflecting\s+", re.I), "This reflects "),
    (re.compile(r"^pointing to\s+", re.I), "This points to "),
    (re.compile(r"^pointing\s+", re.I), "This points "),
    (re.compile(r"^signalling\s+", re.I), "This signals "),
    (re.compile(r"^signaling\s+", re.I), "This signals "),
    (re.compile(r"^showing\s+", re.I), "This shows "),
    (re.compile(r"^increasing\s+", re.I), "This increases "),
    (re.compile(r"^reinforcing\s+", re.I), "This reinforces "),
    (re.compile(r"^supporting\s+", re.I), "This supports "),
    (re.compile(r"^raising\s+", re.I), "This raises "),
    (re.compile(r"^impairing\s+", re.I), "This impairs "),
    (re.compile(r"^contributing\s+", re.I), "This contributes "),
    (re.compile(r"^influencing\s+", re.I), "This influences "),
    (re.compile(r"^helping\s+", re.I), "This helps "),
    (re.compile(r"^warranting\s+", re.I), "This warrants "),
    (re.compile(r"^making\s+", re.I), "This makes "),
    (re.compile(r"^forming\s+", re.I), "This forms "),
    (re.compile(r"^which may indicate\s+", re.I), "This may indicate "),
    (re.compile(r"^which may signal\s+", re.I), "This may signal "),
    (re.compile(r"^which may highlight\s+", re.I), "This may highlight "),
    (re.compile(r"^which may point\s+", re.I), "This may point "),
    (re.compile(r"^which may increase\s+", re.I), "This may increase "),
    (re.compile(r"^which may raise\s+", re.I), "This may raise "),
    (re.compile(r"^which may show\s+", re.I), "This may show "),
    (re.compile(r"^which may impair\s+", re.I), "This may impair "),
    (re.compile(r"^which\s+", re.I), "This "),
    (re.compile(r"^potentially an\s+", re.I), "This may be an "),
    (re.compile(r"^potentially a\s+", re.I), "This may be a "),
    (re.compile(r"^an\s+", re.I), "This reflects an "),
    (re.compile(r"^a\s+", re.I), "This reflects a "),
    (re.compile(r"^with\s+", re.I), "This comes with "),
]


def _is_clause(text: str) -> bool:
    """True only for lowercase participial/prepositional openings.

    Capitalised openings (``Increasing risk suggests…``) are complete sentences.
    """
    t = (text or "").strip()
    if not t:
        return False
    return bool(_CLAUSE_OPENERS.match(t))


def _promote_clause_to_sentence(text: str) -> str:
    """Turn a dangling clause into a complete executive sentence."""
    t = _strip_trailing_punct((text or "").strip())
    if not t:
        return ""
    if not _is_clause(t):
        return _capitalize(t)
    out = t
    for pattern, replacement in _CLAUSE_TO_SENTENCE:
        if pattern.match(out):
            out = pattern.sub(replacement, out, count=1)
            break
    else:
        out = f"This reflects {out}"
    return _capitalize(out)


def _normalize_narrative_sentence(text: str) -> str:
    """Ensure a field is a complete sentence ending with a single full stop."""
    t = (text or "").strip()
    if not t:
        return ""
    t = _strip_trailing_punct(t)
    if not t:
        return ""
    if _is_clause(t):
        t = _promote_clause_to_sentence(t)
    else:
        t = _capitalize(t)
    return _ensure_sentence(t)


def _join_explanation_parts(primary: str, secondary: str | None = None) -> str:
    """Join why-phrases without creating ``.,`` splices or dangling fragments.

    - Two complete sentences → spaced sentences (``A. B``).
    - Complete sentence + clause → comma attachment (``A, clause``).
    - Clause-only material → promoted to a full sentence.
    """
    parts = [p.strip() for p in (primary, secondary) if p and p.strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        part = parts[0]
        return _promote_clause_to_sentence(part) if _is_clause(part) else _strip_trailing_punct(part)

    first, second = parts[0], parts[1]
    first_bare = _strip_trailing_punct(first)
    second_bare = _strip_trailing_punct(second)
    first_is_clause = _is_clause(first_bare)
    second_is_clause = _is_clause(second_bare)

    if first_is_clause and second_is_clause:
        return _promote_clause_to_sentence(f"{first_bare}, {second_bare}")
    if first_is_clause and not second_is_clause:
        # Prefer the complete sentence; drop the orphan clause rather than splice badly.
        return second_bare
    if not first_is_clause and second_is_clause:
        return f"{first_bare}, {second_bare}"
    # Two complete sentences — never join with a comma.
    return f"{first_bare}. {_capitalize(second_bare)}"


def ensure_structured_field_punctuation(
    observation: str,
    explanation: str,
    recommendation: str,
) -> tuple[str, str, str]:
    """Ensure structured O → E → R fields are complete, well-punctuated sentences.

    Each non-empty field becomes a standalone sentence ending with a single full
    stop. Participial fragments are promoted (``indicating X`` → ``This indicates X``).
    Content wording is preserved aside from that grammatical normalisation.
    """
    return (
        _normalize_narrative_sentence(observation),
        _normalize_narrative_sentence(explanation),
        _normalize_narrative_sentence(recommendation),
    )


def _unique_preserve(items: list | None) -> list:
    """Deduplicate while preserving order."""
    seen: set = set()
    out: list = []
    for item in items or []:
        if item in seen or item is None or item == "":
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_compare(text: str) -> str:
    """Loose comparison key to detect near-duplicate O/E/R sentences."""
    t = re.sub(r"[^a-z0-9\s]", "", (text or "").lower())
    return re.sub(r"\s+", " ", t).strip()


def _shares_action_theme(explanation: str, recommendation: str) -> bool:
    """True when recommendation restates programme advice already in the explanation."""
    expl = _normalize_compare(explanation)
    rec = _normalize_compare(recommendation)
    if not expl or not rec:
        return False
    if expl == rec:
        return True
    themes = (
        "nutrition",
        "movement",
        "physical activity",
        "sleep",
        "recovery",
        "screening",
        "weight",
        "stress",
        "preventive",
    )
    shared = [t for t in themes if t in expl and t in rec]
    action_words = ("priorit", "promote", "strengthen", "focus", "should be", "key focus")
    expl_actionable = any(w in expl for w in action_words)
    return bool(shared) and expl_actionable


_ACTION_LIKE_EXPLANATION = re.compile(
    r"\b(should be|should become|must be|key focus|a priority for|prioritise|prioritize|promote)\b",
    re.IGNORECASE,
)

# Deterministic opener pools — medical clause after the opener is unchanged.
_WHY_STEM_POOLS: dict[str, list[str]] = {
    "indicates": [
        "Elevated findings indicate",
        "The observed pattern indicates",
        "Workforce data indicate",
        "Current results indicate",
    ],
    "suggests": [
        "Available evidence suggests",
        "The distribution suggests",
        "Clinical patterns suggest",
        "These findings suggest",
    ],
    "supports": [
        "The profile supports",
        "Current results support",
        "Healthy bands support",
        "The distribution supports",
    ],
    "highlights": [
        "The pattern highlights",
        "These findings highlight",
        "Workforce data highlight",
        "The distribution highlights",
    ],
    "shows": [
        "The distribution shows",
        "Current data show",
        "Workforce results show",
        "The profile shows",
    ],
    "increases": [
        "The pattern increases",
        "Elevated findings increase",
        "This distribution increases",
        "Sustained elevation increases",
    ],
    "affects": [
        "The pattern affects",
        "Elevated findings affect",
        "Poor sleep patterns affect",
        "The distribution affects",
    ],
    "contributes": [
        "The pattern contributes",
        "Elevated findings contribute",
        "This distribution contributes",
        "Sustained elevation contributes",
    ],
    "influences": [
        "The pattern influences",
        "Dietary findings influence",
        "These results influence",
        "The distribution influences",
    ],
    "reflects": [
        "The pattern reflects",
        "Current findings reflect",
        "Workforce data reflect",
        "The distribution reflects",
    ],
    "identifies": [
        "The pattern identifies",
        "Current findings identify",
        "Workforce data identify",
        "The distribution identifies",
    ],
}

_LEVER_PLAIN_LABELS: dict[str, str] = {
    "metabolic_screening": "metabolic screening",
    "lipid_screening": "lipid screening",
    "bp_screening": "blood-pressure screening",
    "thyroid_screening": "thyroid screening",
    "liver_screening": "liver health monitoring",
    "cardiac_screening": "cardiovascular screening",
    "nutrition_refined_carb": "nutrition",
    "nutrition_heart_healthy": "heart-healthy nutrition",
    "nutrition_sodium": "sodium-aware nutrition",
    "nutrition_whole_food": "whole-food nutrition",
    "movement_programme": "daily movement",
    "weight_management": "weight-management",
    "sleep_health": "sleep hygiene",
    "stress_management": "stress-management",
    "recovery_programme": "recovery",
    "womens_health": "women's-health support",
    "smoking_cessation": "smoking-cessation support",
    "alcohol_moderation": "alcohol-moderation education",
    "maintain_wellness": "preventive wellness monitoring",
    "target_high_risk": "targeted follow-up for elevated-risk groups",
    "scale_preventive_care": "scaled preventive care",
    "clinical_review": "clinical review pathways",
}


def _restyle_why_opening(text: str, plan: InsightPlan) -> str:
    """Rotate 'This indicates…' stems without changing the medical clause."""
    bare = _strip_trailing_punct(text or "")
    if not bare:
        return bare
    match = re.match(
        r"^This\s+(indicates|suggests|supports|highlights|shows|increases|affects|"
        r"contributes|influences|reflects|identifies)\b(.*)$",
        bare,
        re.IGNORECASE,
    )
    if not match:
        return bare
    stem = match.group(1).lower()
    remainder = match.group(2).strip()
    pool = _WHY_STEM_POOLS.get(stem)
    if not pool:
        return bare
    opener = select_variant(
        pool,
        plan.observation.metric_id,
        plan.section_id,
        plan.severity_band,
        _profile_cluster(plan),
        stem,
    )
    if not remainder:
        return opener
    return f"{opener} {remainder}"


def _generate_structured_insight_from_plan(
    plan: InsightPlan,
    *,
    used_phrases: frozenset[str] | None = None,
    used_why_stems: frozenset[str] | None = None,
) -> StructuredInsight:
    """Plan -> StructuredInsight (Observation / Why / Action)."""
    if _is_insufficient_plan(plan):
        return _insufficient_structured(plan)

    observation = sanitize_insight_text(_build_observation(plan))
    explanation = sanitize_insight_text(
        _build_explanation(plan, used_why_stems=used_why_stems)
    )
    render_levers = _select_render_levers(plan)
    recommendation = sanitize_insight_text(
        _build_recommendation(
            plan,
            render_levers=render_levers,
            used_phrases=used_phrases,
            explanation=explanation,
        )
    )

    if plan.confidence.band == "low":
        explanation = _soften_explanation(explanation)
        recommendation = _soften_recommendation(recommendation)

    observation, explanation, recommendation = ensure_structured_field_punctuation(
        observation, explanation, recommendation
    )

    # Final guard: recommendation must not restate explanation.
    if _shares_action_theme(explanation, recommendation):
        alt = sanitize_insight_text(
            _build_recommendation(
                plan,
                render_levers=render_levers,
                used_phrases=frozenset(
                    list(used_phrases or []) + [_strip_trailing_punct(recommendation)]
                ),
                explanation=explanation,
                force_alternate=True,
            )
        )
        _, _, recommendation = ensure_structured_field_punctuation(
            observation, explanation, alt
        )

    return StructuredInsight(
        section_id=plan.section_id,
        tone=plan.tone,
        severity_band=plan.severity_band,
        confidence=plan.confidence,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        effect_ids=_unique_preserve(plan.explanation.effect_ids),
        lever_ids=_unique_preserve(render_levers or plan.recommendation.lever_ids),
        related_metrics=[plan.observation.metric_id],
    )


def _profile_cluster(plan: InsightPlan) -> str:
    """Company-profile signal available on the plan (dominant cluster). Used only
    as a deterministic selection key so wording can vary by company without ever
    affecting reasoning."""
    return (plan.explanation.cluster or "mixed").lower()


def _population_noun(facts: dict) -> str:
    """Denominator label for a percentage, taken from existing analyzer facts.

    Does not invent a population. Defaults to employees when both genders (or
    neither) are represented.
    """
    explicit = str(facts.get("population_label") or facts.get("denominator_label") or "").strip()
    if explicit:
        return explicit
    male_has = facts.get("male_has_data")
    female_has = facts.get("female_has_data")
    if male_has is False and female_has is not False:
        return "assessed female employees"
    if female_has is False and male_has is not False:
        return "assessed male employees"
    view = str(facts.get("graph_view") or "").strip().lower()
    if view == "female":
        return "assessed female employees"
    if view == "male":
        return "assessed male employees"
    return "employees"


def _of_population(facts: dict) -> str:
    return f"of {_population_noun(facts)}"


def _few_elevated(
    elevated_value: float,
    elevated: str,
    predicate: str,
    *,
    population: str = "employees",
) -> str:
    """Lead a positive observation with the elevated share, without claiming a majority."""
    if elevated_value <= 0:
        return f"No {population} {predicate}"
    return f"Only {elevated}% of {population} {predicate}"


def _share_lead(plan: InsightPlan) -> str:
    """Deterministic synonym for the 'a substantial elevated group' quantifier.

    The exact share always follows in the sentence, so precision is preserved —
    only the qualitative framing rotates. The population noun follows the
    analyzer denominator when it is not the full workforce.
    """
    lead = select_variant(
        ELEVATED_SHARE_LEADS,
        plan.observation.metric_id,
        plan.severity_band,
        plan.section_id,
        _profile_cluster(plan),
    )
    population = _population_noun(plan.observation.facts)
    if population and population != "employees":
        lead = lead.replace("employees", population)
    return lead


def _oxidative_observation(plan: InsightPlan, *, elevated: str, healthy: str, majority_healthy: bool) -> str:
    """Present oxidative shares without claiming Low is dominant when it is not."""
    f = plan.observation.facts
    dominant = str(f.get("dominant_label", "") or "")
    dominant_pct = _fmt(float(f.get("dominant_percent", 0) or 0))
    population = _population_noun(f)
    if plan.mode == "positive":
        if majority_healthy:
            return (
                f"Oxidative stress remains well controlled for most {population} "
                f"({healthy}% in healthy bands)"
            )
        lead = _few_elevated(
            float(f.get("elevated_share", 0) or 0),
            elevated,
            "show high oxidative stress",
            population=population,
        )
        if dominant and dominant.lower() != "low":
            return (
                f"{lead}, though only {healthy}% sit in the healthy range, "
                f"with {dominant} the most common category at {dominant_pct}%"
            )
        return f"{lead}, though only {healthy}% sit in the healthy range"
    base = f"Elevated oxidative stress affects {elevated}% of {population}"
    if dominant and dominant.lower() not in ("high", "very high"):
        return f"{base}, with {dominant} the most common category at {dominant_pct}%"
    return base


def _strength_observation(facts: dict) -> str:
    names = str(facts.get("low_risk_names") or "").strip()
    profiles = str(facts.get("profile_names") or "").strip()
    habits = str(facts.get("habit_labels") or "").strip()
    parts: list[str] = []
    if names:
        parts.append(f"Low-risk disease areas include {names}")
    else:
        low_risk = int(facts.get("low_risk_count", 0) or 0)
        if low_risk:
            parts.append(f"{low_risk} disease areas remain predominantly healthy")
    if profiles:
        parts.append(f"in-range lab profiles include {profiles}")
    else:
        count = int(facts.get("profiles_count", 0) or 0)
        if count:
            parts.append(f"{count} lab profiles show strong in-range rates")
    if habits:
        parts.append(f"healthy habits include {habits}")
    if not parts:
        return "No positive-win signals are listed in this report"
    text = parts[0]
    for part in parts[1:]:
        text += ". " + part[0].upper() + part[1:]
    return text


def _build_observation(plan: InsightPlan) -> str:
    f = plan.observation.facts
    elevated_value = float(f.get("elevated_share", 0) or 0)
    healthy_value = float(f.get("healthy_share", 0) or 0)
    elevated = _fmt(elevated_value)
    healthy = _fmt(healthy_value)
    # A positive tone can come from a low elevated share alone, so "most" is only
    # truthful when the healthy band actually holds a majority.
    majority_healthy = healthy_value > 50
    metric = human_metric_name(plan.observation.metric_id)
    dominant = str(f.get("dominant_label", "") or "")
    people = _of_population(f)
    population = _population_noun(f)

    kind = plan.observation.kind

    if kind == "disease_lead":
        return f"{metric} leads with {elevated}% {people} in elevated risk bands"

    if kind == "strength":
        return _strength_observation(f)

    if kind == "status":
        if plan.section_id == "participation":
            group = str(f.get("top_age_group", "") or "").strip()
            if not group:
                return ""
            return f"The largest enrolled age group is {group} at {_fmt(float(f.get('top_percent', 0) or 0))}%"
        return f"{elevated}% {people} sit in elevated risk bands"

    if kind == "cluster":
        return (
            f"{elevated}% {people} sit in elevated risk bands, with a "
            f"{human_cluster_name(str(f.get('dominant_cluster', '')))} health profile"
        )

    if kind == "lifestyle":
        if plan.observation.metric_id == "oxidative_stress":
            return _oxidative_observation(
                plan, elevated=elevated, healthy=healthy, majority_healthy=majority_healthy
            )
        if plan.mode == "positive":
            if plan.observation.metric_id == "physical_activity":
                if majority_healthy:
                    return f"Most {population} maintain healthy physical activity levels ({healthy}% in healthy bands)"
                return (
                    f"{_few_elevated(elevated_value, elevated, 'report poor physical activity', population=population)}, "
                    f"though just {healthy}% reach healthy activity levels"
                )
            if plan.observation.metric_id == "sleep":
                if majority_healthy:
                    return f"Most {population} achieve healthy sleep duration ({healthy}% in the recommended range)"
                return (
                    f"{_few_elevated(elevated_value, elevated, 'sleep outside the recommended range', population=population)}, "
                    f"though just {healthy}% sit within it"
                )
            if majority_healthy:
                return f"Most {population} show healthy {metric.lower()} patterns ({healthy}% in healthy bands)"
            return (
                f"{_few_elevated(elevated_value, elevated, f'show elevated {metric.lower()}', population=population)}, "
                f"though just {healthy}% sit in healthy bands"
            )
        if plan.observation.metric_id == "physical_activity":
            return (
                f"{_share_lead(plan)} report low physical activity "
                f"({elevated}% {people} in elevated bands)"
            )
        if plan.observation.metric_id == "sleep":
            return (
                f"{_share_lead(plan)} fall outside the recommended sleep range "
                f"({elevated}% {people})"
            )
        if plan.observation.metric_id == "nutrition":
            return f"Nutrition patterns leave {elevated}% {people} below a healthy score range"
        return f"{elevated}% {people} show elevated concern for {metric.lower()}"

    # 'distribution' and default
    if plan.mode == "positive":
        if plan.observation.metric_id == "overall_risk":
            if majority_healthy:
                return f"Most {population} remain in healthy risk bands, while {elevated}% fall within elevated risk categories"
            return f"{healthy}% {people} remain in healthy risk bands, while {elevated}% fall within elevated risk categories"
        if is_disease_metric(plan.observation.metric_id):
            narrative = select_variant(
                disease_positive_narratives(plan.observation.metric_id),
                plan.observation.metric_id,
                plan.severity_band,
                plan.section_id,
                _profile_cluster(plan),
            )
            if narrative:
                return f"{_strip_trailing_punct(narrative)} ({healthy}% in healthy bands)"
        return f"{metric} remains favourable, with {healthy}% {people} in healthy bands"

    if plan.observation.metric_id == "overall_risk":
        if float(elevated) <= 40:
            return f"{elevated}% {people} fall within Increased or High risk bands"
        suffix = f", with {dominant} as a major share" if dominant else ""
        return f"Elevated risk affects {elevated}% {people}{suffix}"

    if plan.observation.metric_id == "oxidative_stress":
        return _oxidative_observation(
            plan, elevated=elevated, healthy=healthy, majority_healthy=majority_healthy
        )

    concern_frames = [
        f"{metric} shows {elevated}% {people} in elevated risk bands",
        f"{metric} places {elevated}% {people} in elevated risk bands",
        f"Elevated {metric.lower()} risk reaches {elevated}% {people}",
    ]
    dominant_is_healthy = dominant.lower() in ("healthy", "low", "optimal", "low risk")
    if dominant and not dominant_is_healthy:
        concern_frames = [f"{frame}, with {dominant} as a major share" for frame in concern_frames]
    return select_variant(
        concern_frames,
        plan.observation.metric_id,
        plan.severity_band,
        plan.section_id,
        _profile_cluster(plan),
    )


def _metric_specific_medical_frame(frame_id: str | None) -> str:
    """Return a knowledge-layer medical frame when it is specific to this id."""
    if not frame_id:
        return ""
    generic = medical_frame("disease_generic")
    text = medical_frame(frame_id)
    if not text:
        return ""
    if frame_id != "disease_generic" and text.strip() == generic.strip():
        return ""
    return _strip_trailing_punct(text)


def _select_interpretation_clause(plan: InsightPlan) -> str:
    """Disease interpretation from knowledge.py — medical meaning, not actions."""
    if not is_disease_metric(plan.observation.metric_id):
        return ""
    clauses = [
        clause
        for clause in disease_interpretation_clauses(plan.observation.metric_id)
        if clause
        and _is_clause(clause)
        and not _ACTION_LIKE_EXPLANATION.search(clause)
    ]
    if not clauses:
        return ""
    return select_variant(
        clauses,
        plan.observation.metric_id,
        plan.severity_band,
        plan.section_id,
        _profile_cluster(plan),
        "interpretation",
    )


def _clinical_focus_why(plan: InsightPlan) -> str:
    """Existing knowledge clinical_focus, used only as a wording fallback."""
    metric_id = plan.observation.metric_id
    disease = get_disease_knowledge(metric_id)
    if disease is not None and disease.clinical_focus:
        return _strip_trailing_punct(disease.clinical_focus)
    lifestyle = get_lifestyle_knowledge(metric_id)
    if lifestyle is not None and lifestyle.clinical_focus:
        return _strip_trailing_punct(lifestyle.clinical_focus)
    return ""


def _build_explanation(
    plan: InsightPlan,
    *,
    used_why_stems: frozenset[str] | None = None,
) -> str:
    """Compose the Why sentence — medical significance only, never programme advice."""
    metric_id = plan.observation.metric_id
    frame_id = plan.explanation.medical_frame_id
    effect_ids = _unique_preserve(plan.explanation.effect_ids)
    effect = effect_ids[0] if effect_ids else None

    if plan.mode == "positive":
        positive_frames = {
            "positive_wins",
            "maintain",
            "participation",
            "workforce_resilience",
        }
        knowledge_why = ""
        if str(frame_id).endswith("_healthy"):
            knowledge_why = _metric_specific_medical_frame(frame_id)
        if not knowledge_why:
            knowledge_why = _metric_specific_medical_frame(f"{metric_id}_healthy")
        short_why = (
            knowledge_why
            or SHORT_WHY.get(f"{frame_id}_healthy")
            or SHORT_WHY.get(f"{metric_id}_healthy")
            or (SHORT_WHY.get(frame_id) if str(frame_id).endswith("_healthy") else None)
            or (SHORT_WHY.get(frame_id) if frame_id in positive_frames else None)
        )
        if not short_why and is_disease_metric(metric_id):
            metric = human_metric_name(metric_id)
            short_why = select_variant(
                [
                    f"Favourable {metric} bands reduce near-term clinical escalation risk across the workforce",
                    f"A predominantly healthy {metric} profile is a protective feature of the current cohort",
                    f"Low elevated {metric} prevalence supports continued preventive momentum",
                ],
                metric_id,
                plan.section_id,
                plan.severity_band,
                _profile_cluster(plan),
            )
        if not short_why:
            short_why = (
                _metric_specific_medical_frame("maintain")
                or SHORT_WHY.get("maintain")
                or SHORT_WHY.get("positive_wins")
            )
        styled = _restyle_why_opening(_strip_trailing_punct(short_why or ""), plan)
        return _avoid_used_why_stem(styled, plan, used_why_stems)

    knowledge_why = _metric_specific_medical_frame(frame_id) or _metric_specific_medical_frame(
        metric_id
    )
    short_why = knowledge_why or SHORT_WHY.get(frame_id) or SHORT_WHY.get(metric_id)
    if short_why:
        effect_clause = EFFECT_CLAUSES.get(effect) if effect else None
        # Never fold programme advice into the explanation.
        extra = None
        if (
            effect_clause
            and effect != frame_id
            and not _ACTION_LIKE_EXPLANATION.search(effect_clause)
        ):
            extra = effect_clause
        else:
            extra = _select_interpretation_clause(plan) or None
        if extra:
            joined = _join_explanation_parts(short_why, extra)
        else:
            joined = _strip_trailing_punct(short_why)
        styled = _restyle_why_opening(joined, plan)
        return _avoid_used_why_stem(styled, plan, used_why_stems)

    styled = _restyle_why_opening(_strip_trailing_punct(medical_frame(frame_id)), plan)
    return _avoid_used_why_stem(styled, plan, used_why_stems)


def _avoid_used_why_stem(
    text: str,
    plan: InsightPlan,
    used_why_stems: frozenset[str] | None,
) -> str:
    """If this report already used the same opener stem, rotate once more."""
    bare = _strip_trailing_punct(text)
    if not bare or not used_why_stems:
        return bare
    stem = bare.split(" ", 2)
    key = " ".join(stem[:2]).lower() if len(stem) >= 2 else bare.lower()
    if key not in used_why_stems:
        return bare
    retry = _restyle_why_opening(bare, plan)
    if _normalize_compare(retry) != _normalize_compare(bare):
        return _strip_trailing_punct(retry)

    metric_id = plan.observation.metric_id
    metric = human_metric_name(metric_id)
    if plan.mode == "positive":
        knowledge_retry = (
            _metric_specific_medical_frame(f"{metric_id}_healthy")
            or _metric_specific_medical_frame("maintain")
        )
        if knowledge_retry and _normalize_compare(knowledge_retry) != _normalize_compare(bare):
            return knowledge_retry
        return select_variant(
            [
                f"A predominantly healthy {metric} profile is a protective feature of the current cohort",
                f"Favourable {metric} bands reduce near-term clinical escalation risk across the workforce",
                f"Low elevated {metric} prevalence supports continued preventive momentum",
            ],
            metric_id,
            plan.section_id,
            "retry-positive",
            _profile_cluster(plan),
        )

    knowledge_retry = (
        _metric_specific_medical_frame(plan.explanation.medical_frame_id)
        or _metric_specific_medical_frame(metric_id)
        or _clinical_focus_why(plan)
    )
    if knowledge_retry and _normalize_compare(knowledge_retry) != _normalize_compare(bare):
        return knowledge_retry
    interpretation = _select_interpretation_clause(plan)
    if interpretation:
        promoted = _promote_clause_to_sentence(interpretation)
        if promoted and _normalize_compare(promoted) != _normalize_compare(bare):
            return _strip_trailing_punct(promoted)
    return select_variant(
        [
            f"Clinically, the {metric} pattern warrants preventive attention",
            f"The {metric} pattern has clear implications for long-term metabolic and cardiovascular health",
        ],
        metric_id,
        plan.section_id,
        "retry-concern",
        _profile_cluster(plan),
    )


def _select_render_levers(plan: InsightPlan) -> list[str]:
    """Choose levers for narrative rendering.

    For disease sections in concern mode: disease-native defaults first; company
    levers from the plan are kept as modifiers (never as the sole replacement).
    Reasoning output is unchanged — this only affects how text is composed.
    """
    plan_levers = _unique_preserve(list(plan.recommendation.lever_ids or []))
    section = plan.section_id
    metric_id = plan.observation.metric_id

    if plan.mode == "positive" or section not in ("disease_risks", "disease_deep_dive"):
        return plan_levers[:2]

    if not is_disease_metric(metric_id) and section != "disease_risks":
        return plan_levers[:2]

    defaults = list(get_default_levers(metric_id).get("high") or [])
    native = _unique_preserve(defaults)[:3]
    if not native:
        return plan_levers[:2]

    # Company / graph levers that are not disease-native become modifiers.
    modifiers = [
        lev
        for lev in plan_levers
        if lev not in native and lev != "maintain_wellness"
    ]
    one = plan.recommendation.one_thing
    if one and one not in native and one not in modifiers:
        modifiers.insert(0, one)

    # Render order: native disease levers, then at most one company modifier.
    ordered = native[:2]
    for mod in modifiers[:1]:
        if mod not in ordered:
            ordered.append(mod)
    return ordered


def _compose_disease_first_recommendation(
    plan: InsightPlan,
    render_levers: list[str],
) -> str:
    """Disease-native programmes first; company strategy as a trailing modifier."""
    metric = human_metric_name(plan.observation.metric_id)
    cluster = human_cluster_name(plan.explanation.cluster or "mixed").lower()
    defaults = list(get_default_levers(plan.observation.metric_id).get("high") or [])
    native = [lev for lev in render_levers if lev in defaults] or defaults[:3]
    native = _unique_preserve(native)[:3]
    labels = [_LEVER_PLAIN_LABELS.get(lev, lev.replace("_", " ")) for lev in native]
    labels = [lab for lab in labels if lab]

    verb = select_variant(
        ["Strengthen", "Expand", "Reinforce", "Scale"],
        plan.observation.metric_id,
        plan.section_id,
        plan.severity_band,
        _profile_cluster(plan),
    )

    if not labels:
        core = f"{verb} targeted prevention programmes for employees with elevated {metric} risk"
    elif len(labels) == 1:
        core = f"{verb} {labels[0]} programmes for employees with elevated {metric} risk"
    elif len(labels) == 2:
        core = (
            f"{verb} {labels[0]} and {labels[1]} programmes for employees "
            f"with elevated {metric} risk"
        )
    else:
        core = (
            f"{verb} {labels[0]}, {labels[1]} and {labels[2]} programmes "
            f"for employees with elevated {metric} risk"
        )

    company = plan.recommendation.one_thing
    company_outside = company and company not in native
    cluster_mod = cluster in ("cardiovascular", "metabolic", "hormonal")
    if company_outside or (cluster_mod and plan.section_id in ("disease_risks", "disease_deep_dive")):
        return (
            f"{core}, while integrating these interventions into broader "
            f"{cluster} prevention efforts"
        )
    return core


def _build_recommendation(
    plan: InsightPlan,
    *,
    render_levers: list[str] | None = None,
    used_phrases: frozenset[str] | None = None,
    explanation: str | None = None,
    force_alternate: bool = False,
) -> str:
    """HR action sentence — distinct from explanation; disease-first for disease sections."""
    levers = render_levers if render_levers is not None else _select_render_levers(plan)
    if not levers:
        return "Continue routine workforce health monitoring with targeted follow-up where risk rises"

    section = plan.section_id
    if (
        section in ("disease_risks", "disease_deep_dive")
        and plan.mode != "positive"
        and is_disease_metric(plan.observation.metric_id)
    ):
        primary = _compose_disease_first_recommendation(plan, levers)
        variants = _unique_phrases(
            [primary, *_recommendation_variants(plan, levers[0], disease_first=True)]
        )
    else:
        variants = _recommendation_variants(plan, levers[0], disease_first=False)

    used = set(used_phrases or [])
    if force_alternate and variants:
        used.add(_strip_trailing_punct(variants[0]))

    for phrase in variants:
        bare = _strip_trailing_punct(phrase)
        if bare in used:
            continue
        if explanation and _shares_action_theme(explanation, bare):
            continue
        return bare
    return _strip_trailing_punct(variants[0]) if variants else (
        "Continue routine workforce health monitoring with targeted follow-up where risk rises"
    )


def _recommendation_variants(
    plan: InsightPlan,
    lever: str,
    *,
    disease_first: bool = False,
) -> list[str]:
    """Ordered phrase candidates for a lever — first unused wins."""
    role = plan.recommendation.action_role or "section_support"
    cluster = human_cluster_name(plan.explanation.cluster or "mixed").lower()
    metric = human_metric_name(plan.observation.metric_id)
    section = plan.section_id
    lifestyle = plan.recommendation.lifestyle_priority or ""
    severity = plan.severity_band
    base = SHORT_ACTIONS.get(lever) or _soften(intervention_phrase(lever))
    enriched = (
        select_variant(
            lever_action_variants(lever), lever, section, role, severity, cluster, metric
        )
        or base
    )

    variants: list[str] = []

    # Disease concern sections only: disease/enriched phrasing first.
    if disease_first:
        variants.append(enriched)
        variants.append(base)
        keyed = SECTION_SUPPORT_ACTIONS.get((lever, section))
        if keyed:
            variants.append(keyed.format(cluster=cluster, metric=metric))
        metric_tied = _metric_tied_support_action(
            lever, metric, section, cluster, lifestyle, prefer_disease=True
        )
        if metric_tied:
            variants.append(metric_tied)
        return _unique_phrases(variants)

    if role == "org_primary":
        org = ORG_PRIMARY_ACTIONS.get(lever)
        if org:
            variants.append(
                org.format(cluster=cluster, metric=metric, severity=severity.replace("_", "-"))
            )
        variants.append(
            f"Given this company's {cluster} health profile, "
            f"{enriched[0].lower() + enriched[1:] if enriched else enriched}"
        )

    if role == "section_support":
        keyed = SECTION_SUPPORT_ACTIONS.get((lever, section))
        if keyed:
            variants.append(keyed.format(cluster=cluster, metric=metric))
        metric_tied = _metric_tied_support_action(
            lever, metric, section, cluster, lifestyle, prefer_disease=False
        )
        if metric_tied:
            variants.append(metric_tied)
        if not keyed and not metric_tied:
            variants.append(enriched)

    if role == "maintain":
        if section == "disease_deep_dive":
            variants.extend(
                [
                    f"Keep {metric} under routine preventive monitoring while most employees remain in healthy bands",
                    f"Protect the favourable {metric} profile with light-touch screening and lifestyle reinforcement",
                    f"Retain existing {metric} controls so this area does not become an organisational priority",
                ]
            )
        maintain = MAINTAIN_ACTIONS.get(section)
        if maintain:
            variants.append(maintain.format(cluster=cluster, metric=metric))
        variants.append(
            "Continue preventive wellness initiatives with routine workforce health monitoring"
        )

    variants.append(enriched)
    variants.append(base)
    return _unique_phrases(variants)


def _metric_tied_support_action(
    lever: str,
    metric: str,
    section: str,
    cluster: str,
    lifestyle: str,
    *,
    prefer_disease: bool = False,
) -> str | None:
    """Section/metric-aware supporting action — complementary to the org primary."""
    if section == "disease_deep_dive" or (section == "disease_risks" and prefer_disease):
        if lever == "lipid_screening":
            return f"Expand lipid screening pathways for employees with elevated {metric} risk"
        if lever == "bp_screening":
            return f"Expand blood-pressure screening pathways alongside {metric} prevention"
        if lever == "metabolic_screening":
            return f"Expand metabolic screening for employees with elevated {metric} risk"
        if lever == "cardiac_screening":
            return f"Expand cardiovascular screening for employees with elevated {metric} risk"
        if lever == "thyroid_screening":
            return f"Include thyroid screening for employees with elevated {metric} risk"
        if lever == "liver_screening":
            return f"Expand liver health monitoring alongside {metric} prevention"
        if lever.startswith("nutrition"):
            return f"Strengthen nutrition programmes for employees with elevated {metric} risk"
        if lever == "movement_programme":
            return f"Strengthen daily movement programmes for employees with elevated {metric} risk"
        if lever == "weight_management":
            return f"Strengthen weight-management programmes for employees with elevated {metric} risk"
        if lever == "stress_management":
            return f"Add stress-management support alongside {metric} prevention"
        if lever == "sleep_health":
            return f"Strengthen sleep hygiene support for employees with elevated {metric} risk"
        return f"Target clinical and lifestyle support at elevated {metric} risk groups"

    # Generic cluster-replacement line removed for disease_risks — disease-first path handles it.
    if section == "disease_risks":
        return (
            f"Strengthen disease-specific programmes for elevated {metric} risk, "
            f"aligned with broader {cluster} prevention"
        )

    if section == "physical_activity" and lever == "movement_programme":
        return select_variant(
            [
                "Close the activity gap with daily movement programmes and active work breaks",
                "Build daily movement into the working day through structured activity programmes",
                "Raise activity levels with workplace movement programmes and active breaks",
            ],
            lever,
            section,
            metric,
        )

    if section == "sleep" and lever in ("sleep_health", "recovery_programme"):
        return select_variant(
            [
                "Improve sleep hygiene through recovery-focused wellbeing initiatives",
                "Raise recovery capacity with sleep-health programmes across the workforce",
                "Strengthen sleep and recovery support for employees outside healthy ranges",
            ],
            lever,
            section,
            metric,
        )

    if section == "oxidative_stress" and lever in ("recovery_programme", "nutrition_whole_food"):
        return "Strengthen recovery programmes that reduce oxidative load across the workforce"

    if section == "overall_risk" and lifestyle:
        return None

    if lever == "target_high_risk":
        return "Direct coaching and clinical follow-up to the highest-risk employee groups"

    return None


def _unique_phrases(phrases: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for phrase in phrases:
        cleaned = _strip_trailing_punct(phrase.strip())
        if not cleaned:
            continue
        key = _normalize_compare(cleaned)
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def compose_footer_text(insight: StructuredInsight) -> str:
    """Join Observation + Why + Action into one polished paragraph.

    Complete sentences are joined with spaces. Lowercase clause-style why text
    (rare after normalisation) still attaches with a comma.
    """
    if insight.observation == INSUFFICIENT_DATA_MESSAGE or not insight.observation.strip():
        return INSUFFICIENT_DATA_MESSAGE

    obs = _strip_trailing_punct(sanitize_insight_text(insight.observation))
    why_raw = sanitize_insight_text(insight.explanation)
    why = _strip_trailing_punct(why_raw)
    action = _ensure_sentence(sanitize_insight_text(insight.recommendation))

    if not why:
        body = _ensure_sentence(obs)
    elif _is_clause(why):
        body = f"{obs}, {why}."
    else:
        # Structured fields are already complete sentences — join without
        # introducing ``.,`` or lowercase mid-paragraph starts.
        body = f"{_ensure_sentence(obs)} {_ensure_sentence(_capitalize(why))}"

    text = _polish_punctuation(f"{body} {action}")
    text = sanitize_insight_text(text)

    max_words = word_limit_for_tone(insight.tone)
    if count_words(text) <= max_words:
        return text

    # Prefer observation + action when over budget
    compact = _polish_punctuation(f"{_ensure_sentence(obs)} {action}")
    if count_words(compact) <= max_words:
        return sanitize_insight_text(compact)

    return limit_words(text, max_words)


def word_limit_for_tone(tone: InsightTone) -> int:
    if tone == "positive":
        return POSITIVE_MAX_WORDS
    if tone == "neutral":
        return NEUTRAL_MAX_WORDS
    return CONCERN_MAX_WORDS


def _is_insufficient_plan(plan: InsightPlan) -> bool:
    f = plan.observation.facts
    if plan.observation.kind == "status" and plan.section_id == "participation":
        return not str(f.get("top_age_group", "") or "").strip()
    if plan.observation.kind == "strength":
        return (
            int(f.get("low_risk_count", 0) or 0) == 0
            and int(f.get("profiles_count", 0) or 0) == 0
            and int(f.get("habits_count", 0) or 0) == 0
            and not str(f.get("low_risk_names") or "").strip()
            and not str(f.get("habit_labels") or "").strip()
            and not str(f.get("profile_names") or "").strip()
        )
    # No numeric signal at all
    elevated = f.get("elevated_share")
    healthy = f.get("healthy_share")
    if elevated is None and healthy is None and not f.get("display_name"):
        return True
    return False


def _insufficient_structured(plan: InsightPlan) -> StructuredInsight:
    observation, explanation, recommendation = ensure_structured_field_punctuation(
        INSUFFICIENT_DATA_MESSAGE, "", ""
    )
    return StructuredInsight(
        section_id=plan.section_id,
        tone="neutral",
        severity_band=plan.severity_band,
        confidence=plan.confidence,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        effect_ids=[],
        lever_ids=[],
        related_metrics=[plan.observation.metric_id],
    )


def _soften_explanation(text: str) -> str:
    """Hedge confident verbs for low-confidence insights — keep full sentences."""
    replacements = [
        (r"^This indicates ", "This may indicate "),
        (r"^This suggests ", "This may suggest "),
        (r"^This highlights ", "This may highlight "),
        (r"^This shows ", "This may show "),
        (r"^This increases ", "This may increase "),
        (r"^This raises ", "This may raise "),
        (r"^This affects ", "This may affect "),
        (r"^This contributes ", "This may contribute "),
        (r"^This influences ", "This may influence "),
        (r"^This reflects ", "This may reflect "),
        (r"^This supports ", "This may support "),
        (r"^This identifies ", "This may identify "),
        # Legacy participial openings (pre-promotion)
        (r"^indicating ", "which may indicate "),
        (r"^signalling ", "which may signal "),
        (r"^signaling ", "which may signal "),
        (r"^highlighting ", "which may highlight "),
        (r"^pointing ", "which may point "),
        (r"^increasing ", "which may increase "),
        (r"^raising ", "which may raise "),
        (r"^showing ", "which may show "),
        (r"^impairing ", "which may impair "),
        (r"^an upstream", "potentially an upstream"),
        (r"^a primary", "potentially a primary"),
        (r"^a screenable", "potentially a screenable"),
    ]
    out = text
    for pattern, replacement in replacements:
        out = re.sub(pattern, replacement, out, count=1, flags=re.IGNORECASE)
    return out


def _soften_recommendation(text: str) -> str:
    replacements = [
        (r"^Prioritise ", "Consider prioritising "),
        (r"^Prioritize ", "Consider prioritizing "),
        (r"^Strengthen ", "Consider strengthening "),
        (r"^Promote ", "Consider promoting "),
        (r"^Support ", "Consider supporting "),
        (r"^Scale ", "Consider scaling "),
        (r"^Focus ", "Consider focusing "),
        (r"^Include ", "Consider including "),
        (r"^Introduce ", "Consider introducing "),
        (r"^Expand ", "Consider expanding "),
        (r"^Make ", "Consider making "),
        (r"^Reduce ", "Consider reducing "),
        (r"^Sustain ", "Consider sustaining "),
        (r"^Reinforce ", "Consider reinforcing "),
        (r"^Maintain ", "Consider maintaining "),
        (r"^Continue ", "Consider continuing "),
    ]
    out = text
    for pattern, replacement in replacements:
        out = re.sub(pattern, replacement, out, count=1, flags=re.IGNORECASE)
    return out


def _polish_punctuation(text: str) -> str:
    out = re.sub(r"\s+", " ", text)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    # Fix composition artifacts from joining punctuated phrases
    out = re.sub(r"\.\s*,", ".", out)
    out = re.sub(r",\s*\.", ".", out)
    out = re.sub(r",\s*,", ",", out)
    out = re.sub(r"\.\s*\.", ".", out)
    out = re.sub(r"([.!?]){2,}", r"\1", out)
    # Capitalise the start of a new sentence after a terminator
    out = re.sub(r"([.!?])\s+([a-z])", lambda m: f"{m.group(1)} {m.group(2).upper()}", out)
    out = re.sub(r"\(\s+", "(", out)
    out = re.sub(r"\s+\)", ")", out)
    # keep "). " as valid when sentence ends after paren — join logic avoids "). indicating"
    out = re.sub(r"\)\.", ").", out)
    return out.strip()


def _strip_trailing_punct(s: str) -> str:
    return re.sub(r"[.!,;:]+$", "", s.strip()).strip()


def _soften(phrase: str) -> str:
    return re.sub(r"\.$", "", phrase)


def _trim_period(s: str) -> str:
    return re.sub(r"\.$", "", s)


def _ensure_sentence(text: str) -> str:
    trimmed = text.strip()
    if not trimmed:
        return trimmed
    return trimmed if re.search(r"[.!?]$", trimmed) else f"{trimmed}."


def _capitalize(text: str) -> str:
    t = text.strip()
    if not t:
        return t
    return t[0].upper() + t[1:]


def _fmt(n: float) -> str:
    """Mirror JS `String(Math.round(n * 10) / 10)` — no trailing .0 for whole numbers."""
    value = round1(n)
    if value == int(value):
        return str(int(value))
    return str(value)

SHORT_WHY: dict[str, str] = {
    "overall_risk": "This indicates that a meaningful proportion of employees are at elevated overall health risk.",
    "overall_risk_healthy": "This reflects a healthy overall workforce risk profile.",

    "physical_activity": "This increases the risk of future metabolic and cardiovascular conditions.",
    "physical_activity_healthy": "This supports long-term metabolic and cardiovascular health.",

    "sleep": "This affects recovery, concentration, and metabolic health.",
    "sleep_healthy": "This supports healthy recovery and day-to-day wellbeing.",

    "nutrition": "This influences metabolic health, blood sugar regulation, and heart health.",
    "nutrition_healthy": "This helps maintain healthy metabolic and cardiovascular function.",

    "oxidative_stress": "This indicates increased cellular stress and slower recovery.",
    "oxidative_stress_healthy": "This reflects healthy recovery and good cellular resilience.",

    "type_2_diabetes": "This indicates an increased risk of developing Type 2 diabetes.",
    "hypertension": "This increases the risk of cardiovascular disease and stroke.",
    "obesity": "This contributes to several long-term metabolic and cardiovascular conditions.",
    "dyslipidemia": "This indicates an increased long-term risk of cardiovascular disease.",
    "cardiac_health": "This highlights increased cardiovascular risk across the workforce.",
    "nafld": "This suggests early metabolic changes that may affect liver health.",
    "thyroid_health": "This may contribute to changes in energy levels and metabolism.",
    "pcos_pcod": "This highlights the need for targeted women's metabolic and hormonal health support.",
    "metabolic_syndrome": "This indicates that multiple metabolic risk factors are occurring together.",

    "metabolic_age": "This suggests accelerated metabolic ageing in part of the workforce.",
    "bmi_waist": "This increases the risk of metabolic and cardiovascular disease.",

    "positive_wins": "This highlights areas where workforce health is performing well.",
    "participation": "This helps identify opportunities to improve programme participation.",
    "maintain": "This supports continued investment in preventive health initiatives.",

    "recovery_strain": "This suggests that employee recovery may be inadequate.",
    "cardio_nutrition": "This highlights the importance of heart-healthy nutrition.",
    "movement_priority": "This identifies physical activity as the most impactful lifestyle intervention.",
    "metabolic_cluster": "This indicates that multiple metabolic risk factors are occurring together.",
    "cardio_cluster": "This shows that cardiovascular risk factors are reinforcing each other.",

    "workforce_resilience": "This supports long-term workforce health and resilience.",
    "disease_generic": "This supports preventive care through targeted screening and lifestyle intervention.",
}

EFFECT_CLAUSES: dict[str, str] = {
    "recovery_strain": "Sleep quality and recovery require greater attention across the workforce.",
    "cardio_nutrition": "Nutrition should be a key focus for improving cardiovascular health.",
    "movement_priority": "Increasing daily physical activity should be a priority for the workforce.",
    "metabolic_cluster": "Several metabolic risk factors are occurring together, increasing the likelihood of future chronic disease.",
    "cardio_cluster": "Blood pressure and lipid abnormalities are occurring together, increasing overall cardiovascular risk.",
    "workforce_resilience": "Healthy sleep and regular physical activity are supporting long-term workforce wellbeing.",
}
SHORT_ACTIONS: dict[str, str] = {
    "metabolic_screening": "Include annual metabolic screening and lifestyle support for at-risk employees",
    "lipid_screening": "Include routine lipid screening with heart-healthy nutrition support",
    "bp_screening": "Promote regular blood pressure screening and stress management support",
    "thyroid_screening": "Include thyroid function screening in routine health checks",
    "liver_screening": "Include liver function screening alongside metabolic assessments",
    "cardiac_screening": "Prioritise cardiovascular screening for employees at elevated risk",
    "nutrition_refined_carb": "Promote nutrition programmes that reduce refined carbohydrate intake",
    "nutrition_heart_healthy": "Promote heart-healthy nutrition programmes across the workforce",
    "nutrition_sodium": "Promote nutrition programmes that encourage lower sodium intake",
    "nutrition_whole_food": "Promote balanced eating with whole foods and healthy portions",
    "movement_programme": "Promote daily movement programmes and active work breaks",
    "weight_management": "Support weight management programmes for at-risk employees",
    "sleep_health": "Promote healthy sleep habits and recovery-focused wellbeing initiatives",
    "stress_management": "Promote stress management and healthy workload practices",
    "recovery_programme": "Support recovery through healthy sleep, stress, and restorative habits",
    "womens_health": "Strengthen women's health awareness and confidential support services",
    "smoking_cessation": "Provide smoking cessation support for interested employees",
    "alcohol_moderation": "Promote responsible alcohol use through health education",
    "maintain_wellness": "Continue preventive wellness initiatives and routine health monitoring",
    "target_high_risk": "Focus coaching and clinical follow-up on employees at elevated risk",
    "scale_preventive_care": "Expand preventive care across screening, nutrition, and physical activity",
    "clinical_review": "Recommend clinical review for employees with elevated health risk",
}

# Company-level "single most appropriate action" phrasing (uses profile cluster).
ORG_PRIMARY_ACTIONS: dict[str, str] = {
    "metabolic_screening": "Make annual metabolic screening a key response to this {cluster} risk profile",
    "lipid_screening": "Make routine lipid screening with heart-healthy nutrition a core prevention programme",
    "bp_screening": "Make blood pressure screening and stress management a key prevention programme",
    "thyroid_screening": "Make thyroid screening a routine part of the organisation's preventive health programme",
    "liver_screening": "Make liver function and metabolic screening a key clinical response",
    "cardiac_screening": "Make cardiovascular screening a key response for this {cluster} risk profile",
    "nutrition_refined_carb": "Make reducing refined carbohydrates the organisation's primary nutrition initiative",
    "nutrition_heart_healthy": "Make heart-healthy nutrition the primary prevention programme for this {cluster} profile",
    "nutrition_sodium": "Make sodium reduction a key workplace nutrition initiative",
    "nutrition_whole_food": "Make whole-food, balanced eating the organisation's primary lifestyle programme",
    "movement_programme": "Make daily workplace movement the organisation's primary physical activity initiative",
    "weight_management": "Make weight management programmes the primary response for higher-risk employees",
    "sleep_health": "Make healthy sleep a key workplace recovery initiative",
    "stress_management": "Make stress management and healthy workload practices a core wellbeing programme",
    "recovery_programme": "Make sleep and stress recovery the organisation's primary recovery initiative",
    "target_high_risk": "Make targeted coaching for higher-risk employees the organisation's primary intervention",
    "scale_preventive_care": "Expand preventive care across screening, nutrition, and physical activity",
    "clinical_review": "Make clinical review for higher-risk employees the organisation's primary follow-up action",
}

# Section-specific supporting actions — avoid repeating the org-primary sentence.
SECTION_SUPPORT_ACTIONS: dict[tuple[str, str], str] = {
    ("movement_programme", "physical_activity"): "Reduce the activity gap through daily movement programmes and active work breaks",
    ("movement_programme", "oxidative_stress"): "Use daily movement programmes to support recovery and reduce oxidative stress",
    ("movement_programme", "bmi_waist"): "Combine body composition support with daily workplace movement programmes",

    ("sleep_health", "sleep"): "Promote healthy sleep habits through recovery-focused wellbeing initiatives",
    ("sleep_health", "oxidative_stress"): "Strengthen healthy sleep habits to reduce oxidative stress across the workforce",

    ("recovery_programme", "oxidative_stress"): "Support recovery through sleep, stress management, and restorative habits",
    ("recovery_programme", "sleep"): "Develop an integrated recovery programme covering sleep and stress",

    ("nutrition_heart_healthy", "nutrition"): "Promote heart-healthy nutrition programmes across the workforce",
    ("nutrition_whole_food", "nutrition"): "Promote balanced whole-food eating patterns across the organisation",

    ("weight_management", "bmi_waist"): "Support weight management programmes for employees with elevated BMI and waist circumference",

    ("target_high_risk", "overall_risk"): "Focus coaching and clinical follow-up on employees at elevated risk",
    ("scale_preventive_care", "overall_risk"): "Expand preventive care across screening, nutrition, and physical activity",

    ("lipid_screening", "disease_risks"): "Prioritise routine lipid screening for employees with the leading disease risk",
    ("bp_screening", "disease_risks"): "Prioritise blood pressure screening and stress management for the leading disease risk",
    ("metabolic_screening", "disease_risks"): "Prioritise metabolic screening for employees within the leading disease risk group",
}

MAINTAIN_ACTIONS: dict[str, str] = {
    "positive_highlights": "Continue the initiatives that are supporting these positive health outcomes",
    "participation": "Maintain strong participation while encouraging more engagement where needed",
    "overall_risk": "Continue preventive wellness initiatives and routine workforce health monitoring",
    "physical_activity": "Continue promoting healthy physical activity through existing movement programmes",
    "sleep": "Continue reinforcing healthy sleep habits through existing wellbeing initiatives",
    "oxidative_stress": "Continue promoting recovery habits that help maintain healthy oxidative stress levels",
    "disease_risks": "Continue preventive screening and lifestyle programmes where disease risk remains low",
    "disease_deep_dive": "Continue routine monitoring while reinforcing healthy behaviours in this disease area",
}

# ---------------------------------------------------------------------------
# compose/compose_insight.py
# ---------------------------------------------------------------------------


def compose_insight(
    plan: InsightPlan,
    *,
    used_phrases: frozenset[str] | None = None,
    used_why_stems: frozenset[str] | None = None,
) -> StructuredInsight:
    return _generate_structured_insight_from_plan(
        plan, used_phrases=used_phrases, used_why_stems=used_why_stems
    )


# ---------------------------------------------------------------------------
# insight/generate_leadership.py
# ---------------------------------------------------------------------------

BODY_MAX_WORDS = 30


def generate_leadership_cards(profile: CompanyHealthProfile) -> list[LeadershipTakeawayCard]:
    return [
        _workforce_status_card(profile),
        _lifestyle_priority_card(profile),
        _disease_focus_card(profile),
        _strategic_next_step_card(profile),
    ]


def _workforce_status_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    overall_risk_finding = profile.findings.get("overall_risk")
    elevated = (
        overall_risk_finding.elevated_share if overall_risk_finding else None
    )
    if elevated is None:
        elevated = profile.overall_burden
    healthy_share = (
        overall_risk_finding.healthy_share
        if overall_risk_finding and overall_risk_finding.healthy_share is not None
        else max(0.0, 100 - elevated)
    )
    healthy = profile.overall_severity in ("very_low", "low")

    if healthy:
        headline = "Healthy Workforce"
        body = (
            f"{_fmt(healthy_share)}% of employees sit in healthy risk bands. Maintain annual "
            "assessments and reinforce preventive habits."
        )
    elif profile.overall_severity == "moderate":
        headline = "Emerging Health Risks"
        body = (
            f"{_fmt(elevated)}% of employees sit in elevated risk bands. Early lifestyle "
            "programmes can reverse this trajectory."
        )
    elif profile.overall_severity == "high":
        headline = "Growing Disease Burden"
        body = (
            f"{_fmt(elevated)}% of employees are at elevated risk. Organisation-wide prevention "
            "should become a leadership priority."
        )
    else:
        headline = "Immediate Attention Needed"
        body = (
            f"{_fmt(elevated)}% of employees are at elevated risk. A coordinated preventive "
            "strategy is required now."
        )

    return _card("workforce-health", "Workforce Health Status", headline, body, profile, healthy)


def _lifestyle_priority_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    priority = profile.lifestyle_priority
    activity_finding = profile.findings.get("physical_activity")
    sleep_finding = profile.findings.get("sleep")
    nutrition_finding = profile.findings.get("nutrition")
    activity = activity_finding.elevated_share if activity_finding else 0
    sleep = sleep_finding.elevated_share if sleep_finding else 0
    nutrition = nutrition_finding.elevated_share if nutrition_finding else 0

    if priority == "recovery":
        headline = "Recovery Opportunity"
        body = (
            f"Poor sleep ({_fmt(sleep)}%) and elevated oxidative stress point to a recovery gap. "
            "Improve sleep hygiene before adding new wellness programmes."
        )
    elif priority == "sleep":
        headline = "Sleep Opportunity"
        body = (
            f"{_fmt(sleep)}% of employees fall outside healthy sleep ranges. Recovery-focused "
            "initiatives will lift energy and metabolic health."
        )
    elif priority == "nutrition":
        headline = "Nutrition Opportunity"
        body = (
            f"Nutrition scores leave {_fmt(nutrition)}% below a healthy range. Heart-healthy "
            "eating is the clearest lifestyle lever."
        )
    elif priority == "strong":
        headline = "Healthy Lifestyle Foundation"
        body = (
            "Movement and recovery habits are strong. Keep reinforcing them while supporting "
            "higher-risk individuals."
        )
    else:
        headline = "Movement Opportunity"
        body = (
            f"{_fmt(activity)}% of employees report low activity. Daily movement programmes "
            "offer the largest lifestyle gain."
        )

    return _card(
        "lifestyle-priority",
        "Lifestyle Priority",
        headline,
        body,
        profile,
        priority == "strong",
    )


def _disease_focus_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    # Same ranking the Top Disease Risk chart displays
    lead = profile.chart_top_diseases[0] if profile.chart_top_diseases else None
    top_disease_id: MetricId | None = None
    if lead is not None:
        top_disease_id = lead.metric_id
    else:
        top_disease_id = next(
            (r.metric_id for r in profile.top_risks if is_disease_metric(r.metric_id)),
            profile.top_risks[0].metric_id if profile.top_risks else None,
        )

    if lead is not None:
        elevated = lead.high_risk_percent
    elif top_disease_id:
        finding = profile.findings.get(top_disease_id)
        elevated = finding.elevated_share if finding else 0
    else:
        elevated = 0

    if lead is not None:
        disease_name = lead.name
    elif top_disease_id:
        disease_name = human_metric_name(top_disease_id)
    else:
        disease_name = None

    cluster = profile.dominant_cluster

    if cluster == "healthy" or not disease_name or elevated < 20:
        headline = "Preventive Healthcare Focus"
        body = (
            "No single disease dominates. Broad screening and prevention will deliver the best "
            "organisational return."
        )
    elif cluster == "cardiovascular":
        headline = "Heart Health Priority"
        body = (
            f"{disease_name} leads at {_fmt(elevated)}% elevated risk. Focus screening on blood "
            "pressure, lipids and cardiac markers."
        )
    elif cluster == "hormonal":
        headline = "Hormonal Health Opportunity"
        body = (
            f"{disease_name} affects {_fmt(elevated)}% in elevated bands. Offer targeted "
            "screening and specialist support pathways."
        )
    else:
        headline = "Metabolic Health Priority"
        body = (
            f"{disease_name} leads at {_fmt(elevated)}% elevated risk. Prioritise metabolic "
            "screening and lifestyle support for this group."
        )

    # Prefer a curated leadership takeaway for the identified disease when it fits
    # the body budget. The disease, headline, number and card identity are all
    # unchanged — only the supporting sentence gains medically-authored phrasing.
    if disease_name and cluster != "healthy" and elevated >= 20 and top_disease_id:
        takeaways = disease_leadership_takeaways(top_disease_id)
        if takeaways:
            pick = select_variant(
                takeaways, top_disease_id, cluster, profile.overall_severity
            )
            candidate = f"{disease_name} leads at {_fmt(elevated)}% elevated risk. {pick}"
            if pick and count_words(candidate) <= BODY_MAX_WORDS:
                body = candidate

    return _card(
        "disease-focus",
        "Primary Disease Focus",
        headline,
        body,
        profile,
        cluster == "healthy",
    )


def _strategic_next_step_card(profile: CompanyHealthProfile) -> LeadershipTakeawayCard:
    severity = profile.overall_severity
    one_thing = profile.one_thing
    lever_label = _lever_to_plain_action(one_thing.lever) if one_thing else None

    if severity in ("very_low", "low"):
        headline = "Maintain Momentum"
        body = "Keep annual health assessments and recognise healthy behaviours to protect current gains."
    elif severity == "moderate":
        headline = "Target Elevated-Risk Groups"
        body = (
            f"Concentrate resources on elevated-risk employees. Start with {lever_label}."
            if lever_label
            else "Concentrate coaching and clinical follow-up on employees in elevated risk bands."
        )
    elif severity == "high":
        headline = "Scale Preventive Care"
        body = (
            f"Expand screening and lifestyle programmes company-wide, led by {lever_label}."
            if lever_label
            else "Expand screening, nutrition and fitness programmes across the organisation."
        )
    else:
        headline = "Build a Long-Term Health Strategy"
        body = (
            "Integrate regular screenings, leadership accountability and outcome tracking into "
            "organisational health planning."
        )

    return _card("strategic-next-step", "Strategic Next Step", headline, body, profile, False)


def _card(
    id_: str,
    title: str,
    headline: str,
    body: str,
    profile: CompanyHealthProfile,
    positive: bool,
) -> LeadershipTakeawayCard:
    confidence = _profile_confidence_as_insight(profile)
    clean_body = sanitize_insight_text(body)
    if confidence.band == "low":
        clean_body = _soften_leadership_body(clean_body)
    clean_body = limit_words(clean_body, BODY_MAX_WORDS)

    observation, explanation, recommendation = _leadership_structured_fields(
        headline, clean_body
    )

    structured = StructuredInsight(
        section_id="leadership",
        tone="positive" if positive else "concern",
        severity_band=profile.overall_severity,
        confidence=confidence,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        headline=headline,
        effect_ids=[],
        lever_ids=[],
    )

    return LeadershipTakeawayCard(
        id=id_,
        title=title,
        headline=headline,
        body=clean_body,  # UI unchanged — not cloned from structured fields
        confidence=confidence,
        structured=structured,
    )


def _leadership_structured_fields(headline: str, body: str) -> tuple[str, str, str]:
    """Split leadership body into distinct Observation / Explanation / Recommendation.

    Headline and body for the card UI are unchanged; only structured.* differs.
    """
    sentences = [
        s.strip()
        for s in re.split(r"(?<=[.!?])\s+", (body or "").strip())
        if s and s.strip()
    ]
    if not sentences:
        return ensure_structured_field_punctuation(
            f"{headline} requires structured leadership attention",
            "Workforce health patterns in this area carry organisational implications.",
            "Align leadership follow-up with the organisation's preventive health priorities.",
        )

    observation = sentences[0]
    if len(sentences) == 1:
        explanation = (
            f"{headline} reflects a workforce pattern with material implications "
            "for preventive planning."
        )
        recommendation = (
            "Translate this priority into a clear ownership plan with measurable follow-up."
        )
    else:
        recommendation = sentences[-1]
        mid = sentences[1:-1]
        if mid:
            explanation = " ".join(mid)
        else:
            explanation = (
                "Early, coordinated preventive action reduces long-term health "
                "and productivity risk for the workforce."
            )
        if _normalize_compare(explanation) == _normalize_compare(recommendation):
            explanation = (
                "This pattern benefits from structured preventive investment "
                "and clear leadership ownership."
            )
        if _normalize_compare(observation) == _normalize_compare(recommendation):
            recommendation = (
                "Convert this finding into a time-bound preventive action owned by HR and leadership."
            )

    return ensure_structured_field_punctuation(observation, explanation, recommendation)


def _soften_leadership_body(text: str) -> str:
    out = text
    out = re.sub(r"\bshould become\b", "may need to become", out, flags=re.IGNORECASE)
    out = re.sub(r"\bis required\b", "may be needed", out, flags=re.IGNORECASE)
    out = re.sub(r"\bwill deliver\b", "can help deliver", out, flags=re.IGNORECASE)
    out = re.sub(r"\bwill lift\b", "may lift", out, flags=re.IGNORECASE)
    out = re.sub(r"\boffer the largest\b", "may offer a strong", out, flags=re.IGNORECASE)
    out = re.sub(r"\bPrioritise\b", "Consider prioritising", out)
    out = re.sub(r"\bFocus screening\b", "Consider focusing screening", out)
    out = re.sub(r"\bExpand screening\b", "Consider expanding screening", out)
    out = re.sub(r"\bConcentrate resources\b", "Consider concentrating resources", out)
    return out


def _profile_confidence_as_insight(profile: CompanyHealthProfile) -> InsightConfidence:
    score = profile.profile_confidence
    band = "high" if score >= 0.75 else "moderate" if score >= 0.5 else "low"
    return InsightConfidence(
        score=score,
        band=band,
        factors=ConfidenceFactors(
            data_quality=score,
            signal_strength=0.7,
            graph_support=0.8 if len(profile.emergent_priorities) > 0 else 0.45,
            knowledge_coverage=1,
        ),
        notes=["leadership"],
    )


_LEVER_TO_PLAIN_ACTION: dict[str, str] = {
    "movement_programme": "daily movement programmes",
    "sleep_health": "sleep-health initiatives",
    "recovery_programme": "recovery programmes",
    "nutrition_heart_healthy": "heart-healthy nutrition",
    "nutrition_refined_carb": "nutrition counselling",
    "weight_management": "weight-management support",
    "metabolic_screening": "metabolic screening",
    "lipid_screening": "lipid screening",
    "bp_screening": "blood-pressure screening",
    "cardiac_screening": "cardiovascular screening",
    "target_high_risk": "targeted high-risk support",
    "scale_preventive_care": "scaled preventive care",
}


def _lever_to_plain_action(lever: str) -> str | None:
    return _LEVER_TO_PLAIN_ACTION.get(lever)


def top_disease_metric(profile: CompanyHealthProfile) -> MetricId | None:
    """Exported for tests — ensure disease metric lookup stays typed."""
    return next(
        (r.metric_id for r in profile.top_risks if is_disease_metric(r.metric_id)),
        None,
    )


# ---------------------------------------------------------------------------
# generate.py — public API
# ---------------------------------------------------------------------------

# @deprecated Prefer compose_company_profile
build_profile_from_slices = compose_company_profile

_EMPTY_CONFIDENCE = InsightConfidence(
    score=0,
    band="low",
    factors=ConfidenceFactors(
        data_quality=0,
        signal_strength=0,
        graph_support=0,
        knowledge_coverage=0,
    ),
    notes=["insufficient_data"],
)


def _insufficient_narrative(section_id: SectionId) -> ChartNarrative:
    observation, explanation, recommendation = ensure_structured_field_punctuation(
        INSUFFICIENT_DATA_MESSAGE, "", ""
    )
    structured = StructuredInsight(
        section_id=section_id,
        tone="neutral",
        severity_band="low",
        confidence=_EMPTY_CONFIDENCE,
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
    )
    return ChartNarrative(
        tone="neutral",
        text=INSUFFICIENT_DATA_MESSAGE,
        structured=structured,
        confidence=_EMPTY_CONFIDENCE,
    )


ChartDataInput = MetricFinding | None
GenerateInsightOptions = dict[str, Any]


@dataclass
class RecommendationLedger:
    """Tracks levers/phrases already used on a report so sections vary deterministically."""

    used_levers: list[str] = field(default_factory=list)
    used_phrases: list[str] = field(default_factory=list)
    used_why_stems: list[str] = field(default_factory=list)

    def note(
        self,
        lever: str | None,
        phrase: str | None,
        *,
        explanation: str | None = None,
    ) -> None:
        if lever:
            self.used_levers.append(lever)
        if phrase:
            # Compare bare text so trailing punctuation does not defeat dedupe.
            cleaned = re.sub(r"[.!,;:]+$", "", phrase.strip()).strip()
            if cleaned:
                self.used_phrases.append(cleaned)
        if explanation:
            stem = " ".join(_strip_trailing_punct(explanation).split()[:2]).lower()
            if stem:
                self.used_why_stems.append(stem)


def generate_insight(
    section_id: SectionId,
    chart_data: ChartDataInput,
    profile: CompanyHealthProfile,
    options: GenerateInsightOptions | None = None,
    *,
    ledger: RecommendationLedger | None = None,
) -> ChartNarrative:
    """Generate a chart/report narrative for one section.

    Args:
        section_id: Section key (e.g. 'overall_risk', 'disease_risks').
        chart_data: Finding for the chart currently displayed — single source
            of truth for facts. Pass ``None`` to reason from the company
            profile alone.
        profile: Company health profile from ``compose_company_profile(data)``.
        options: Optional dict with an ``active_metric_id`` override.
        ledger: Optional cross-section ledger to avoid repeating actions.
    """

    options = options or {}

    if len(profile.coverage) == 0 and not chart_data:
        return _insufficient_narrative(section_id)

    working_profile = (
        rebuild_profile_with_finding(profile, chart_data) if chart_data else profile
    )

    active_metric_id = options.get("active_metric_id") or (
        chart_data.metric_id if chart_data else None
    )
    plan = reason_about_section(
        section_id,
        working_profile,
        ReasonOptions(
            active_metric_id=active_metric_id,
            chart_finding=chart_data or None,
            used_levers=list(ledger.used_levers) if ledger else None,
        ),
    )

    used_phrases = frozenset(ledger.used_phrases) if ledger else None
    used_why = frozenset(ledger.used_why_stems) if ledger else None
    if isinstance(plan, list):
        structured = compose_insight(
            plan[0], used_phrases=used_phrases, used_why_stems=used_why
        )
    else:
        structured = compose_insight(
            plan, used_phrases=used_phrases, used_why_stems=used_why
        )

    narrative = format_chart_footer(structured)
    if ledger is not None:
        primary = (structured.lever_ids or [None])[0]
        ledger.note(
            primary,
            structured.recommendation,
            explanation=structured.explanation,
        )
    return narrative


def generate_structured_insight(
    section_id: SectionId,
    chart_data: ChartDataInput,
    profile: CompanyHealthProfile,
    options: GenerateInsightOptions | None = None,
) -> StructuredInsight:
    """Structured Observation -> Why -> Action (no footer join)."""
    return generate_insight(section_id, chart_data, profile, options).structured


def generate_leadership_takeaways(profile: CompanyHealthProfile) -> list[LeadershipTakeawayCard]:

    if len(profile.coverage) == 0:
        return []
    return format_leadership_cards(profile)


def generate_positive_insights(profile: CompanyHealthProfile) -> ChartNarrative:
    """Positive organisational strengths narrative.

    Reusable by dashboard Positive Wins panel, PDF reports, and APIs.
    """
    chart_data = profile.findings.get("positive_wins")
    if not chart_data and len(profile.coverage) == 0:
        return _insufficient_narrative("positive_highlights")
    return generate_insight("positive_highlights", chart_data, profile)


def generate_insight_from_data(
    section_id: SectionId,
    data: DashboardInput,
    options: GenerateInsightOptions | None = None,
) -> ChartNarrative:
    """Convenience: profile from data then insight (no local chart override)."""
    return generate_insight(section_id, None, compose_company_profile(data), options)


def generate_insight_from_slices(
    section_id: SectionId,
    slices: DashboardInput,
    options: GenerateInsightOptions | None = None,
) -> ChartNarrative:
    """@deprecated Prefer generate_insight_from_data."""
    return generate_insight_from_data(section_id, slices, options)


def generate_insight_with_local_finding(
    section_id: SectionId,
    profile: CompanyHealthProfile,
    local_finding: MetricFinding,
    options: GenerateInsightOptions | None = None,
) -> ChartNarrative:
    """@deprecated Prefer generate_insight(section_id, chart_data, profile)."""
    return generate_insight(section_id, local_finding, profile, options)


def plan_insight(
    section_id: SectionId,
    profile: CompanyHealthProfile,
    options: ReasonOptions | None = None,
) -> InsightPlan | list[InsightPlan]:
    return reason_about_section(section_id, profile, options)


def validate_insight_structured(structured: StructuredInsight) -> list[str]:
    """Return a list of quality violations for one structured insight (empty = OK)."""
    errors: list[str] = []
    obs = (structured.observation or "").strip()
    why = (structured.explanation or "").strip()
    rec = (structured.recommendation or "").strip()

    if structured.section_id == "leadership":
        fields = [("observation", obs), ("explanation", why), ("recommendation", rec)]
    else:
        # Chart insights must have all three when data was sufficient.
        if obs == INSUFFICIENT_DATA_MESSAGE or not obs:
            return errors
        fields = [("observation", obs), ("explanation", why), ("recommendation", rec)]

    for name, text in fields:
        if not text:
            errors.append(f"{name} empty")
            continue
        if not (text[0].isupper() or text[0].isdigit()):
            errors.append(f"{name} must start with a capital letter")
        if text.endswith(","):
            errors.append(f"{name} must not end with a comma")
        if not re.search(r"[.!?]$", text):
            errors.append(f"{name} must end with sentence punctuation")
        if _is_clause(text):
            errors.append(f"{name} is a sentence fragment")

    if why and rec and _normalize_compare(why) == _normalize_compare(rec):
        errors.append("explanation equals recommendation")
    if obs and why and _normalize_compare(obs) == _normalize_compare(why):
        errors.append("observation equals explanation")
    if obs and rec and _normalize_compare(obs) == _normalize_compare(rec):
        errors.append("observation equals recommendation")

    if structured.effect_ids is not None:
        if len(structured.effect_ids) != len(set(structured.effect_ids)):
            errors.append("duplicate effect_ids")
    if structured.lever_ids is not None:
        if len(structured.lever_ids) != len(set(structured.lever_ids)):
            errors.append("duplicate lever_ids")

    if (
        structured.section_id in ("disease_risks", "disease_deep_dive")
        and structured.tone == "concern"
        and rec
    ):
        # Disease-first: should not be pure company-cluster replacement copy.
        if re.match(r"^Focus the \w+ prevention response\b", rec, re.I):
            errors.append("recommendation replaced by company-wide cluster template")

    return errors


def validate_report_insights(payload: dict) -> list[str]:
    """Validate a full generate_report_insights payload; return violation messages."""
    errors: list[str] = []
    concerns = payload.get("concerns") or {}

    def check_narrative(label: str, narrative: dict | None) -> None:
        if not narrative or not isinstance(narrative, dict):
            return
        structured = narrative.get("structured") or {}
        if not isinstance(structured, dict):
            return
        # Build a lightweight duck object
        class _S:
            pass

        s = _S()
        s.section_id = structured.get("section_id") or label
        s.observation = structured.get("observation") or ""
        s.explanation = structured.get("explanation") or ""
        s.recommendation = structured.get("recommendation") or ""
        s.tone = narrative.get("tone") or structured.get("tone")
        s.effect_ids = structured.get("effect_ids")
        s.lever_ids = structured.get("lever_ids")
        for err in validate_insight_structured(s):  # type: ignore[arg-type]
            errors.append(f"{label}: {err}")

    for key, value in concerns.items():
        if key in ("physical_activity", "sleep") and isinstance(value, dict) and "both" in value:
            for view, narr in value.items():
                check_narrative(f"{key}.{view}", narr)
        elif key == "disease_deep_dive" and isinstance(value, dict):
            for disease, narr in value.items():
                check_narrative(f"disease_deep_dive.{disease}", narr)
        elif isinstance(value, dict) and "text" in value:
            check_narrative(key, value)

    check_narrative("positives", payload.get("positives"))

    for i, card in enumerate(payload.get("leadership_cards") or []):
        structured = (card or {}).get("structured") or {}
        if not structured:
            continue

        class _S:
            pass

        s = _S()
        s.section_id = "leadership"
        s.observation = structured.get("observation") or ""
        s.explanation = structured.get("explanation") or ""
        s.recommendation = structured.get("recommendation") or ""
        s.tone = structured.get("tone")
        s.effect_ids = structured.get("effect_ids") or []
        s.lever_ids = structured.get("lever_ids") or []
        for err in validate_insight_structured(s):  # type: ignore[arg-type]
            errors.append(f"leadership[{i}]: {err}")
        if s.explanation and s.recommendation and _normalize_compare(s.explanation) == _normalize_compare(s.recommendation):
            errors.append(f"leadership[{i}]: structured explanation equals recommendation")

    return errors


__all__ = [
    "compose_company_profile",
    "rebuild_profile_with_finding",
    "build_profile_from_slices",
    "generate_insight",
    "generate_structured_insight",
    "generate_leadership_takeaways",
    "generate_positive_insights",
    "generate_insight_from_data",
    "generate_insight_from_slices",
    "generate_insight_with_local_finding",
    "plan_insight",
    "compose_insight",
    "reason_about_section",
    "ChartDataInput",
    "GenerateInsightOptions",
    "human_metric_name",
    "human_cluster_name",
    "sanitize_insight_text",
    "compose_footer_text",
    "generate_leadership_cards",
    "validate_insight_structured",
    "validate_report_insights",
    "INSUFFICIENT_DATA_MESSAGE",
]

# ============================================================
# FORMATTING
# ============================================================

"""Format / serialize layer for chart footers, leadership cards, and JSON output.

Flat merge (no business-logic changes) of:
  - intelligence/format/format_chart_footer.py
  - intelligence/serialize.py

Imports presentation helpers from ``.generator`` (sanitize / compose_footer /
generate_leadership_cards). Generator lazy-imports this module to avoid cycles.
"""


from dataclasses import asdict, is_dataclass
from typing import Any


def format_chart_footer(insight: StructuredInsight) -> ChartNarrative:
    """Format structured insight as a single dashboard footer paragraph."""
    text = sanitize_insight_text(compose_footer_text(insight))
    return ChartNarrative(
        tone=insight.tone,
        text=text,
        structured=insight,
        confidence=insight.confidence,
    )


def format_leadership_cards(profile: CompanyHealthProfile) -> list[LeadershipTakeawayCard]:
    """Leadership cards — dedicated generator, not footer reuse."""
    return generate_leadership_cards(profile)


class ReportBlock(dict):
    """Future report block — exposes all three parts separately.

    Behaves like a plain mapping with attribute-style access so callers can use
    either ``block["observation"]`` or ``block.observation``.
    """

    def __getattr__(self, item: str):
        try:
            return self[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc


def format_report_block(insight: StructuredInsight) -> ReportBlock:

    observation, explanation, recommendation = ensure_structured_field_punctuation(
        sanitize_insight_text(insight.observation),
        sanitize_insight_text(insight.explanation),
        sanitize_insight_text(insight.recommendation),
    )
    return ReportBlock(
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        confidence=insight.confidence,
        tone=insight.tone,
    )


# ---------------------------------------------------------------------------
# serialize.py
# ---------------------------------------------------------------------------


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value))
    if hasattr(value, "__dict__"):
        return to_jsonable(vars(value))
    return str(value)


def narrative_to_dict(narrative: Any) -> dict:
    return to_jsonable(narrative)


def leadership_to_dict(cards: list) -> list:
    return [to_jsonable(card) for card in cards]

# ============================================================
# ENGINE
# ============================================================

"""Public Health Intelligence Engine entrypoint.

``generate_report_insights(report_json)`` builds the raw intelligence payload
(profile / concerns / leadership_cards). For camp-report–shaped responses, use
``enrich_camp_report_with_intelligence`` in ``assembly.py`` (or the package
export). No FastAPI, SQLAlchemy, or auth dependencies.
"""


from typing import Any, Dict, List, Optional


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

# ============================================================
# CAMP REPORT ENRICHMENT
# ============================================================

"""Section-aware assembly: enrich Camp Report JSON with intelligence in place.

Keeps ``generate_report_insights`` generation logic unchanged. Only maps its
output onto existing camp-report section objects under an ``intelligence`` key.
"""


import copy
from typing import Any, Dict, Mapping, MutableMapping, Optional


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


# ============================================================
# PUBLIC API
# ============================================================

__all__ = [
    "generate_report_insights",
    "enrich_camp_report_with_intelligence",
    "INTELLIGENCE_CAMP_SECTIONS",
    "compose_company_profile",
    "generate_insight",
    "generate_structured_insight",
    "generate_leadership_takeaways",
    "generate_positive_insights",
    "reason_about_section",
]
