"""
Internal numeric helpers shared across the engine.

Python's built-in ``round()`` uses banker's rounding (round-half-to-even),
while JavaScript's ``Math.round`` always rounds halves toward +Infinity.
To keep scoring bit-identical to the TypeScript source, every rounding call
in this package must go through ``js_round``/``round1``/``round3`` below
instead of the Python builtin.
"""

from __future__ import annotations

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
