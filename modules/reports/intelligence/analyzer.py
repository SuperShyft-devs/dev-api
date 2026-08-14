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

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from .graph_facts import (
    join_names,
    lifestyle_graph_extras,
    merge_extras,
    overall_risk_graph_extras,
    oxidative_graph_extras,
    participation_graph_extras,
)
from .knowledge import disease_metric_from_name, get_disease_knowledge, get_lifestyle_knowledge
from .models import (
    BmiWaistSummary,
    CategoryPolarity,
    CategoryShare,
    ChartTopDisease,
    CompanyAverageScores,
    CompanyHealthProfile,
    DashboardInput,
    DiseaseDefinition,
    DiseaseRiskData,
    DistributionSlice,
    GenderDistributionPair,
    GenderGap,
    GenderWeights,
    LifestyleGenderView,
    MetabolicAgeSummary,
    MetricFinding,
    MetricId,
    NutritionSummary,
    OverallRiskScoreBucket,
    OxidativeStressByDept,
    ParticipationByAge,
    PositiveWins,
    RiskDistributionBucket,
    RiskLevel,
    TopHighRiskDisease,
    round1,
)

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
    from .profile import ComposeProfileOptions, compose_profile_from_findings

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
    from .profile import ComposeProfileOptions, compose_profile_from_findings

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
