"""Graph-derived extras for MetricFinding.

Helpers only. Does not decide tone, mode, or observation wording.
Analyzer shares (elevated/healthy/dominant/opportunity) remain the
intelligence inputs; extras are supporting facts from plotted data.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence

from .models import (
    CategoryShare,
    DistributionSlice,
    MetricFinding,
    ParticipationByAge,
    round1,
)

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


