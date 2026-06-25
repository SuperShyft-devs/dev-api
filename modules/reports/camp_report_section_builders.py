"""Builders for camp report section payloads."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

AGE_GROUPS: tuple[str, ...] = ("18–25", "26–35", "36–45", "46–55", "55+")
METABOLIC_SCORE_BANDS: tuple[str, ...] = ("optimal", "low_risk", "increased_risk", "high_risk")
PHYSICAL_ACTIVITY_BUCKETS: tuple[str, ...] = (
    "less_than_30mins",
    "30_60_mins",
    "more_than_60_mins",
    "rarely_or_never",
)
_OPTION_VALUE_TO_PHYSICAL_ACTIVITY_BUCKET: dict[str, str] = {
    "1": "less_than_30mins",
    "2": "30_60_mins",
    "3": "more_than_60_mins",
    "5": "rarely_or_never",
}
SLEEPING_HOURS_BUCKETS: tuple[str, ...] = (
    "less_than_5hrs",
    "between_5_7_hrs",
    "between_7_9_hrs",
    "more_than_9hrs",
)
_OPTION_VALUE_TO_SLEEPING_HOURS_BUCKET: dict[str, str] = {
    "0": "less_than_5hrs",
    "1": "between_5_7_hrs",
    "2": "between_7_9_hrs",
    "3": "more_than_9hrs",
}


def resolve_user_age(
    *,
    date_of_birth: date | None,
    stored_age: int,
    reference_date: date,
) -> int:
    """Derive age from DOB at reference_date, else use stored_age."""
    if date_of_birth is not None:
        years = reference_date.year - date_of_birth.year
        had_birthday = (reference_date.month, reference_date.day) >= (
            date_of_birth.month,
            date_of_birth.day,
        )
        return years if had_birthday else years - 1
    return stored_age


def age_to_bucket(age: int) -> str:
    if age <= 25:
        return "18–25"
    if age <= 35:
        return "26–35"
    if age <= 45:
        return "36–45"
    if age <= 55:
        return "46–55"
    return "55+"


def _percent(part: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((part / total) * 1000) / 10


def extract_metabolic_age(reports: dict) -> float | None:
    """Read metabolic_age from Metsights report JSON (top-level or nested data)."""
    ma = reports.get("metabolic_age")
    if isinstance(ma, (int, float)):
        return float(ma)
    data = reports.get("data")
    if isinstance(data, dict):
        nested = data.get("metabolic_age")
        if isinstance(nested, (int, float)):
            return float(nested)
    return None


def extract_metabolic_score(reports: dict) -> float | None:
    """Read metabolic_score from Metsights report JSON (top-level or nested data)."""
    ms = reports.get("metabolic_score")
    if isinstance(ms, (int, float)):
        return float(ms)
    data = reports.get("data")
    if isinstance(data, dict):
        nested = data.get("metabolic_score")
        if isinstance(nested, (int, float)):
            return float(nested)
    return None


def metabolic_score_to_band(score: float) -> str:
    if score <= 25:
        return "optimal"
    if score <= 42:
        return "low_risk"
    if score <= 58:
        return "increased_risk"
    return "high_risk"


def is_high_metabolic_risk(*, metabolic_age: float | None, chronological_age: int) -> bool:
    """True when metabolic age gap is at least 3 years."""
    effective_metabolic = metabolic_age if metabolic_age is not None else float(chronological_age)
    gap_years = effective_metabolic - chronological_age
    return gap_years >= 3


def build_kpis(metrics: dict) -> dict:
    """Build kpis section payload from aggregated metrics."""
    enrolled = int(metrics["employees_enrolled"])
    blood = int(metrics["total_blood_test"])
    return {
        "data": {
            "employees_enrolled": enrolled,
            "male_enrolled": int(metrics["male_enrolled"]),
            "female_enrolled": int(metrics["female_enrolled"]),
            "total_blood_test": blood,
            "blood_test_percent": round(blood / enrolled * 100) if enrolled else 0,
            "doctor_consultation": int(metrics["doctor_consultation"]),
            "high_risk_group": int(metrics["high_risk_group"]),
        },
    }


def build_participation_by_age(
    users: list[tuple[int, date | None, int]],
    *,
    reference_date: date,
) -> dict:
    """Build participation_by_age section payload from distinct enrolled users."""
    total = len(users)
    counts = {group: 0 for group in AGE_GROUPS}
    for _user_id, dob, stored_age in users:
        age = resolve_user_age(date_of_birth=dob, stored_age=stored_age, reference_date=reference_date)
        bucket = age_to_bucket(age)
        counts[bucket] += 1

    enrolled = [counts[group] for group in AGE_GROUPS]
    percent = [_percent(count, total) for count in enrolled]

    return {
        "data": {
            "age_group": list(AGE_GROUPS),
            "enrolled": enrolled,
            "percent": percent,
            "total_enrolled": total,
        },
    }


def normalize_camp_gender(value: object | None) -> str | None:
    """Map user gender to male/female using the same values as camp KPI aggregation."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"m", "male", "1"}:
        return "male"
    if normalized in {"f", "female", "2"}:
        return "female"
    return None


def physical_activity_answer_to_bucket(answer: object | None) -> str | None:
    """Map questionnaire option_value to a physical activity bucket key."""
    if answer is None:
        return None
    return _OPTION_VALUE_TO_PHYSICAL_ACTIVITY_BUCKET.get(str(answer).strip())


def _build_gender_distribution(counts: dict[str, int], buckets: tuple[str, ...]) -> dict:
    total = sum(counts[bucket] for bucket in buckets)
    count = [counts[bucket] for bucket in buckets]
    percent = [_percent(c, total) for c in count]
    return {
        "group": list(buckets),
        "count": count,
        "percent": percent,
    }


def build_distribution_by_physical_activity_frequency(
    rows: list[tuple[str | None, object | None]],
) -> dict:
    """Build distribution_by_physical_activity_frequency from (gender, answer) rows."""
    male_counts = {bucket: 0 for bucket in PHYSICAL_ACTIVITY_BUCKETS}
    female_counts = {bucket: 0 for bucket in PHYSICAL_ACTIVITY_BUCKETS}

    for gender_raw, answer in rows:
        gender = normalize_camp_gender(gender_raw)
        bucket = physical_activity_answer_to_bucket(answer)
        if gender is None or bucket is None:
            continue
        if gender == "male":
            male_counts[bucket] += 1
        else:
            female_counts[bucket] += 1

    return {
        "data": {
            "male": _build_gender_distribution(male_counts, PHYSICAL_ACTIVITY_BUCKETS),
            "female": _build_gender_distribution(female_counts, PHYSICAL_ACTIVITY_BUCKETS),
        },
    }


def sleeping_hours_answer_to_bucket(answer: object | None) -> str | None:
    """Map questionnaire option_value to a sleeping hours bucket key."""
    if answer is None:
        return None
    return _OPTION_VALUE_TO_SLEEPING_HOURS_BUCKET.get(str(answer).strip())


def build_distribution_by_sleeping_hours(
    rows: list[tuple[str | None, object | None]],
) -> dict:
    """Build distribution_by_sleeping_hours from (gender, answer) rows."""
    male_counts = {bucket: 0 for bucket in SLEEPING_HOURS_BUCKETS}
    female_counts = {bucket: 0 for bucket in SLEEPING_HOURS_BUCKETS}

    for gender_raw, answer in rows:
        gender = normalize_camp_gender(gender_raw)
        bucket = sleeping_hours_answer_to_bucket(answer)
        if gender is None or bucket is None:
            continue
        if gender == "male":
            male_counts[bucket] += 1
        else:
            female_counts[bucket] += 1

    return {
        "data": {
            "male": _build_gender_distribution(male_counts, SLEEPING_HOURS_BUCKETS),
            "female": _build_gender_distribution(female_counts, SLEEPING_HOURS_BUCKETS),
        },
    }


def build_overall_risk_score(scores: list[float]) -> dict:
    """Build overall_risk_score section payload from metabolic scores."""
    counts = {band: 0 for band in METABOLIC_SCORE_BANDS}
    for score in scores:
        counts[metabolic_score_to_band(score)] += 1

    total = len(scores)
    count = [counts[band] for band in METABOLIC_SCORE_BANDS]
    percent = [_percent(c, total) for c in count]
    avg = round(sum(scores) / total, 1) if total else 0.0

    return {
        "data": {
            "group": list(METABOLIC_SCORE_BANDS),
            "count": count,
            "percent": percent,
            "total_employees": total,
            "avg_metabolic_score": avg,
        },
    }


SECTION_BUILDERS: dict[str, Callable[..., dict]] = {
    "participation_by_age": build_participation_by_age,
    "kpis": build_kpis,
    "overall_risk_score": build_overall_risk_score,
    "distribution_by_physical_activity_frequency": build_distribution_by_physical_activity_frequency,
    "distribution_by_sleeping_hours": build_distribution_by_sleeping_hours,
}
