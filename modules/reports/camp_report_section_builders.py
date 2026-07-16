"""Builders for camp report section payloads."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

AGE_GROUPS: tuple[str, ...] = ("18–25", "26–35", "36–45", "46–55", "55+")
METABOLIC_SCORE_BANDS: tuple[str, ...] = ("optimal", "low_risk", "increased_risk", "high_risk")
OXIDATIVE_STRESS_BANDS: tuple[str, ...] = ("low", "moderate", "high", "very_high")
PHYSICAL_ACTIVITY_BUCKETS: tuple[str, ...] = (
    "less_than_30mins",
    "30_60_mins",
    "more_than_60_mins",
    "rarely_or_never",
    "unmapped",
)
_OPTION_VALUE_TO_PHYSICAL_ACTIVITY_BUCKET: dict[str, str] = {
    "1": "less_than_30mins",
    "2": "30_60_mins",
    "3": "more_than_60_mins",
    "5": "rarely_or_never",
    "less than 30 minutes a day": "less_than_30mins",
    "30-60 minutes a day": "30_60_mins",
    "more than 60 minutes a day": "more_than_60_mins",
    "rarely or never": "rarely_or_never",
}
# Metsights OPTIONS for physical_activity_frequency are 1/2/3/5 only (no 4).
PHYSICAL_ACTIVITY_VALID_OPTION_VALUES: frozenset[str] = frozenset({"1", "2", "3", "5"})

SLEEPING_HOURS_BUCKETS: tuple[str, ...] = (
    "less_than_5hrs",
    "between_5_7_hrs",
    "between_7_9_hrs",
    "more_than_9hrs",
    "unmapped",
)
_OPTION_VALUE_TO_SLEEPING_HOURS_BUCKET: dict[str, str] = {
    "0": "less_than_5hrs",
    "1": "between_5_7_hrs",
    "2": "between_7_9_hrs",
    "3": "more_than_9hrs",
    "less than 5 hours": "less_than_5hrs",
    "between 5 to 7 hours": "between_5_7_hrs",
    "between 7 to 9 hours": "between_7_9_hrs",
    "more than 9 hours": "more_than_9hrs",
}
SLEEPING_HOURS_VALID_OPTION_VALUES: frozenset[str] = frozenset({"0", "1", "2", "3"})


def normalize_questionnaire_answer(answer: object | None) -> str | None:
    """Extract a comparable scalar string from a stored questionnaire JSON answer."""
    if answer is None:
        return None
    if isinstance(answer, list):
        if not answer:
            return None
        return normalize_questionnaire_answer(answer[0])
    if isinstance(answer, dict):
        if "value" in answer:
            return normalize_questionnaire_answer(answer.get("value"))
        if "option_value" in answer:
            return normalize_questionnaire_answer(answer.get("option_value"))
        return None
    if isinstance(answer, bool):
        return None
    if isinstance(answer, float) and answer.is_integer():
        return str(int(answer))
    if isinstance(answer, (int, float)):
        text = str(answer).strip()
        return text or None
    text = str(answer).strip()
    if not text:
        return None
    # JSON string answers sometimes arrive still quoted.
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1].strip()
    return text or None


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


def extract_diseases(reports: dict) -> list[Any]:
    """Read diseases array from Metsights report JSON (top-level or nested data)."""
    diseases = reports.get("diseases")
    if isinstance(diseases, list):
        return diseases
    data = reports.get("data")
    if isinstance(data, dict):
        nested = data.get("diseases")
        if isinstance(nested, list):
            return nested
    return []


def extract_oxidative_stress_score(reports: dict) -> float | None:
    """Read oxidative_stress risk_score_scaled from diseases in report JSON."""
    for entry in extract_diseases(reports):
        if not isinstance(entry, dict):
            continue
        if entry.get("code") != "oxidative_stress":
            continue
        score = entry.get("risk_score_scaled")
        if isinstance(score, (int, float)):
            return float(score)
    return None


def oxidative_stress_to_band(score: float) -> str:
    if score <= 25:
        return "low"
    if score <= 42:
        return "moderate"
    if score <= 58:
        return "high"
    return "very_high"


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
            "nutritionist_consultation": int(metrics["nutritionist_consultation"]),
            "doctor_and_nutritionist_consultation": int(
                metrics["doctor_and_nutritionist_consultation"]
            ),
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
    """Map questionnaire option_value or display text to a physical activity bucket key.

    Returns ``unmapped`` when an answer exists but is not a known Metsights choice
    (so chart totals stay equal to questionnaire responders). Returns ``None`` only
    when there is no answer to count.
    """
    normalized = normalize_questionnaire_answer(answer)
    if normalized is None:
        return None
    mapped = _OPTION_VALUE_TO_PHYSICAL_ACTIVITY_BUCKET.get(normalized.lower())
    if mapped is not None:
        return mapped
    return "unmapped"


def _build_gender_distribution(counts: dict[str, int], buckets: tuple[str, ...]) -> dict:
    total = sum(counts[bucket] for bucket in buckets)
    count = [counts[bucket] for bucket in buckets]
    percent = [_percent(c, total) for c in count]
    mapped_total = total - counts.get("unmapped", 0)
    payload = {
        "group": list(buckets),
        "count": count,
        "percent": percent,
        "total_responded": total,
        "mapped_responded": mapped_total,
        "unmapped_responded": counts.get("unmapped", 0),
    }
    return payload


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
    """Map questionnaire option_value or display text to a sleeping hours bucket key.

    Returns ``unmapped`` for unrecognized non-empty answers so chart totals match
    responders. Returns ``None`` only when there is no answer.
    """
    normalized = normalize_questionnaire_answer(answer)
    if normalized is None:
        return None
    mapped = _OPTION_VALUE_TO_SLEEPING_HOURS_BUCKET.get(normalized.lower())
    if mapped is not None:
        return mapped
    return "unmapped"


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


def build_overall_risk_score(
    scores: list[float],
    *,
    total_enrolled: int | None = None,
    bio_ai_reports: int | None = None,
) -> dict:
    """Build overall_risk_score section payload from metabolic scores.

    ``total_employees`` is the number of participants with an extractable
    metabolic_score (Bio AI report JSON), not total camp enrollment.
    """
    counts = {band: 0 for band in METABOLIC_SCORE_BANDS}
    for score in scores:
        counts[metabolic_score_to_band(score)] += 1

    total = len(scores)
    count = [counts[band] for band in METABOLIC_SCORE_BANDS]
    percent = [_percent(c, total) for c in count]
    elevated = _percent(counts["increased_risk"] + counts["high_risk"], total)

    enrolled = int(total_enrolled) if total_enrolled is not None else total
    reports = int(bio_ai_reports) if bio_ai_reports is not None else total
    missing = max(reports - total, 0)

    return {
        "data": {
            "group": list(METABOLIC_SCORE_BANDS),
            "count": count,
            "percent": percent,
            "total_employees": total,
            "total_with_metabolic_score": total,
            "total_enrolled": enrolled,
            "bio_ai_reports": reports,
            "missing_metabolic_score": missing,
            "elevated_metabolic_score": elevated,
        },
    }


DISEASE_RISK_BANDS: tuple[str, ...] = ("healthy", "increased", "high", "very_high")

CAMP_REPORT_DISEASE_CODES: tuple[str, ...] = (
    "type_2_diabetes",
    "hypertension",
    "obesity",
    "pcos_pcod",
    "nafld",
    "cardiac_health",
    "thyroid_health",
    "dyslipidemia",
)

_DASHBOARD_DISEASE_ALIASES: dict[str, tuple[str, ...]] = {
    "type_2_diabetes": ("type_2_diabetes", "diabetes"),
    "pcos_pcod": ("pcos_pcod", "pcos", "pcos/pcod"),
}


def _matches_disease_code(*, requested: str, report_code: str) -> bool:
    req = (requested or "").strip().lower()
    code = (report_code or "").strip().lower()
    if not req or not code:
        return False
    if code == req:
        return True
    if code.startswith(f"{req}/") or req.startswith(f"{code}/"):
        return True
    return False


def match_dashboard_disease_code(report_code: str) -> str | None:
    """Map a report diseases[].code value to a dashboard disease code, if recognized."""
    for dashboard_code in CAMP_REPORT_DISEASE_CODES:
        aliases = _DASHBOARD_DISEASE_ALIASES.get(dashboard_code, (dashboard_code,))
        for alias in aliases:
            if _matches_disease_code(requested=alias, report_code=report_code):
                return dashboard_code
    return None


def risk_score_scaled_to_band(score: float) -> str:
    if score <= 25:
        return "healthy"
    if score <= 42:
        return "increased"
    if score <= 58:
        return "high"
    return "very_high"


def extract_disease_risk_scores(reports: dict) -> dict[str, float]:
    """Return {dashboard_disease_code: risk_score_scaled} for diseases present in a report."""
    scores: dict[str, float] = {}
    for entry in extract_diseases(reports):
        if not isinstance(entry, dict):
            continue
        code = entry.get("code")
        if not isinstance(code, str):
            continue
        dashboard_code = match_dashboard_disease_code(code)
        if dashboard_code is None:
            continue
        risk_score = entry.get("risk_score_scaled")
        if not isinstance(risk_score, (int, float)):
            continue
        scores[dashboard_code] = float(risk_score)
    return scores


def _build_gender_risk_distribution(counts: dict[str, int], buckets: tuple[str, ...]) -> dict:
    distribution = _build_gender_distribution(counts, buckets)
    percent = distribution["percent"]
    distribution["elevated_percent"] = round(percent[2] + percent[3], 1)
    return distribution


def build_distribution_by_gender_by_metabolic_syndrome(
    rows: list[tuple[str | None, dict]],
) -> dict:
    """Build distribution_by_gender_by_metabolic_syndrome from (gender, reports) rows."""
    disease_counts: dict[str, dict[str, dict[str, int]]] = {
        code: {
            "male": {band: 0 for band in DISEASE_RISK_BANDS},
            "female": {band: 0 for band in DISEASE_RISK_BANDS},
        }
        for code in CAMP_REPORT_DISEASE_CODES
    }

    for gender_raw, reports in rows:
        gender = normalize_camp_gender(gender_raw)
        if gender is None:
            continue
        for dashboard_code, risk_score in extract_disease_risk_scores(reports).items():
            band = risk_score_scaled_to_band(risk_score)
            disease_counts[dashboard_code][gender][band] += 1

    diseases: list[dict[str, Any]] = []
    for code in CAMP_REPORT_DISEASE_CODES:
        male_total = sum(disease_counts[code]["male"].values())
        female_total = sum(disease_counts[code]["female"].values())
        if male_total + female_total == 0:
            continue
        diseases.append(
            {
                "code": code,
                "male": _build_gender_risk_distribution(disease_counts[code]["male"], DISEASE_RISK_BANDS),
                "female": _build_gender_risk_distribution(disease_counts[code]["female"], DISEASE_RISK_BANDS),
            }
        )

    return {"data": {"diseases": diseases}}


def build_distribution_by_oxidative_stress(scores: list[float]) -> dict:
    """Build distribution_by_oxidative_stress section payload from oxidative stress scores."""
    counts = {band: 0 for band in OXIDATIVE_STRESS_BANDS}
    for score in scores:
        counts[oxidative_stress_to_band(score)] += 1

    total = len(scores)
    count = [counts[band] for band in OXIDATIVE_STRESS_BANDS]
    percent = [_percent(c, total) for c in count]
    elevated = _percent(counts["high"] + counts["very_high"], total)

    return {
        "data": {
            "group": list(OXIDATIVE_STRESS_BANDS),
            "count": count,
            "percent": percent,
            "total_employees": total,
            "elevated_oxidative_stress_percent": elevated,
        },
    }


def aggregate_top_healthy_habits(
    participant_habits: list[list[dict[str, Any]]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Top habits by how many participants have each habit_label."""
    counts: dict[str, int] = {}
    keys_by_label: dict[str, str | None] = {}
    for habits in participant_habits:
        for habit in habits:
            label = str(habit.get("habit_label") or "").strip()
            if not label:
                continue
            counts[label] = counts.get(label, 0) + 1
            if label not in keys_by_label:
                keys_by_label[label] = habit.get("habit_key")
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return [
        {"habit_key": keys_by_label[label], "habit_label": label}
        for label, _ in ranked[:limit]
    ]


def aggregate_top_healthy_profiles(
    participant_profiles: list[list[str]],
    *,
    limit: int = 3,
) -> list[str]:
    """Top profile group names by how many participants have each."""
    counts: dict[str, int] = {}
    for profiles in participant_profiles:
        for name in profiles:
            label = str(name or "").strip()
            if not label:
                continue
            counts[label] = counts.get(label, 0) + 1
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return [name for name, _ in ranked[:limit]]


def aggregate_top_low_risk(
    participant_low_risk: list[list[dict[str, Any]]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Top disease codes by how many participants have each in their low_risk list."""
    counts: dict[str, int] = {}
    meta_by_code: dict[str, dict[str, Any]] = {}
    for low_risk_items in participant_low_risk:
        for item in low_risk_items:
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            counts[code] = counts.get(code, 0) + 1
            if code not in meta_by_code:
                meta_by_code[code] = item
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return [meta_by_code[code] for code, _ in ranked[:limit]]


def build_positive_wins(
    *,
    low_risk: list[dict[str, Any]],
    healthy_habits: list[dict[str, Any]],
    healthy_profiles: list[str],
) -> dict:
    """Build positive_wins camp report section payload."""
    return {
        "data": {
            "low_risk": low_risk,
            "healthy_habits": healthy_habits,
            "healthy_profiles": healthy_profiles,
        },
    }


def build_company_average_scores(scores: list[dict[str, float | None]]) -> dict:
    """Build company_average_scores section payload from per-participant score dicts.

    Each entry in *scores* has keys "nutrition", "fitness", "lifestyle" with float or None.
    Averages each category across participants that have a valid (non-None) value.
    """
    totals: dict[str, float] = {"nutrition": 0.0, "fitness": 0.0, "lifestyle": 0.0}
    counts: dict[str, int] = {"nutrition": 0, "fitness": 0, "lifestyle": 0}

    for entry in scores:
        for key in ("nutrition", "fitness", "lifestyle"):
            val = entry.get(key)
            if val is not None:
                totals[key] += val
                counts[key] += 1

    data: dict[str, dict[str, int]] = {}
    for key in ("nutrition", "fitness", "lifestyle"):
        avg = round(totals[key] / counts[key]) if counts[key] > 0 else 0
        data[key] = {"score": avg}

    return {"data": data}


def build_blood_and_lab_intelligence(group_stats: dict[str, dict[str, dict[str, int]]]) -> dict:
    """Build blood_and_lab_intelligence section from pre-computed in-range stats.

    ``group_stats`` maps group_key -> {parameter_key: {"in_range": N, "total": N}}.
    """
    data: dict[str, dict[str, Any]] = {}
    for group_key, tests in group_stats.items():
        group_data: dict[str, Any] = {}
        for param_key, counts in tests.items():
            total = counts.get("total", 0)
            in_range = counts.get("in_range", 0)
            if total > 0:
                group_data[param_key] = {"in_range_percent": round(in_range / total * 100)}
            else:
                group_data[param_key] = {"in_range_percent": 0}
        data[group_key] = group_data
    return {"data": data}


SECTION_BUILDERS: dict[str, Callable[..., dict]] = {
    "participation_by_age": build_participation_by_age,
    "kpis": build_kpis,
    "overall_risk_score": build_overall_risk_score,
    "distribution_by_physical_activity_frequency": build_distribution_by_physical_activity_frequency,
    "distribution_by_sleeping_hours": build_distribution_by_sleeping_hours,
    "distribution_by_oxidative_stress": build_distribution_by_oxidative_stress,
    "distribution_by_gender_by_metabolic_syndrome": build_distribution_by_gender_by_metabolic_syndrome,
    "positive_wins": build_positive_wins,
    "company_average_scores": build_company_average_scores,
    "blood_and_lab_intelligence": build_blood_and_lab_intelligence,
    "ranking": lambda **_: {},  # computed in service via _compute_ranking_payload
}


def build_ranking(data: dict) -> dict:
    """Wrap ranking data dict into the standard section payload shape."""
    return {"data": data}
