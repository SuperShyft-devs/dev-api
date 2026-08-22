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
)
PHYSICAL_ACTIVITY_BUCKET_LABELS: dict[str, str] = {
    "less_than_30mins": "Less than 30 minutes a day",
    "30_60_mins": "30–60 minutes a day",
    "more_than_60_mins": "More than 60 minutes a day",
    "rarely_or_never": "Rarely or never",
}
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
)
SLEEPING_HOURS_BUCKET_LABELS: dict[str, str] = {
    "less_than_5hrs": "Less than 5 hours",
    "between_5_7_hrs": "Between 5 and 7 hours",
    "between_7_9_hrs": "Between 7 and 9 hours",
    "more_than_9hrs": "More than 9 hours",
}
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


def metabolic_age_gap(*, metabolic_age: float | None, chronological_age: int) -> float:
    """Years by which metabolic age exceeds chronological age (missing MA → gap 0)."""
    effective_metabolic = metabolic_age if metabolic_age is not None else float(chronological_age)
    return float(effective_metabolic) - float(chronological_age)


def metabolic_risk_bucket(*, metabolic_age: float | None, chronological_age: int) -> str:
    """Classify Bio AI participants by metabolic-age gap.

    - high: gap >= 3
    - caution: 0 < gap < 3
    - good: gap <= 0 (includes missing metabolic_age)
    """
    gap_years = metabolic_age_gap(
        metabolic_age=metabolic_age,
        chronological_age=chronological_age,
    )
    if gap_years >= 3:
        return "high"
    if gap_years > 0:
        return "caution"
    return "good"


def is_high_metabolic_risk(*, metabolic_age: float | None, chronological_age: int) -> bool:
    """True when metabolic age gap is at least 3 years."""
    return (
        metabolic_risk_bucket(
            metabolic_age=metabolic_age,
            chronological_age=chronological_age,
        )
        == "high"
    )


def build_kpis(metrics: dict) -> dict:
    """Build kpis section payload from aggregated metrics."""
    enrolled = int(metrics["employees_enrolled"])
    blood = int(metrics["total_blood_test"])
    consultations = metrics.get("consultations") or {}
    return {
        "data": {
            "employees_enrolled": enrolled,
            "male_enrolled": int(metrics["male_enrolled"]),
            "female_enrolled": int(metrics["female_enrolled"]),
            "total_blood_test": blood,
            "blood_test_percent": round(blood / enrolled * 100) if enrolled else 0,
            "consultations": {str(k): int(v) for k, v in dict(consultations).items()},
            "doctor_consultation": int(metrics["doctor_consultation"]),
            "nutritionist_consultation": int(metrics["nutritionist_consultation"]),
            "doctor_and_nutritionist_consultation": int(
                metrics["doctor_and_nutritionist_consultation"]
            ),
            "questionnaire_completed": int(metrics.get("questionnaire_completed") or 0),
            "bio_ai_report_generated": int(metrics.get("bio_ai_report_generated") or 0),
            "high_risk_group": int(metrics["high_risk_group"]),
            "caution_risk_group": int(metrics.get("caution_risk_group") or 0),
            "good_risk_group": int(metrics.get("good_risk_group") or 0),
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


def _display_person_name(first_name: str | None, last_name: str | None) -> str:
    parts = [p.strip() for p in (first_name or "", last_name or "") if p and str(p).strip()]
    return " ".join(parts) if parts else "Unknown"


def build_participation_by_age_details(
    users: list[tuple[int, date | None, int, str | None, str | None, int]],
    *,
    reference_date: date,
    engagement_count: int,
    participant_rows: int,
    scope_label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build section data + BTS details from the age roster in one pass.

    ``users`` rows: (user_id, date_of_birth, age, first_name, last_name, engagement_id).
    Returns ``(section_payload, details)`` where section_payload matches
    ``build_participation_by_age`` shape (``{"data": ...}``).
    """
    total = len(users)
    groups: dict[str, dict[str, Any]] = {
        group: {"count": 0, "people": []} for group in AGE_GROUPS
    }
    age_from_dob = 0
    age_from_profile = 0
    under_18_count = 0

    for user_id, dob, stored_age, first_name, last_name, engagement_id in users:
        age = resolve_user_age(
            date_of_birth=dob,
            stored_age=stored_age,
            reference_date=reference_date,
        )
        if dob is not None:
            age_source = "date_of_birth"
            age_from_dob += 1
        else:
            age_source = "profile_age"
            age_from_profile += 1
        if age < 18:
            under_18_count += 1

        bucket = age_to_bucket(age)
        groups[bucket]["count"] += 1
        groups[bucket]["people"].append(
            {
                "user_id": int(user_id),
                "name": _display_person_name(first_name, last_name),
                "age_used": int(age),
                "age_source": age_source,
                "date_of_birth": dob.isoformat() if dob is not None else None,
                "profile_age": int(stored_age),
                "engagement_id": int(engagement_id),
            }
        )

    enrolled = [int(groups[group]["count"]) for group in AGE_GROUPS]
    percent = [_percent(count, total) for count in enrolled]

    for group in AGE_GROUPS:
        groups[group]["people"].sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    notes: list[str] = []
    if under_18_count == 1:
        notes.append(
            "1 person is under 18. They are included in the 18–25 group "
            "because of how age groups are set up."
        )
    elif under_18_count > 1:
        notes.append(
            f"{under_18_count} people are under 18. They are included in the 18–25 group "
            "because of how age groups are set up."
        )
    if age_from_profile == 1:
        notes.append(
            "1 person had no date of birth on file, so we used the age saved on their profile."
        )
    elif age_from_profile > 1:
        notes.append(
            f"{age_from_profile} people had no date of birth on file, "
            "so we used the age saved on their profile."
        )
    if participant_rows > total:
        duplicates = participant_rows - total
        if duplicates == 1:
            notes.append(
                "1 enrollment was for a person already counted once — "
                "each person is only counted once even if they joined more than one session."
            )
        else:
            notes.append(
                f"{duplicates} enrollments were for people already counted once — "
                "each person is only counted once even if they joined more than one session."
            )

    details: dict[str, Any] = {
        "method": {
            "reference_date": reference_date.isoformat(),
            "reference_date_label": "Camp start date",
            "counting_rule": (
                "Each person is counted once, even if they joined more than one session in this camp."
            ),
            "engagement_count": int(engagement_count),
            "participant_rows": int(participant_rows),
            "distinct_people": total,
            "age_from_date_of_birth": age_from_dob,
            "age_from_profile": age_from_profile,
            "under_18_count": under_18_count,
            "scope_label": scope_label,
        },
        "age_groups": groups,
        "notes": notes,
    }

    section_payload = {
        "data": {
            "age_group": list(AGE_GROUPS),
            "enrolled": enrolled,
            "percent": percent,
            "total_enrolled": total,
        },
    }
    return section_payload, details


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

    Returns ``None`` when there is no answer or the answer is not a known Metsights choice.
    """
    normalized = normalize_questionnaire_answer(answer)
    if normalized is None:
        return None
    return _OPTION_VALUE_TO_PHYSICAL_ACTIVITY_BUCKET.get(normalized.lower())


def _build_gender_distribution(counts: dict[str, int], buckets: tuple[str, ...]) -> dict:
    total = sum(counts[bucket] for bucket in buckets)
    count = [counts[bucket] for bucket in buckets]
    percent = [_percent(c, total) for c in count]
    return {
        "group": list(buckets),
        "count": count,
        "percent": percent,
        "total_responded": total,
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
    """Map questionnaire option_value or display text to a sleeping hours bucket key.

    Returns ``None`` when there is no answer or the answer is not a known Metsights choice.
    """
    normalized = normalize_questionnaire_answer(answer)
    if normalized is None:
        return None
    return _OPTION_VALUE_TO_SLEEPING_HOURS_BUCKET.get(normalized.lower())


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


def _format_answer_shown(answer: object | None) -> str | None:
    normalized = normalize_questionnaire_answer(answer)
    if normalized is not None:
        return normalized
    if answer is None:
        return None
    text = str(answer).strip()
    return text or None


def build_questionnaire_gender_distribution_details(
    roster: list[tuple[int, str | None, str | None, str | None, object | None]],
    *,
    filled_user_ids: set[int],
    questionnaire_completed: int,
    buckets: tuple[str, ...],
    answer_to_bucket: Callable[[object | None], str | None],
    bucket_labels: dict[str, str],
    scope_label: str,
    question_label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build gender-split questionnaire distribution + BTS details from enrolled roster.

    ``roster`` rows: (user_id, first_name, last_name, gender, answer).
    """
    groups: dict[str, dict[str, dict[str, Any]]] = {
        gender: {bucket: {"count": 0, "people": []} for bucket in buckets}
        for gender in ("male", "female")
    }
    exceptions: dict[str, list[dict[str, Any]]] = {
        "answered_without_finishing_questionnaire": [],
        "finished_questionnaire_without_this_answer": [],
        "answer_not_a_known_choice": [],
        "gender_not_male_or_female": [],
        "blank_answer": [],
    }

    answered_user_ids: set[int] = set()

    for user_id, first_name, last_name, gender_raw, answer in roster:
        uid = int(user_id)
        name = _display_person_name(first_name, last_name)
        gender = normalize_camp_gender(gender_raw)
        normalized = normalize_questionnaire_answer(answer)
        bucket = answer_to_bucket(answer) if normalized is not None else None
        questionnaire_filled = uid in filled_user_ids

        if answer is not None and normalized is None:
            exceptions["blank_answer"].append(
                {
                    "user_id": uid,
                    "name": name,
                    "reason": "A response was saved, but the answer was empty.",
                }
            )
            continue

        if normalized is None:
            if questionnaire_filled:
                exceptions["finished_questionnaire_without_this_answer"].append(
                    {
                        "user_id": uid,
                        "name": name,
                        "reason": (
                            "This person finished the full questionnaire, "
                            f"but has no answer saved for {question_label}."
                        ),
                    }
                )
            continue

        answered_user_ids.add(uid)

        if bucket is None:
            answer_shown = _format_answer_shown(answer) or normalized
            exceptions["answer_not_a_known_choice"].append(
                {
                    "user_id": uid,
                    "name": name,
                    "answer_shown": answer_shown,
                    "reason": (
                        f"Their answer ({answer_shown!r}) is not one of the usual choices "
                        f"for {question_label}, so they are not shown on the chart."
                    ),
                }
            )

        if gender is None:
            exceptions["gender_not_male_or_female"].append(
                {
                    "user_id": uid,
                    "name": name,
                    "answer_shown": bucket_labels.get(bucket, normalized) if bucket else normalized,
                    "reason": (
                        "This person answered the question, but their gender on file is "
                        "not male or female, so they cannot be placed on the men/women chart."
                    ),
                }
            )

        if not questionnaire_filled:
            exceptions["answered_without_finishing_questionnaire"].append(
                {
                    "user_id": uid,
                    "name": name,
                    "answer_shown": bucket_labels.get(bucket, normalized) if bucket else normalized,
                    "reason": (
                        f"This person answered {question_label}, but has not finished every "
                        "required question in their Metsights Pro/Basic health assessment. "
                        "Questionnaire completed in KPIs only counts a fully finished "
                        "Pro/Basic questionnaire."
                    ),
                }
            )

        if gender in ("male", "female") and bucket is not None:
            groups[gender][bucket]["count"] += 1
            groups[gender][bucket]["people"].append(
                {
                    "user_id": uid,
                    "name": name,
                    "answer_label": bucket_labels[bucket],
                }
            )

    male_counts = {bucket: int(groups["male"][bucket]["count"]) for bucket in buckets}
    female_counts = {bucket: int(groups["female"][bucket]["count"]) for bucket in buckets}
    male_data = _build_gender_distribution(male_counts, buckets)
    female_data = _build_gender_distribution(female_counts, buckets)

    for gender in ("male", "female"):
        for bucket in buckets:
            groups[gender][bucket]["people"].sort(
                key=lambda p: (p["name"].lower(), p["user_id"])
            )

    enrolled = len(roster)
    male_total = int(male_data["total_responded"])
    female_total = int(female_data["total_responded"])
    chart_total = male_total + female_total
    answered_this_question = len(answered_user_ids)
    not_on_chart = max(answered_this_question - chart_total, 0)

    notes: list[str] = []
    partial = exceptions["answered_without_finishing_questionnaire"]
    if len(partial) == 1:
        notes.append(
            "1 person answered this question without finishing the rest of the questionnaire. "
            "That is why the chart total may not match Questionnaire completed in KPIs."
        )
    elif len(partial) > 1:
        notes.append(
            f"{len(partial)} people answered this question without finishing the rest of the "
            "questionnaire. That is why the chart total may not match Questionnaire completed "
            "in KPIs."
        )

    missing = exceptions["finished_questionnaire_without_this_answer"]
    if len(missing) == 1:
        notes.append(
            "1 person finished the questionnaire but has no answer saved for this question."
        )
    elif len(missing) > 1:
        notes.append(
            f"{len(missing)} people finished the questionnaire but have no answer saved "
            "for this question."
        )

    unknown = exceptions["answer_not_a_known_choice"]
    if len(unknown) == 1:
        notes.append(
            "1 person gave an answer we could not map to a chart group. "
            "They are listed below and are not on the chart."
        )
    elif len(unknown) > 1:
        notes.append(
            f"{len(unknown)} people gave answers we could not map to a chart group. "
            "They are listed below and are not on the chart."
        )

    other_gender = exceptions["gender_not_male_or_female"]
    if len(other_gender) == 1:
        notes.append(
            "1 person answered this question but is not shown on the chart because "
            "their gender is not recorded as male or female."
        )
    elif len(other_gender) > 1:
        notes.append(
            f"{len(other_gender)} people answered this question but are not shown on the chart "
            "because their gender is not recorded as male or female."
        )

    if chart_total == questionnaire_completed and chart_total > 0:
        comparison_summary = (
            f"Everyone on the chart ({chart_total} people) matches Questionnaire completed "
            f"({questionnaire_completed})."
        )
    elif chart_total != questionnaire_completed:
        comparison_summary = (
            f"The chart shows {chart_total} people ({male_total} men + {female_total} women), "
            f"while Questionnaire completed is {questionnaire_completed}. "
            f"{answered_this_question} people answered this question in total. "
            "See the lists below for who is included or left out and why."
        )
    else:
        comparison_summary = (
            "No one is on the chart yet because nobody answered with a known choice "
            "and a recorded male or female gender."
        )

    for key in exceptions:
        exceptions[key].sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    details: dict[str, Any] = {
        "method": {
            "scope_label": scope_label,
            "question_label": question_label,
            "counting_rule": (
                f"We count each enrolled person once using their latest answer to {question_label} "
                "in this camp. Only answers that match a usual choice and a male or female gender "
                "appear on the chart."
            ),
            "who_is_included": (
                f"Enrolled people with a known answer to {question_label} and gender recorded as "
                "male or female."
            ),
            "who_is_excluded": (
                "People with no answer, an empty answer, an unrecognized answer, or gender not "
                "recorded as male or female."
            ),
            "enrolled": enrolled,
            "questionnaire_completed": questionnaire_completed,
            "answered_this_question": answered_this_question,
            "counted_on_chart": chart_total,
            "not_on_chart": not_on_chart,
        },
        "groups": groups,
        "exceptions": exceptions,
        "comparison": {
            "chart_total": chart_total,
            "male_total": male_total,
            "female_total": female_total,
            "answered_this_question": answered_this_question,
            "questionnaire_completed": questionnaire_completed,
            "summary": comparison_summary,
        },
        "notes": notes,
    }

    section_payload = {
        "data": {
            "male": male_data,
            "female": female_data,
        },
    }
    return section_payload, details


_METABOLIC_BAND_SCORE_RANGES: dict[str, str] = {
    "optimal": "0 to 25",
    "low_risk": "26 to 42",
    "increased_risk": "43 to 58",
    "high_risk": "59 and above",
}

_METABOLIC_EXCLUSION_REASON_LABELS: dict[str, str] = {
    "No Metsights Basic/Pro assessment instance for this camp": (
        "No Metsights Basic or Pro health assessment for this camp"
    ),
    "Bio AI not generated — report row missing or reports JSON is null "
    "(empty shell excluded from Overall Risk Score)": (
        "Bio AI report was not generated yet"
    ),
    "Bio AI not generated — reports JSON is empty (excluded from Overall Risk Score)": (
        "Bio AI report was not generated yet (empty report)"
    ),
    "Bio AI generated but metabolic_score field is missing from reports JSON": (
        "Bio AI report exists, but the metabolic score is missing"
    ),
}


def _friendly_metabolic_exclusion_reason(reason: str | None) -> str:
    if not reason:
        return "Not included in this chart"
    return _METABOLIC_EXCLUSION_REASON_LABELS.get(reason, reason)


def build_overall_risk_score(scores: list[float]) -> dict:
    """Build overall_risk_score section payload from metabolic scores.

    Inclusion rule: Bio AI must be generated (non-empty reports JSON). Banding uses
    extractable ``metabolic_score`` from that JSON. Only people with an extractable
    score appear in ``count`` / ``percent`` / ``elevated_metabolic_score``.
    """
    counts = {band: 0 for band in METABOLIC_SCORE_BANDS}
    for score in scores:
        counts[metabolic_score_to_band(score)] += 1

    total = len(scores)
    count = [counts[band] for band in METABOLIC_SCORE_BANDS]
    percent = [_percent(c, total) for c in count]
    elevated = _percent(counts["increased_risk"] + counts["high_risk"], total)

    return {
        "data": {
            "group": list(METABOLIC_SCORE_BANDS),
            "count": count,
            "percent": percent,
            "elevated_metabolic_score": elevated,
        },
    }


def build_elevated_metabolic_math(
    *,
    increased_risk_count: int,
    high_risk_count: int,
    total_with_score: int,
) -> dict[str, Any]:
    """Primary-school style steps for elevated_metabolic_score."""
    elevated_count = int(increased_risk_count) + int(high_risk_count)
    result_percent = _percent(elevated_count, total_with_score)

    if total_with_score <= 0:
        return {
            "increased_risk_count": int(increased_risk_count),
            "high_risk_count": int(high_risk_count),
            "elevated_count": elevated_count,
            "total_with_score": 0,
            "result_percent": 0.0,
            "steps": [
                "No one in this camp has a metabolic score yet.",
                "So we cannot calculate an elevated percentage (there is nothing to divide).",
                "Elevated metabolic score = 0%.",
            ],
        }

    ratio = elevated_count / total_with_score
    percent_raw = ratio * 100
    ratio_text = f"{ratio:.10f}".rstrip("0").rstrip(".") or "0"
    percent_raw_text = f"{percent_raw:.10f}".rstrip("0").rstrip(".") or "0"
    steps = [
        f"Step 1: Count people in Increased Risk = {increased_risk_count}",
        f"Step 2: Count people in High Risk = {high_risk_count}",
        (
            f"Step 3: Add them together: {increased_risk_count} + {high_risk_count} "
            f"= {elevated_count}"
        ),
        f"Step 4: Count everyone who has a metabolic score = {total_with_score}",
        f"Step 5: Divide: {elevated_count} ÷ {total_with_score} = {ratio_text}",
        f"Step 6: Turn into a percent: {ratio_text} × 100 = {percent_raw_text}",
        f"Step 7: Round to 1 decimal place: {result_percent}%",
    ]

    return {
        "increased_risk_count": int(increased_risk_count),
        "high_risk_count": int(high_risk_count),
        "elevated_count": elevated_count,
        "total_with_score": int(total_with_score),
        "result_percent": result_percent,
        "steps": steps,
    }


def build_overall_risk_score_details(
    status_rows: list[tuple[int, str | None, str | None, str | None, float | None, str | None]],
    *,
    total_enrolled: int,
    bio_ai_reports: int,
    scope_label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build slim section data + BTS details from metabolic score status rows.

    ``status_rows``: (user_id, first_name, last_name, gender, score, reason).
    ``reason`` is set when the person is excluded from the chart.
    """
    bands: dict[str, dict[str, Any]] = {
        band: {
            "count": 0,
            "score_range_label": _METABOLIC_BAND_SCORE_RANGES[band],
            "people": [],
        }
        for band in METABOLIC_SCORE_BANDS
    }
    excluded_people: list[dict[str, Any]] = []
    scores: list[float] = []

    for user_id, first_name, last_name, _gender, score, reason in status_rows:
        name = _display_person_name(first_name, last_name)
        if score is None or reason is not None:
            excluded_people.append(
                {
                    "user_id": int(user_id),
                    "name": name,
                    "reason": _friendly_metabolic_exclusion_reason(reason),
                }
            )
            continue

        band = metabolic_score_to_band(float(score))
        scores.append(float(score))
        bands[band]["count"] += 1
        bands[band]["people"].append(
            {
                "user_id": int(user_id),
                "name": name,
                "metabolic_score": float(score),
                "band": band,
            }
        )

    for band in METABOLIC_SCORE_BANDS:
        bands[band]["people"].sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    excluded_people.sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    payload = build_overall_risk_score(scores)
    data = payload["data"]
    total_with_score = sum(int(c) for c in data["count"])
    missing_metabolic_score = max(int(bio_ai_reports) - total_with_score, 0)

    increased = int(bands["increased_risk"]["count"])
    high = int(bands["high_risk"]["count"])
    elevated_math = build_elevated_metabolic_math(
        increased_risk_count=increased,
        high_risk_count=high,
        total_with_score=total_with_score,
    )

    notes: list[str] = []
    if total_with_score == 0:
        notes.append(
            "Nobody has a metabolic score yet, so every risk group is 0 and the elevated "
            "percentage is 0%."
        )
    if excluded_people:
        notes.append(
            f"{len(excluded_people)} enrolled "
            f"{'person was' if len(excluded_people) == 1 else 'people were'} "
            "not included in this chart — see the list below for why."
        )
    if missing_metabolic_score > 0:
        notes.append(
            f"{missing_metabolic_score} Bio AI "
            f"{'report has' if missing_metabolic_score == 1 else 'reports have'} "
            "no metabolic score, so they are left out of the risk groups."
        )

    details: dict[str, Any] = {
        "method": {
            "scope_label": scope_label,
            "counting_rule": (
                "We only count people who have a Metsights Basic or Pro Bio AI report "
                "with a metabolic score. FitPrint and empty reports are left out."
            ),
            "who_is_included": (
                "Enrolled people whose latest Basic/Pro health report has a metabolic score."
            ),
            "who_is_excluded": (
                "People with no Basic/Pro assessment, no Bio AI report yet, or a Bio AI report "
                "missing the metabolic score."
            ),
            "band_rules": [
                {"band": band, "score_range_label": _METABOLIC_BAND_SCORE_RANGES[band]}
                for band in METABOLIC_SCORE_BANDS
            ],
            "total_enrolled": int(total_enrolled),
            "bio_ai_reports": int(bio_ai_reports),
            "with_metabolic_score": total_with_score,
            "missing_metabolic_score": missing_metabolic_score,
            "excluded_people_count": len(excluded_people),
        },
        "elevated_math": elevated_math,
        "bands": bands,
        "excluded": {
            "count": len(excluded_people),
            "people": excluded_people,
        },
        "notes": notes,
    }
    return payload, details


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


_DISEASE_RISK_BAND_SCORE_RANGES: dict[str, str] = {
    "healthy": "0 to 25",
    "increased": "26 to 42",
    "high": "43 to 58",
    "very_high": "59 and above",
}

_DISEASE_RISK_BAND_LABELS: dict[str, str] = {
    "healthy": "Healthy",
    "increased": "Increased",
    "high": "High",
    "very_high": "Very High",
}

_CAMP_REPORT_DISEASE_LABELS: dict[str, str] = {
    "type_2_diabetes": "Type 2 Diabetes",
    "hypertension": "Hypertension",
    "obesity": "Obesity",
    "pcos_pcod": "PCOS/PCOD",
    "nafld": "NAFLD",
    "cardiac_health": "Cardiac Health",
    "thyroid_health": "Thyroid Health",
    "dyslipidemia": "Dyslipidemia",
}

_DISEASE_RISK_EXCLUSION_REASON_LABELS: dict[str, str] = {
    "No Metsights Basic/Pro assessment instance for this camp": (
        "No Metsights Basic or Pro health assessment for this camp"
    ),
    "Bio AI not generated — report row missing or reports JSON is null "
    "(empty shell excluded from Disease Risk by Gender)": (
        "Bio AI report was not generated yet"
    ),
    "Bio AI not generated — reports JSON is empty "
    "(excluded from Disease Risk by Gender)": (
        "Bio AI report was not generated yet (empty report)"
    ),
}


def _friendly_disease_risk_exclusion_reason(reason: str | None) -> str:
    if not reason:
        return "Not included in this chart"
    return _DISEASE_RISK_EXCLUSION_REASON_LABELS.get(reason, reason)


def _extract_disease_risk_entries(reports: dict) -> dict[str, tuple[float, str]]:
    """Return {dashboard_code: (risk_score_scaled, report_code)} from a Bio AI report."""
    entries: dict[str, tuple[float, str]] = {}
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
        entries[dashboard_code] = (float(risk_score), code)
    return entries


def build_band_percent_math(
    *,
    band_label: str,
    count: int,
    total: int,
) -> dict[str, Any]:
    """Primary-school style steps for one band percent."""
    result_percent = _percent(count, total)

    if total <= 0:
        return {
            "band_label": band_label,
            "count": int(count),
            "total": 0,
            "result_percent": 0.0,
            "steps": [
                f"No one has a score for this disease in this group ({band_label}),",
                "so the share is 0%.",
            ],
        }

    ratio = count / total
    percent_raw = ratio * 100
    ratio_text = f"{ratio:.10f}".rstrip("0").rstrip(".") or "0"
    percent_raw_text = f"{percent_raw:.10f}".rstrip("0").rstrip(".") or "0"
    steps = [
        f"Step 1: Count people in {band_label} = {count}",
        f"Step 2: Count everyone with a score for this disease = {total}",
        f"Step 3: Divide: {count} ÷ {total} = {ratio_text}",
        f"Step 4: Turn into a percent: {ratio_text} × 100 = {percent_raw_text}",
        f"Step 5: Round to 1 decimal place: {result_percent}%",
    ]

    return {
        "band_label": band_label,
        "count": int(count),
        "total": int(total),
        "result_percent": result_percent,
        "steps": steps,
    }


def build_elevated_disease_risk_math(
    *,
    high_count: int,
    very_high_count: int,
    high_percent: float,
    very_high_percent: float,
    total: int,
) -> dict[str, Any]:
    """Primary-school style steps for elevated_percent (sum of High% + Very High%)."""
    elevated_from_percents = round(high_percent + very_high_percent, 1)
    elevated_from_counts = _percent(high_count + very_high_count, total)

    if total <= 0:
        return {
            "kind": "disease_risk_by_gender",
            "high_count": int(high_count),
            "very_high_count": int(very_high_count),
            "high_percent": 0.0,
            "very_high_percent": 0.0,
            "elevated_count": int(high_count) + int(very_high_count),
            "total": 0,
            "result_percent": 0.0,
            "alternate_from_counts": 0.0,
            "steps": [
                "No one has a score for this disease in this gender group yet.",
                "So we cannot calculate an elevated percentage (there is nothing to divide).",
                "Elevated risk = 0%.",
            ],
        }

    sum_percents_text = f"{high_percent + very_high_percent:.10f}".rstrip("0").rstrip(".") or "0"
    steps = [
        f"Step 1: Count people in High = {high_count}",
        f"Step 2: High share = {high_percent}% (from {high_count} ÷ {total}, rounded to 1 decimal)",
        f"Step 3: Count people in Very High = {very_high_count}",
        f"Step 4: Very High share = {very_high_percent}% "
        f"(from {very_high_count} ÷ {total}, rounded to 1 decimal)",
        f"Step 5: Add the two shares: {high_percent} + {very_high_percent} = {sum_percents_text}",
        f"Step 6: Round to 1 decimal place: {elevated_from_percents}%",
        (
            f"Note: If you add the counts first ({high_count} + {very_high_count} = "
            f"{high_count + very_high_count}) and divide by {total}, you would get "
            f"{elevated_from_counts}%. We use the sum-of-shares method above."
        ),
    ]

    return {
        "kind": "disease_risk_by_gender",
        "high_count": int(high_count),
        "very_high_count": int(very_high_count),
        "high_percent": float(high_percent),
        "very_high_percent": float(very_high_percent),
        "elevated_count": int(high_count) + int(very_high_count),
        "total": int(total),
        "result_percent": elevated_from_percents,
        "alternate_from_counts": elevated_from_counts,
        "steps": steps,
    }


def _build_disease_gender_bts_side(
    *,
    gender: str,
    counts: dict[str, int],
) -> dict[str, Any]:
    total = sum(counts[band] for band in DISEASE_RISK_BANDS)
    groups: dict[str, dict[str, Any]] = {}
    percent_math: dict[str, Any] = {}

    for band in DISEASE_RISK_BANDS:
        band_label = _DISEASE_RISK_BAND_LABELS[band]
        count = int(counts[band])
        groups[band] = {
            "count": count,
            "score_range_label": _DISEASE_RISK_BAND_SCORE_RANGES[band],
            "people": [],
        }
        percent_math[band] = build_band_percent_math(
            band_label=band_label,
            count=count,
            total=total,
        )

    high_count = int(counts["high"])
    very_high_count = int(counts["very_high"])
    high_percent = _percent(high_count, total)
    very_high_percent = _percent(very_high_count, total)
    elevated_math = build_elevated_disease_risk_math(
        high_count=high_count,
        very_high_count=very_high_count,
        high_percent=high_percent,
        very_high_percent=very_high_percent,
        total=total,
    )

    return {
        "groups": groups,
        "percent_math": percent_math,
        "elevated_math": elevated_math,
        "total_responded": total,
    }


def build_distribution_by_gender_by_metabolic_syndrome_details(
    status_rows: list[tuple[int, str | None, str | None, str | None, dict[str, Any] | None, str | None]],
    *,
    total_enrolled: int,
    bio_ai_reports: int,
    scope_label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build section payload + BTS details from disease risk status rows."""
    rows_for_builder: list[tuple[str | None, dict]] = []
    excluded_people: list[dict[str, Any]] = []
    unknown_gender_people: list[dict[str, Any]] = []

    disease_side_data: dict[str, dict[str, dict[str, Any]]] = {
        code: {
            "male": _build_disease_gender_bts_side(gender="male", counts={b: 0 for b in DISEASE_RISK_BANDS}),
            "female": _build_disease_gender_bts_side(gender="female", counts={b: 0 for b in DISEASE_RISK_BANDS}),
            "not_counted": [],
            "notes": [],
        }
        for code in CAMP_REPORT_DISEASE_CODES
    }

    with_bio_ai = 0

    for user_id, first_name, last_name, gender_raw, reports, reason in status_rows:
        name = _display_person_name(first_name, last_name)
        uid = int(user_id)

        if reason is not None or reports is None:
            excluded_people.append(
                {
                    "user_id": uid,
                    "name": name,
                    "reason": _friendly_disease_risk_exclusion_reason(reason),
                }
            )
            continue

        with_bio_ai += 1
        gender = normalize_camp_gender(gender_raw)
        disease_entries = _extract_disease_risk_entries(reports)

        if gender is None:
            unknown_gender_people.append(
                {
                    "user_id": uid,
                    "name": name,
                    "reason": (
                        "This person has a Bio AI report, but their gender on file is "
                        "not recorded as male or female, so they cannot be placed on "
                        "the men/women chart."
                    ),
                }
            )
            continue

        rows_for_builder.append((gender_raw, reports))
        scored_codes = set(disease_entries.keys())

        for dashboard_code in CAMP_REPORT_DISEASE_CODES:
            if dashboard_code not in scored_codes:
                disease_side_data[dashboard_code]["not_counted"].append(
                    {
                        "user_id": uid,
                        "name": name,
                        "gender": gender,
                        "reason": (
                            "This person has a Bio AI report, but it does not include a "
                            f"numeric risk score for {_CAMP_REPORT_DISEASE_LABELS.get(dashboard_code, dashboard_code)}."
                        ),
                    }
                )

        for dashboard_code, (risk_score, report_code) in disease_entries.items():
            band = risk_score_scaled_to_band(risk_score)
            side = disease_side_data[dashboard_code][gender]
            side["groups"][band]["count"] += 1
            person_row: dict[str, Any] = {
                "user_id": uid,
                "name": name,
                "risk_score": float(risk_score),
                "band": band,
            }
            if report_code != dashboard_code:
                person_row["report_code"] = report_code
            side["groups"][band]["people"].append(person_row)

    section_payload = build_distribution_by_gender_by_metabolic_syndrome(rows_for_builder)

    diseases_details: dict[str, Any] = {}
    for code in CAMP_REPORT_DISEASE_CODES:
        male_side = disease_side_data[code]["male"]
        female_side = disease_side_data[code]["female"]

        for gender, side in (("male", male_side), ("female", female_side)):
            counts = {band: int(side["groups"][band]["count"]) for band in DISEASE_RISK_BANDS}
            people_by_band = {
                band: list(side["groups"][band]["people"]) for band in DISEASE_RISK_BANDS
            }
            rebuilt = _build_disease_gender_bts_side(gender=gender, counts=counts)
            for band in DISEASE_RISK_BANDS:
                rebuilt["groups"][band]["people"] = sorted(
                    people_by_band[band],
                    key=lambda p: (p["name"].lower(), p["user_id"]),
                )
            side["groups"] = rebuilt["groups"]
            side["percent_math"] = rebuilt["percent_math"]
            side["elevated_math"] = rebuilt["elevated_math"]
            side["total_responded"] = rebuilt["total_responded"]

        male_total = int(male_side["total_responded"])
        female_total = int(female_side["total_responded"])
        if male_total + female_total == 0:
            continue

        not_counted = disease_side_data[code]["not_counted"]
        not_counted.sort(key=lambda p: (p["name"].lower(), p["user_id"]))

        disease_notes: list[str] = list(disease_side_data[code]["notes"])
        if code == "pcos_pcod" and male_total == 0 and female_total > 0:
            disease_notes.append(
                "PCOS/PCOD scores are usually only present for women. "
                "Men with zero counts are expected."
            )
        if not_counted:
            disease_notes.append(
                f"{len(not_counted)} "
                f"{'person has' if len(not_counted) == 1 else 'people have'} "
                "a Bio AI report but no score for this disease — see the list below."
            )

        diseases_details[code] = {
            "label": _CAMP_REPORT_DISEASE_LABELS.get(code, code.replace("_", " ").title()),
            "male": {
                "groups": male_side["groups"],
                "percent_math": male_side["percent_math"],
                "elevated_math": male_side["elevated_math"],
                "total_responded": male_total,
            },
            "female": {
                "groups": female_side["groups"],
                "percent_math": female_side["percent_math"],
                "elevated_math": female_side["elevated_math"],
                "total_responded": female_total,
            },
            "not_counted": not_counted,
            "notes": disease_notes,
        }

    excluded_people.sort(key=lambda p: (p["name"].lower(), p["user_id"]))
    unknown_gender_people.sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    notes: list[str] = []
    if with_bio_ai == 0:
        notes.append(
            "Nobody has a usable Bio AI report yet, so every disease group is empty."
        )
    if excluded_people:
        notes.append(
            f"{len(excluded_people)} enrolled "
            f"{'person was' if len(excluded_people) == 1 else 'people were'} "
            "not included in this chart — see the list below for why."
        )
    if unknown_gender_people:
        notes.append(
            f"{len(unknown_gender_people)} "
            f"{'person has' if len(unknown_gender_people) == 1 else 'people have'} "
            "a Bio AI report but gender is not male or female."
        )

    details: dict[str, Any] = {
        "method": {
            "scope_label": scope_label,
            "counting_rule": (
                "For each disease, we count enrolled people by gender using the risk band "
                "from their latest Metsights Basic or Pro Bio AI report. Each person is "
                "counted separately for every disease that has a numeric risk score."
            ),
            "who_is_included": (
                "Enrolled people with gender recorded as male or female and a numeric "
                "risk_score_scaled for that disease in their latest Basic/Pro Bio AI report."
            ),
            "who_is_excluded": (
                "People with no Basic/Pro assessment, no Bio AI report yet, an empty Bio AI "
                "report, unknown gender, or a missing score for a specific disease."
            ),
            "band_rules": [
                {"band": band, "score_range_label": _DISEASE_RISK_BAND_SCORE_RANGES[band]}
                for band in DISEASE_RISK_BANDS
            ],
            "total_enrolled": int(total_enrolled),
            "bio_ai_reports": int(bio_ai_reports),
            "with_bio_ai_report": with_bio_ai,
            "unknown_gender_count": len(unknown_gender_people),
            "global_excluded_count": len(excluded_people),
        },
        "excluded": {
            "count": len(excluded_people),
            "people": excluded_people,
        },
        "unknown_gender": {
            "count": len(unknown_gender_people),
            "people": unknown_gender_people,
        },
        "diseases": diseases_details,
        "notes": notes,
    }

    return section_payload, details


_OXIDATIVE_BAND_SCORE_RANGES: dict[str, str] = {
    "low": "0 to 25",
    "moderate": "26 to 42",
    "high": "43 to 58",
    "very_high": "59 and above",
}

_OXIDATIVE_EXCLUSION_REASON_LABELS: dict[str, str] = {
    "No Metsights Basic/Pro assessment instance for this camp": (
        "No Metsights Basic or Pro health assessment for this camp"
    ),
    "Bio AI not generated — report row missing or reports JSON is null "
    "(empty shell excluded from Oxidative Stress Distribution)": (
        "Bio AI report was not generated yet"
    ),
    "Bio AI not generated — reports JSON is empty "
    "(excluded from Oxidative Stress Distribution)": (
        "Bio AI report was not generated yet (empty report)"
    ),
    "Bio AI generated but oxidative_stress risk_score_scaled is missing from reports JSON": (
        "Bio AI report exists, but the oxidative stress score is missing"
    ),
}


def _friendly_oxidative_exclusion_reason(reason: str | None) -> str:
    if not reason:
        return "Not included in this chart"
    return _OXIDATIVE_EXCLUSION_REASON_LABELS.get(reason, reason)


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


def build_elevated_oxidative_math(
    *,
    high_count: int,
    very_high_count: int,
    total_with_score: int,
) -> dict[str, Any]:
    """Primary-school style steps for elevated_oxidative_stress_percent."""
    elevated_count = int(high_count) + int(very_high_count)
    result_percent = _percent(elevated_count, total_with_score)

    if total_with_score <= 0:
        return {
            "kind": "oxidative_stress",
            "high_count": int(high_count),
            "very_high_count": int(very_high_count),
            "elevated_count": elevated_count,
            "total_with_score": 0,
            "result_percent": 0.0,
            "steps": [
                "No one in this camp has an oxidative stress score yet.",
                "So we cannot calculate an elevated percentage (there is nothing to divide).",
                "Elevated oxidative stress = 0%.",
            ],
        }

    ratio = elevated_count / total_with_score
    percent_raw = ratio * 100
    ratio_text = f"{ratio:.10f}".rstrip("0").rstrip(".") or "0"
    percent_raw_text = f"{percent_raw:.10f}".rstrip("0").rstrip(".") or "0"
    steps = [
        f"Step 1: Count people in High = {high_count}",
        f"Step 2: Count people in Very High = {very_high_count}",
        (
            f"Step 3: Add them together: {high_count} + {very_high_count} "
            f"= {elevated_count}"
        ),
        f"Step 4: Count everyone who has an oxidative stress score = {total_with_score}",
        f"Step 5: Divide: {elevated_count} ÷ {total_with_score} = {ratio_text}",
        f"Step 6: Turn into a percent: {ratio_text} × 100 = {percent_raw_text}",
        f"Step 7: Round to 1 decimal place: {result_percent}%",
    ]

    return {
        "kind": "oxidative_stress",
        "high_count": int(high_count),
        "very_high_count": int(very_high_count),
        "elevated_count": elevated_count,
        "total_with_score": int(total_with_score),
        "result_percent": result_percent,
        "steps": steps,
    }


def build_distribution_by_oxidative_stress_details(
    status_rows: list[tuple[int, str | None, str | None, str | None, float | None, str | None]],
    *,
    total_enrolled: int,
    bio_ai_reports: int,
    scope_label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build slim section data + BTS details from oxidative stress status rows."""
    bands: dict[str, dict[str, Any]] = {
        band: {
            "count": 0,
            "score_range_label": _OXIDATIVE_BAND_SCORE_RANGES[band],
            "people": [],
        }
        for band in OXIDATIVE_STRESS_BANDS
    }
    excluded_people: list[dict[str, Any]] = []
    scores: list[float] = []

    for user_id, first_name, last_name, _gender, score, reason in status_rows:
        name = _display_person_name(first_name, last_name)
        if score is None or reason is not None:
            excluded_people.append(
                {
                    "user_id": int(user_id),
                    "name": name,
                    "reason": _friendly_oxidative_exclusion_reason(reason),
                }
            )
            continue

        band = oxidative_stress_to_band(float(score))
        scores.append(float(score))
        bands[band]["count"] += 1
        bands[band]["people"].append(
            {
                "user_id": int(user_id),
                "name": name,
                "oxidative_stress_score": float(score),
                "band": band,
            }
        )

    for band in OXIDATIVE_STRESS_BANDS:
        bands[band]["people"].sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    excluded_people.sort(key=lambda p: (p["name"].lower(), p["user_id"]))

    payload = build_distribution_by_oxidative_stress(scores)
    data = payload["data"]
    total_with_score = sum(int(c) for c in data["count"])
    missing_oxidative_score = max(int(bio_ai_reports) - total_with_score, 0)

    high = int(bands["high"]["count"])
    very_high = int(bands["very_high"]["count"])
    elevated_math = build_elevated_oxidative_math(
        high_count=high,
        very_high_count=very_high,
        total_with_score=total_with_score,
    )

    notes: list[str] = []
    if total_with_score == 0:
        notes.append(
            "Nobody has an oxidative stress score yet, so every group is 0 and the elevated "
            "percentage is 0%."
        )
    if excluded_people:
        notes.append(
            f"{len(excluded_people)} enrolled "
            f"{'person was' if len(excluded_people) == 1 else 'people were'} "
            "not included in this chart — see the list below for why."
        )
    if missing_oxidative_score > 0:
        notes.append(
            f"{missing_oxidative_score} Bio AI "
            f"{'report has' if missing_oxidative_score == 1 else 'reports have'} "
            "no oxidative stress score, so they are left out of the groups."
        )

    details: dict[str, Any] = {
        "method": {
            "scope_label": scope_label,
            "counting_rule": (
                "We only count people who have a Metsights Basic or Pro Bio AI report "
                "with an oxidative stress score. FitPrint and empty reports are left out."
            ),
            "who_is_included": (
                "Enrolled people whose latest Basic/Pro health report has an oxidative stress score."
            ),
            "who_is_excluded": (
                "People with no Basic/Pro assessment, no Bio AI report yet, or a Bio AI report "
                "missing the oxidative stress score."
            ),
            "band_rules": [
                {"band": band, "score_range_label": _OXIDATIVE_BAND_SCORE_RANGES[band]}
                for band in OXIDATIVE_STRESS_BANDS
            ],
            "total_enrolled": int(total_enrolled),
            "bio_ai_reports": int(bio_ai_reports),
            "with_oxidative_stress_score": total_with_score,
            "missing_oxidative_stress_score": missing_oxidative_score,
            "excluded_people_count": len(excluded_people),
        },
        "elevated_math": elevated_math,
        "bands": bands,
        "excluded": {
            "count": len(excluded_people),
            "people": excluded_people,
        },
        "notes": notes,
    }
    return payload, details


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
