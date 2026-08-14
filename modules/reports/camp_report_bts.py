"""Camp report behind-the-scenes (BTS) validation payloads."""

from __future__ import annotations

from typing import Any

from modules.reports.camp_report_section_builders import (
    AGE_GROUPS,
    METABOLIC_SCORE_BANDS,
    OXIDATIVE_STRESS_BANDS,
    _percent,
)


def build_not_implemented_bts(*, checked_at: str) -> dict[str, Any]:
    return {
        "status": "not_implemented",
        "checked_at": checked_at,
        "message": "Validation for this section is not available yet.",
    }


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _field_entry(
    *,
    expected: Any,
    stored: Any,
    reason: str | None,
) -> dict[str, Any]:
    match = expected == stored
    return {
        "match": match,
        "expected": expected,
        "stored": stored,
        "reason": None if match else reason,
    }


def _friendly_expert_label(key: str) -> str:
    parts = [p for p in str(key).split("_") if p]
    if not parts:
        return "consultation"
    if len(parts) == 1:
        return parts[0]
    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"
    return ", ".join(parts[:-1]) + f", and {parts[-1]}"


def _enrolled_reason(label: str, expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"This number was missing from the saved report. "
            f"When we counted the people enrolled in this camp, we got {expected}."
        )
    return (
        f"The saved report says {stored} {label}, but when we counted again we got {expected}. "
        f"These should be the same. Someone may have been added or removed since the report was last saved, "
        f"or the report may be out of date."
    )


def _blood_reason(
    *,
    expected: int,
    stored: int | None,
    blood_details: dict[str, int],
) -> str:
    with_booking = int(blood_details.get("with_booking_id", 0) or 0)
    with_collection = int(blood_details.get("with_metsights_collection", 0) or 0)
    missing = int(blood_details.get("missing_collection", 0) or 0)
    no_record = int(blood_details.get("no_record_id", 0) or 0)
    failed = int(blood_details.get("check_failed", 0) or 0)

    if stored is None:
        head = (
            f"The saved report did not have a blood-test count. "
            f"We found {expected} people who completed a blood test."
        )
    else:
        head = (
            f"The saved report says {stored} people completed a blood test, "
            f"but we found {expected}."
        )

    detail = (
        f" How we count: a person is included if they have a lab booking, "
        f"or if their health assessment shows a blood sample was collected. "
        f"Details — with a lab booking: {with_booking}; "
        f"with a collected sample (no booking on file): {with_collection}; "
        f"checked but no sample yet: {missing}; "
        f"no assessment record to check: {no_record}; "
        f"could not check right now: {failed}."
    )
    return head + detail


def _consultation_reason(label: str, expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show a number for {label}. "
            f"We found {expected} people who asked for this consultation."
        )
    return (
        f"The saved report says {stored} people asked for {label}, "
        f"but we found {expected}. "
        f"We only count people who said yes to this consultation."
    )


def _high_risk_reason(expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show a high-risk count. "
            f"We found {expected} people in the high-risk group."
        )
    return (
        f"The saved report says {stored} people are high risk, but we found {expected}. "
        f"Someone is high risk when their body age from the health report is at least "
        f"3 years older than their actual age."
    )


def _caution_risk_reason(expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show a caution-risk count. "
            f"We found {expected} people in the caution group."
        )
    return (
        f"The saved report says {stored} people are in caution, but we found {expected}. "
        f"Someone is in caution when their body age is a little older than their actual age "
        f"(more than 0 years older, but less than 3)."
    )


def _good_risk_reason(expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show a good-risk count. "
            f"We found {expected} people in the good group."
        )
    return (
        f"The saved report says {stored} people are in the good group, but we found {expected}. "
        f"Someone is in the good group when their body age is the same as or younger than "
        f"their actual age."
    )


def _questionnaire_reason(
    expected: int,
    stored: int | None,
    questionnaire_details: dict[str, Any] | None,
) -> str:
    details = dict(questionnaire_details or {})
    by_engagement = details.get("by_engagement") if isinstance(details.get("by_engagement"), list) else []
    sum_cards = _int_or_none(details.get("sum_filled_cards"))
    parts: list[str] = []
    if stored is None:
        parts.append(
            f"The saved report did not show how many people completed the questionnaire. "
            f"We counted {expected}."
        )
    else:
        parts.append(
            f"The saved report says {stored} people completed the questionnaire, "
            f"but we counted {expected}."
        )
    parts.append(
        " We count a person when every required question in their Metsights Pro/Basic "
        "health assessment categories is filled (same as the “Questionnaire Filled” card "
        "in Operations)."
    )
    if by_engagement:
        bits = []
        for row in by_engagement:
            if not isinstance(row, dict):
                continue
            name = row.get("engagement_name") or f"Session {row.get('engagement_id')}"
            filled = _int_or_none(row.get("filled")) or 0
            bits.append(f"{name}: {filled}")
        if bits:
            parts.append(" Per session — " + "; ".join(bits) + ".")
        if sum_cards is not None:
            parts.append(f" Sum of session cards: {sum_cards}.")
    return "".join(parts)


def _bio_ai_reason(
    expected: int,
    stored: int | None,
    mismatch_details: dict[str, Any] | None,
) -> str:
    details = dict(mismatch_details or {})
    people = details.get("people") if isinstance(details.get("people"), list) else []
    q_done = _int_or_none(details.get("questionnaire_completed"))
    if stored is None:
        head = (
            f"The saved report did not show how many Bio AI reports were generated. "
            f"We found {expected}."
        )
    else:
        head = (
            f"The saved report says {stored} Bio AI reports were generated, "
            f"but we found {expected}."
        )
    detail = (
        " We count a person when their Metsights Pro/Basic health report has been generated "
        "(the report content is not empty)."
    )
    if q_done is not None and q_done != expected:
        detail += (
            f" This usually lines up with questionnaire completed ({q_done}), "
            f"but right now it does not."
        )
    if people:
        sample = []
        for person in people[:5]:
            if not isinstance(person, dict):
                continue
            name = person.get("name") or "Someone"
            reasons = person.get("reasons") if isinstance(person.get("reasons"), list) else []
            reason_text = reasons[0] if reasons else "Needs a closer look."
            sample.append(f"{name}: {reason_text}")
        if sample:
            more = len(people) - len(sample)
            extra = f" (+{more} more people — see the list below.)" if more > 0 else ""
            detail += " Examples — " + " | ".join(sample) + extra
    return head + detail


def _risk_sum_reason(
    *,
    high: int,
    caution: int,
    good: int,
    bio_ai: int,
) -> str:
    total = high + caution + good
    return (
        f"High ({high}) + caution ({caution}) + good ({good}) = {total}, "
        f"but Bio AI reports generated is {bio_ai}. "
        f"These three risk groups should add up to the Bio AI count. "
        f"Refresh this section, or check the participant list below."
    )


def _percent_reason(expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show blood-test coverage (%). "
            f"Based on the latest counts it should be {expected}%."
        )
    return (
        f"The saved report shows {stored}% blood-test coverage, "
        f"but based on the latest enrollment and blood-test counts it should be {expected}%."
    )


# Fields added in later KPI shapes. If an older saved report is missing them,
# that is a schema upgrade — not a real number mismatch.
_KPI_SCHEMA_NEW_KEYS = frozenset(
    {
        "caution_risk_group",
        "good_risk_group",
        "questionnaire_completed",
        "bio_ai_report_generated",
    }
)

_LEGACY_CONSULTATION_KEYS = (
    "doctor_consultation",
    "nutritionist_consultation",
    "doctor_and_nutritionist_consultation",
)


def build_kpis_bts(
    *,
    expected_data: dict[str, Any],
    stored_data: dict[str, Any] | None,
    blood_details: dict[str, int] | None,
    checked_at: str,
    kpi_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare KPI report data to freshly computed expected values.

    ``stored_data`` should be the KPI ``data`` currently saved on the report
    (after refresh, the values just written).
    """
    stored = stored_data if isinstance(stored_data, dict) else {}
    details = dict(blood_details or {})
    kpi_details_payload = dict(kpi_details or {})
    questionnaire_details = (
        kpi_details_payload.get("questionnaire")
        if isinstance(kpi_details_payload.get("questionnaire"), dict)
        else {}
    )
    bio_ai_mismatch = (
        kpi_details_payload.get("bio_ai_mismatch")
        if isinstance(kpi_details_payload.get("bio_ai_mismatch"), dict)
        else {}
    )
    risk_groups = (
        kpi_details_payload.get("risk_groups")
        if isinstance(kpi_details_payload.get("risk_groups"), dict)
        else {}
    )
    fields: dict[str, Any] = {}
    newly_added_keys: list[str] = []

    details_out: dict[str, Any] = {
        "blood": details,
        "consultations": {},
        "questionnaire": questionnaire_details,
        "bio_ai_mismatch": bio_ai_mismatch,
        "risk_groups": risk_groups,
    }
    if "previous" in kpi_details_payload:
        details_out["previous"] = kpi_details_payload.get("previous")

    if not stored:
        expected_consultations = expected_data.get("consultations") or {}
        if not isinstance(expected_consultations, dict):
            expected_consultations = {}
        details_out["consultations"] = expected_consultations
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {},
            "details": details_out,
            "message": (
                "This is the first check for KPIs. "
                "We saved the latest numbers into the report."
            ),
        }

    expected_consultations = expected_data.get("consultations") or {}
    if not isinstance(expected_consultations, dict):
        expected_consultations = {}
    # Prefer consultations{} in the field table; skip duplicate legacy flat rows
    # when the nested map is present.
    include_legacy_consult_fields = not bool(expected_consultations)

    simple_specs: list[tuple[str, str, Any]] = [
        ("employees_enrolled", "people enrolled", _enrolled_reason),
        ("male_enrolled", "men enrolled", _enrolled_reason),
        ("female_enrolled", "women enrolled", _enrolled_reason),
        ("high_risk_group", "high-risk people", None),
        ("caution_risk_group", "caution-risk people", None),
        ("good_risk_group", "good-risk people", None),
        ("questionnaire_completed", "questionnaire completed", None),
        ("bio_ai_report_generated", "Bio AI reports generated", None),
        ("blood_test_percent", "blood-test coverage", None),
    ]
    if include_legacy_consult_fields:
        simple_specs.extend(
            [
                ("doctor_consultation", "doctor consultations", None),
                ("nutritionist_consultation", "nutritionist consultations", None),
                (
                    "doctor_and_nutritionist_consultation",
                    "doctor and nutritionist consultations",
                    None,
                ),
            ]
        )

    for key, label, _reason_fn in simple_specs:
        expected = _int_or_none(expected_data.get(key))
        if key not in stored and key in _KPI_SCHEMA_NEW_KEYS:
            # Older report shape: field did not exist yet. We just wrote it.
            newly_added_keys.append(key)
            fields[key] = {
                "match": True,
                "expected": expected,
                "stored": expected,
                "reason": None,
            }
            continue

        stored_val = _int_or_none(stored.get(key)) if key in stored else None
        if key == "blood_test_percent":
            reason = _percent_reason(expected or 0, stored_val)
        elif key == "high_risk_group":
            reason = _high_risk_reason(expected or 0, stored_val)
        elif key == "caution_risk_group":
            reason = _caution_risk_reason(expected or 0, stored_val)
        elif key == "good_risk_group":
            reason = _good_risk_reason(expected or 0, stored_val)
        elif key == "questionnaire_completed":
            reason = _questionnaire_reason(
                expected or 0,
                stored_val,
                questionnaire_details if isinstance(questionnaire_details, dict) else None,
            )
        elif key == "bio_ai_report_generated":
            reason = _bio_ai_reason(
                expected or 0,
                stored_val,
                bio_ai_mismatch if isinstance(bio_ai_mismatch, dict) else None,
            )
        elif key in _LEGACY_CONSULTATION_KEYS:
            reason = _consultation_reason(label, expected or 0, stored_val)
        else:
            reason = _enrolled_reason(label, expected or 0, stored_val)
        fields[key] = _field_entry(expected=expected, stored=stored_val, reason=reason)

    expected_blood = _int_or_none(expected_data.get("total_blood_test"))
    stored_blood = _int_or_none(stored.get("total_blood_test")) if "total_blood_test" in stored else None
    fields["total_blood_test"] = _field_entry(
        expected=expected_blood,
        stored=stored_blood,
        reason=_blood_reason(
            expected=expected_blood or 0,
            stored=stored_blood,
            blood_details=details,
        ),
    )

    stored_consultations = stored.get("consultations") if isinstance(stored.get("consultations"), dict) else {}
    details_out["consultations"] = expected_consultations

    # If the saved report is an older shape without consultations{}, fall back to
    # the legacy doctor/nutritionist fields so we do not false-flag a mismatch.
    legacy_fallback = {
        "doctor": _int_or_none(stored.get("doctor_consultation")),
        "nutritionist": _int_or_none(stored.get("nutritionist_consultation")),
        "doctor_nutritionist": _int_or_none(
            stored.get("doctor_and_nutritionist_consultation")
        ),
    }

    all_consult_keys = sorted(set(expected_consultations) | set(stored_consultations))
    for key in all_consult_keys:
        field_key = f"consultations.{key}"
        expected = _int_or_none(expected_consultations.get(key))
        if key in stored_consultations:
            stored_val = _int_or_none(stored_consultations.get(key))
        elif not stored_consultations and key in legacy_fallback:
            stored_val = legacy_fallback.get(key)
        else:
            stored_val = None
        label = f"{_friendly_expert_label(key)} consultations"
        fields[field_key] = _field_entry(
            expected=expected if expected is not None else 0,
            stored=stored_val,
            reason=_consultation_reason(label, expected or 0, stored_val),
        )

    high = _int_or_none(expected_data.get("high_risk_group")) or 0
    caution = _int_or_none(expected_data.get("caution_risk_group")) or 0
    good = _int_or_none(expected_data.get("good_risk_group")) or 0
    bio_ai = _int_or_none(expected_data.get("bio_ai_report_generated")) or 0
    risk_sum = high + caution + good
    risk_sum_matches = risk_sum == bio_ai
    fields["risk_groups_sum"] = {
        "match": risk_sum_matches,
        "expected": bio_ai,
        "stored": risk_sum,
        "reason": None
        if risk_sum_matches
        else _risk_sum_reason(high=high, caution=caution, good=good, bio_ai=bio_ai),
    }

    all_match = all(bool(entry.get("match")) for entry in fields.values())
    if all_match and newly_added_keys:
        message = (
            "All KPI numbers match. "
            "We also added new KPI fields that were not in the older saved report yet."
        )
    elif all_match:
        message = "All KPI numbers match."
    else:
        message = "Some KPI numbers do not match. See the notes below for each one."

    return {
        "status": "ok" if all_match else "mismatch",
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": details_out,
        "message": message,
    }


def _age_total_reason(expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show a total enrolled count. "
            f"When we counted unique people in this camp, we got {expected}."
        )
    return (
        f"The report says {stored} people enrolled, but when we counted unique people "
        f"in this camp again we got {expected}. "
        f"Someone may have been added or removed since the report was last saved."
    )


def _age_group_enrolled_reason(group: str, expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show how many people are in {group}. "
            f"We now count {expected}."
        )
    return (
        f"The report says {stored} people in {group}, but we now count {expected}. "
        f"Ages can change if a date of birth was updated, or people were added or removed."
    )


def _age_group_percent_reason(group: str, expected: float, stored: float | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show the share for {group}. "
            f"Based on the latest counts it should be {expected}%."
        )
    return (
        f"The share for {group} should be {expected}% based on the latest counts, "
        f"but the report shows {stored}%. "
        f"Percents are calculated from the age-group counts and the total."
    )


def _age_groups_structure_reason(expected_groups: list[str], stored_groups: Any) -> str:
    expected_text = ", ".join(expected_groups)
    return (
        f"The age groups in the saved report do not match the expected list ({expected_text}). "
        f"What we found in the report: {stored_groups!r}."
    )


def _age_buckets_sum_reason(stored_sum: int, stored_total: int | None) -> str:
    total_text = "missing" if stored_total is None else str(stored_total)
    return (
        f"The age-group counts in the saved report add up to {stored_sum}, "
        f"but the total enrolled shown is {total_text}. "
        f"The report data looks incomplete or out of date — refresh this section."
    )


_ORS_BAND_LABELS: dict[str, str] = {
    "optimal": "Optimal",
    "low_risk": "Low Risk",
    "increased_risk": "Increased Risk",
    "high_risk": "High Risk",
}


def _ors_band_label(band: str) -> str:
    return _ORS_BAND_LABELS.get(band, band.replace("_", " ").title())


def _ors_groups_structure_reason(expected_groups: list[str], stored_groups: Any) -> str:
    expected_text = ", ".join(expected_groups)
    return (
        f"The risk groups in the saved report do not match the expected list ({expected_text}). "
        f"What we found in the report: {stored_groups!r}."
    )


def _ors_count_reason(band: str, expected: int, stored: int | None) -> str:
    label = _ors_band_label(band)
    if stored is None:
        return (
            f"The saved report did not show how many people are in {label}. "
            f"We now count {expected}."
        )
    return (
        f"The report says {stored} people in {label}, but we now count {expected}. "
        f"Someone may have a new Bio AI report, an updated metabolic score, "
        f"or may have been added or removed since the report was last saved."
    )


def _ors_percent_reason(band: str, expected: float, stored: float | None) -> str:
    label = _ors_band_label(band)
    if stored is None:
        return (
            f"The saved report did not show the share for {label}. "
            f"Based on the latest counts it should be {expected}%."
        )
    return (
        f"The share for {label} should be {expected}% based on the latest counts, "
        f"but the report shows {stored}%. "
        f"Percents are calculated from the risk-group counts and the total with a score."
    )


def _ors_elevated_reason(expected: float, stored: float | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show the elevated metabolic score percent. "
            f"Based on Increased Risk + High Risk it should be {expected}%."
        )
    return (
        f"The elevated metabolic score should be {expected}%, but the report shows {stored}%. "
        f"This percent is Increased Risk plus High Risk, divided by everyone with a "
        f"metabolic score, then rounded to 1 decimal place."
    )


def _ors_counts_sum_reason(stored_sum: int, expected_total: int) -> str:
    return (
        f"The risk-group counts add up to {stored_sum}, "
        f"but the number of people with a metabolic score is {expected_total}. "
        f"These should match. The report data looks incomplete or out of date — "
        f"refresh this section."
    )


def _ors_elevated_consistency_reason(
    *,
    increased: int,
    high: int,
    total: int,
    expected_elevated: float,
    stored_elevated: float | None,
) -> str:
    elevated_count = increased + high
    stored_text = "missing" if stored_elevated is None else str(stored_elevated)
    return (
        f"Elevated percent should be {expected_elevated}% "
        f"({increased} Increased Risk + {high} High Risk = {elevated_count}, "
        f"then {elevated_count} ÷ {total} as a percent). "
        f"The report shows {stored_text}% instead."
    )


def build_overall_risk_score_bts(
    *,
    expected_data: dict[str, Any],
    stored_data: dict[str, Any] | None,
    details: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    """Compare overall_risk_score data to freshly computed expected values."""
    stored = stored_data if isinstance(stored_data, dict) else {}
    details_payload = dict(details or {})

    expected_groups = list(expected_data.get("group") or list(METABOLIC_SCORE_BANDS))
    expected_count = expected_data.get("count") or []
    expected_percent = expected_data.get("percent") or []
    expected_elevated = _float_or_none(expected_data.get("elevated_metabolic_score"))
    expected_total = (
        sum(_int_or_none(v) or 0 for v in expected_count)
        if isinstance(expected_count, list)
        else 0
    )

    if not stored:
        if expected_total == 0:
            message = (
                "This is the first check for Overall Risk Score. "
                "No one has a metabolic score yet, so every risk group is 0."
            )
        else:
            message = (
                "This is the first check for Overall Risk Score. "
                "We saved the latest numbers and listed who is in each risk group."
            )
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {},
            "details": details_payload,
            "message": message,
        }

    fields: dict[str, Any] = {}

    stored_groups = stored.get("group") if "group" in stored else None
    fields["group"] = _field_entry(
        expected=expected_groups,
        stored=stored_groups,
        reason=_ors_groups_structure_reason(expected_groups, stored_groups),
    )

    stored_count = stored.get("count") if isinstance(stored.get("count"), list) else None
    stored_percent = stored.get("percent") if isinstance(stored.get("percent"), list) else None

    for index, band in enumerate(expected_groups):
        expected_c = _int_or_none(expected_count[index]) if index < len(expected_count) else 0
        if stored_count is not None and index < len(stored_count):
            stored_c = _int_or_none(stored_count[index])
        else:
            stored_c = None
        fields[f"count.{band}"] = _field_entry(
            expected=expected_c if expected_c is not None else 0,
            stored=stored_c,
            reason=_ors_count_reason(band, expected_c or 0, stored_c),
        )

        expected_pct = (
            _float_or_none(expected_percent[index]) if index < len(expected_percent) else 0.0
        )
        if stored_percent is not None and index < len(stored_percent):
            stored_pct = _float_or_none(stored_percent[index])
        else:
            stored_pct = None
        fields[f"percent.{band}"] = _field_entry(
            expected=expected_pct if expected_pct is not None else 0.0,
            stored=stored_pct,
            reason=_ors_percent_reason(band, expected_pct or 0.0, stored_pct),
        )

    stored_elevated = (
        _float_or_none(stored.get("elevated_metabolic_score"))
        if "elevated_metabolic_score" in stored
        else None
    )
    fields["elevated_metabolic_score"] = _field_entry(
        expected=expected_elevated if expected_elevated is not None else 0.0,
        stored=stored_elevated,
        reason=_ors_elevated_reason(
            expected_elevated if expected_elevated is not None else 0.0,
            stored_elevated,
        ),
    )

    # Consistency: band counts must add up to people with a score.
    count_source = stored_count if isinstance(stored_count, list) and stored_count else expected_count
    stored_sum = (
        sum(_int_or_none(v) or 0 for v in count_source) if isinstance(count_source, list) else 0
    )
    sum_matches = stored_sum == expected_total
    fields["counts_sum"] = {
        "match": sum_matches,
        "expected": expected_total,
        "stored": stored_sum,
        "reason": None if sum_matches else _ors_counts_sum_reason(stored_sum, expected_total),
    }

    # Consistency: elevated % must match Increased + High over total with score.
    band_index = {band: i for i, band in enumerate(expected_groups)}
    increased_i = band_index.get("increased_risk")
    high_i = band_index.get("high_risk")
    increased = (
        _int_or_none(expected_count[increased_i]) or 0
        if increased_i is not None and increased_i < len(expected_count)
        else 0
    )
    high = (
        _int_or_none(expected_count[high_i]) or 0
        if high_i is not None and high_i < len(expected_count)
        else 0
    )
    recomputed_elevated = _percent(increased + high, expected_total)
    elevated_value = stored_elevated if stored_elevated is not None else expected_elevated
    elevated_ok = elevated_value == recomputed_elevated
    fields["elevated_consistency"] = {
        "match": elevated_ok,
        "expected": recomputed_elevated,
        "stored": elevated_value,
        "reason": None
        if elevated_ok
        else _ors_elevated_consistency_reason(
            increased=increased,
            high=high,
            total=expected_total,
            expected_elevated=recomputed_elevated,
            stored_elevated=elevated_value,
        ),
    }

    all_match = all(bool(entry.get("match")) for entry in fields.values())
    if all_match and expected_total == 0:
        message = "No one has a metabolic score yet, so every risk group is 0."
    elif all_match:
        message = "All Overall Risk Score numbers match."
    else:
        message = (
            "Some Overall Risk Score numbers do not match. "
            "See the notes below for each one."
        )

    return {
        "status": "ok" if all_match else "mismatch",
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": details_payload,
        "message": message,
    }


_OXIDATIVE_BAND_LABELS: dict[str, str] = {
    "low": "Low",
    "moderate": "Moderate",
    "high": "High",
    "very_high": "Very High",
}


def _oxidative_band_label(band: str) -> str:
    return _OXIDATIVE_BAND_LABELS.get(band, band.replace("_", " ").title())


def _oxidative_groups_structure_reason(expected_groups: list[str], stored_groups: Any) -> str:
    expected_text = ", ".join(expected_groups)
    return (
        f"The groups in the saved report do not match the expected list ({expected_text}). "
        f"What we found in the report: {stored_groups!r}."
    )


def _oxidative_count_reason(band: str, expected: int, stored: int | None) -> str:
    label = _oxidative_band_label(band)
    if stored is None:
        return (
            f"The saved report did not show how many people are in {label}. "
            f"We now count {expected}."
        )
    return (
        f"The report says {stored} people in {label}, but we now count {expected}. "
        f"Someone may have a new Bio AI report, an updated oxidative stress score, "
        f"or may have been added or removed since the report was last saved."
    )


def _oxidative_percent_reason(band: str, expected: float, stored: float | None) -> str:
    label = _oxidative_band_label(band)
    if stored is None:
        return (
            f"The saved report did not show the share for {label}. "
            f"Based on the latest counts it should be {expected}%."
        )
    return (
        f"The share for {label} should be {expected}% based on the latest counts, "
        f"but the report shows {stored}%. "
        f"Percents are calculated from the group counts and the total with a score."
    )


def _oxidative_elevated_reason(expected: float, stored: float | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show the elevated oxidative stress percent. "
            f"Based on High + Very High it should be {expected}%."
        )
    return (
        f"The elevated oxidative stress percent should be {expected}%, but the report shows {stored}%. "
        f"This percent is High plus Very High, divided by everyone with an "
        f"oxidative stress score, then rounded to 1 decimal place."
    )


def _oxidative_total_employees_reason(expected: int, stored: int | None) -> str:
    if stored is None:
        return (
            f"The saved report did not show how many people have an oxidative stress score. "
            f"We now count {expected}."
        )
    return (
        f"The report says {stored} people with a score, but we now count {expected}. "
        f"Someone may have gained or lost a score since the report was last saved."
    )


def _oxidative_counts_sum_reason(stored_sum: int, expected_total: int) -> str:
    return (
        f"The group counts add up to {stored_sum}, "
        f"but the number of people with an oxidative stress score is {expected_total}. "
        f"These should match. The report data looks incomplete or out of date — "
        f"refresh this section."
    )


def _oxidative_elevated_consistency_reason(
    *,
    high: int,
    very_high: int,
    total: int,
    expected_elevated: float,
    stored_elevated: float | None,
) -> str:
    elevated_count = high + very_high
    stored_text = "missing" if stored_elevated is None else str(stored_elevated)
    return (
        f"Elevated percent should be {expected_elevated}% "
        f"({high} High + {very_high} Very High = {elevated_count}, "
        f"then {elevated_count} ÷ {total} as a percent). "
        f"The report shows {stored_text}% instead."
    )


def build_distribution_by_oxidative_stress_bts(
    *,
    expected_data: dict[str, Any],
    stored_data: dict[str, Any] | None,
    details: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    """Compare distribution_by_oxidative_stress data to freshly computed expected values."""
    stored = stored_data if isinstance(stored_data, dict) else {}
    details_payload = dict(details or {})

    expected_groups = list(expected_data.get("group") or list(OXIDATIVE_STRESS_BANDS))
    expected_count = expected_data.get("count") or []
    expected_percent = expected_data.get("percent") or []
    expected_elevated = _float_or_none(expected_data.get("elevated_oxidative_stress_percent"))
    expected_total = _int_or_none(expected_data.get("total_employees"))
    if expected_total is None:
        expected_total = (
            sum(_int_or_none(v) or 0 for v in expected_count)
            if isinstance(expected_count, list)
            else 0
        )

    if not stored:
        if expected_total == 0:
            message = (
                "This is the first check for Oxidative Stress Distribution. "
                "No one has an oxidative stress score yet, so every group is 0."
            )
        else:
            message = (
                "This is the first check for Oxidative Stress Distribution. "
                "We saved the latest numbers and listed who is in each group."
            )
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {},
            "details": details_payload,
            "message": message,
        }

    fields: dict[str, Any] = {}

    stored_groups = stored.get("group") if "group" in stored else None
    fields["group"] = _field_entry(
        expected=expected_groups,
        stored=stored_groups,
        reason=_oxidative_groups_structure_reason(expected_groups, stored_groups),
    )

    stored_count = stored.get("count") if isinstance(stored.get("count"), list) else None
    stored_percent = stored.get("percent") if isinstance(stored.get("percent"), list) else None

    for index, band in enumerate(expected_groups):
        expected_c = _int_or_none(expected_count[index]) if index < len(expected_count) else 0
        if stored_count is not None and index < len(stored_count):
            stored_c = _int_or_none(stored_count[index])
        else:
            stored_c = None
        fields[f"count.{band}"] = _field_entry(
            expected=expected_c if expected_c is not None else 0,
            stored=stored_c,
            reason=_oxidative_count_reason(band, expected_c or 0, stored_c),
        )

        expected_pct = (
            _float_or_none(expected_percent[index]) if index < len(expected_percent) else 0.0
        )
        if stored_percent is not None and index < len(stored_percent):
            stored_pct = _float_or_none(stored_percent[index])
        else:
            stored_pct = None
        fields[f"percent.{band}"] = _field_entry(
            expected=expected_pct if expected_pct is not None else 0.0,
            stored=stored_pct,
            reason=_oxidative_percent_reason(band, expected_pct or 0.0, stored_pct),
        )

    stored_total = (
        _int_or_none(stored.get("total_employees"))
        if "total_employees" in stored
        else None
    )
    fields["total_employees"] = _field_entry(
        expected=expected_total,
        stored=stored_total,
        reason=_oxidative_total_employees_reason(expected_total, stored_total),
    )

    stored_elevated = (
        _float_or_none(stored.get("elevated_oxidative_stress_percent"))
        if "elevated_oxidative_stress_percent" in stored
        else None
    )
    fields["elevated_oxidative_stress_percent"] = _field_entry(
        expected=expected_elevated if expected_elevated is not None else 0.0,
        stored=stored_elevated,
        reason=_oxidative_elevated_reason(
            expected_elevated if expected_elevated is not None else 0.0,
            stored_elevated,
        ),
    )

    count_source = stored_count if isinstance(stored_count, list) and stored_count else expected_count
    stored_sum = (
        sum(_int_or_none(v) or 0 for v in count_source) if isinstance(count_source, list) else 0
    )
    sum_matches = stored_sum == expected_total
    fields["counts_sum"] = {
        "match": sum_matches,
        "expected": expected_total,
        "stored": stored_sum,
        "reason": None if sum_matches else _oxidative_counts_sum_reason(stored_sum, expected_total),
    }

    band_index = {band: i for i, band in enumerate(expected_groups)}
    high_i = band_index.get("high")
    very_high_i = band_index.get("very_high")
    high = (
        _int_or_none(expected_count[high_i]) or 0
        if high_i is not None and high_i < len(expected_count)
        else 0
    )
    very_high = (
        _int_or_none(expected_count[very_high_i]) or 0
        if very_high_i is not None and very_high_i < len(expected_count)
        else 0
    )
    recomputed_elevated = _percent(high + very_high, expected_total)
    elevated_value = stored_elevated if stored_elevated is not None else expected_elevated
    elevated_ok = elevated_value == recomputed_elevated
    fields["elevated_consistency"] = {
        "match": elevated_ok,
        "expected": recomputed_elevated,
        "stored": elevated_value,
        "reason": None
        if elevated_ok
        else _oxidative_elevated_consistency_reason(
            high=high,
            very_high=very_high,
            total=expected_total,
            expected_elevated=recomputed_elevated,
            stored_elevated=elevated_value,
        ),
    }

    all_match = all(bool(entry.get("match")) for entry in fields.values())
    if all_match and expected_total == 0:
        message = "No one has an oxidative stress score yet, so every group is 0."
    elif all_match:
        message = "All Oxidative Stress Distribution numbers match."
    else:
        message = (
            "Some Oxidative Stress Distribution numbers do not match. "
            "See the notes below for each one."
        )

    return {
        "status": "ok" if all_match else "mismatch",
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": details_payload,
        "message": message,
    }


def build_participation_by_age_bts(
    *,
    expected_data: dict[str, Any],
    stored_data: dict[str, Any] | None,
    details: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    """Compare participation_by_age data to freshly computed expected values."""
    stored = stored_data if isinstance(stored_data, dict) else {}
    details_payload = dict(details or {})

    if not stored:
        distinct = _int_or_none(expected_data.get("total_enrolled")) or 0
        if distinct == 0:
            message = (
                "This is the first check for Age wise Participation. "
                "No one is enrolled in this camp yet, so every age group is 0."
            )
        else:
            message = (
                "This is the first check for Age wise Participation. "
                "We saved the latest numbers and listed who is in each age group."
            )
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {},
            "details": details_payload,
            "message": message,
        }

    fields: dict[str, Any] = {}
    expected_groups = list(expected_data.get("age_group") or list(AGE_GROUPS))
    stored_groups = stored.get("age_group")
    fields["age_group"] = _field_entry(
        expected=expected_groups,
        stored=stored_groups if "age_group" in stored else None,
        reason=_age_groups_structure_reason(expected_groups, stored_groups),
    )

    expected_total = _int_or_none(expected_data.get("total_enrolled"))
    stored_total = (
        _int_or_none(stored.get("total_enrolled")) if "total_enrolled" in stored else None
    )
    fields["total_enrolled"] = _field_entry(
        expected=expected_total,
        stored=stored_total,
        reason=_age_total_reason(expected_total or 0, stored_total),
    )

    expected_enrolled = expected_data.get("enrolled") or []
    stored_enrolled = stored.get("enrolled") if isinstance(stored.get("enrolled"), list) else None
    expected_percent = expected_data.get("percent") or []
    stored_percent = stored.get("percent") if isinstance(stored.get("percent"), list) else None

    for index, group in enumerate(expected_groups):
        expected_count = (
            _int_or_none(expected_enrolled[index]) if index < len(expected_enrolled) else 0
        )
        if stored_enrolled is not None and index < len(stored_enrolled):
            stored_count = _int_or_none(stored_enrolled[index])
        else:
            stored_count = None
        fields[f"enrolled.{group}"] = _field_entry(
            expected=expected_count if expected_count is not None else 0,
            stored=stored_count,
            reason=_age_group_enrolled_reason(group, expected_count or 0, stored_count),
        )

        expected_pct = (
            _float_or_none(expected_percent[index]) if index < len(expected_percent) else 0.0
        )
        if stored_percent is not None and index < len(stored_percent):
            stored_pct = _float_or_none(stored_percent[index])
        else:
            stored_pct = None
        fields[f"percent.{group}"] = _field_entry(
            expected=expected_pct if expected_pct is not None else 0.0,
            stored=stored_pct,
            reason=_age_group_percent_reason(group, expected_pct or 0.0, stored_pct),
        )

    if isinstance(stored_enrolled, list) and stored_enrolled:
        stored_sum = sum(_int_or_none(v) or 0 for v in stored_enrolled)
        sum_matches_total = stored_total is not None and stored_sum == stored_total
        fields["buckets_sum"] = {
            "match": sum_matches_total,
            "expected": stored_total,
            "stored": stored_sum,
            "reason": None if sum_matches_total else _age_buckets_sum_reason(stored_sum, stored_total),
        }

    all_match = all(bool(entry.get("match")) for entry in fields.values())
    distinct = expected_total or 0
    if all_match and distinct == 0:
        message = "No one is enrolled in this camp yet, so every age group is 0."
    elif all_match:
        message = "All Age wise Participation numbers match."
    else:
        message = (
            "Some Age wise Participation numbers do not match. "
            "See the notes below for each one."
        )

    return {
        "status": "ok" if all_match else "mismatch",
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": details_payload,
        "message": message,
    }


def _qgd_bucket_label(bucket: str, bucket_labels: dict[str, str]) -> str:
    return bucket_labels.get(bucket, bucket.replace("_", " "))


def _qgd_gender_groups_reason(
    gender: str,
    expected_groups: list[str],
    stored_groups: Any,
    bucket_labels: dict[str, str],
) -> str:
    expected_text = ", ".join(_qgd_bucket_label(b, bucket_labels) for b in expected_groups)
    gender_label = "Men" if gender == "male" else "Women"
    return (
        f"The chart groups for {gender_label} in the saved report do not match the expected "
        f"list ({expected_text}). What we found in the report: {stored_groups!r}."
    )


def _qgd_count_reason(
    *,
    gender: str,
    bucket: str,
    expected: int,
    stored: int | None,
    bucket_labels: dict[str, str],
) -> str:
    gender_label = "Men" if gender == "male" else "Women"
    bucket_label = _qgd_bucket_label(bucket, bucket_labels)
    if stored is None:
        return (
            f"The saved report did not show how many {gender_label.lower()} chose "
            f"{bucket_label}. We now count {expected}."
        )
    return (
        f"The report says {stored} {gender_label.lower()} chose {bucket_label}, "
        f"but we now count {expected}. Someone may have updated their answer or "
        f"been added or removed since the report was last saved."
    )


def _qgd_percent_reason(
    *,
    gender: str,
    bucket: str,
    expected: float,
    stored: float | None,
    bucket_labels: dict[str, str],
) -> str:
    gender_label = "Men" if gender == "male" else "Women"
    bucket_label = _qgd_bucket_label(bucket, bucket_labels)
    if stored is None:
        return (
            f"The saved report did not show the share for {gender_label.lower()} in "
            f"{bucket_label}. Based on the latest counts it should be {expected}%."
        )
    return (
        f"The share for {gender_label.lower()} in {bucket_label} should be {expected}% "
        f"based on the latest counts, but the report shows {stored}%. "
        f"Percents are calculated from the group counts and the total for that gender."
    )


def _qgd_total_reason(gender: str, expected: int, stored: int | None) -> str:
    gender_label = "Men" if gender == "male" else "Women"
    if stored is None:
        return (
            f"The saved report did not show how many {gender_label.lower()} are on the chart. "
            f"We now count {expected}."
        )
    return (
        f"The report says {stored} {gender_label.lower()} are on the chart, "
        f"but we now count {expected}."
    )


def _qgd_counts_sum_reason(gender: str, stored_sum: int, expected_total: int) -> str:
    gender_label = "Men" if gender == "male" else "Women"
    return (
        f"The {gender_label.lower()} group counts add up to {stored_sum}, "
        f"but total responded for {gender_label.lower()} is {expected_total}. "
        f"These should match. The report data looks incomplete or out of date — "
        f"refresh this section."
    )


def _qgd_answered_vs_questionnaire_reason(
    *,
    answered: int,
    questionnaire_completed: int,
    chart_total: int,
    question_label: str,
) -> str:
    if answered == questionnaire_completed:
        return (
            f"{answered} people answered {question_label}, which matches Questionnaire "
            f"completed ({questionnaire_completed}). The chart shows {chart_total} people "
            f"because only known answers with male or female gender appear on the chart."
        )
    if answered > questionnaire_completed:
        extra = answered - questionnaire_completed
        return (
            f"{answered} people answered {question_label}, but Questionnaire completed is "
            f"{questionnaire_completed}. The extra {extra} "
            f"{'person' if extra == 1 else 'people'} answered this question without finishing "
            f"every required question in their Metsights Pro/Basic health assessment. "
            f"They are listed below. The chart shows {chart_total} people with a known answer "
            f"and male or female gender."
        )
    missing = questionnaire_completed - answered
    return (
        f"Questionnaire completed is {questionnaire_completed}, but only {answered} people "
        f"answered {question_label}. {missing} "
        f"{'person finished' if missing == 1 else 'people finished'} the questionnaire but "
        f"has no answer saved for this question. They are listed below. The chart shows "
        f"{chart_total} people."
    )


def _qgd_unknown_answers_reason(count: int, question_label: str) -> str:
    if count == 0:
        return None
    if count == 1:
        return (
            f"1 person answered {question_label} with a choice we could not map to the chart. "
            f"They are listed below under “Answer is not a known choice”."
        )
    return (
        f"{count} people answered {question_label} with choices we could not map to the chart. "
        f"They are listed below under “Answer is not a known choice”."
    )


def _qgd_unknown_gender_reason(count: int) -> str:
    if count == 0:
        return None
    if count == 1:
        return (
            "1 person answered this question but is not on the chart because their gender "
            "is not recorded as male or female. They are listed below."
        )
    return (
        f"{count} people answered this question but are not on the chart because their gender "
        "is not recorded as male or female. They are listed below."
    )


def build_questionnaire_gender_distribution_bts(
    *,
    expected_data: dict[str, Any],
    stored_data: dict[str, Any] | None,
    details: dict[str, Any],
    checked_at: str,
    section_title: str,
    bucket_labels: dict[str, str],
) -> dict[str, Any]:
    """Compare questionnaire gender distribution data to freshly computed expected values."""
    stored = stored_data if isinstance(stored_data, dict) else {}
    details_payload = dict(details or {})
    method = details_payload.get("method") if isinstance(details_payload.get("method"), dict) else {}
    comparison = (
        details_payload.get("comparison")
        if isinstance(details_payload.get("comparison"), dict)
        else {}
    )
    exceptions = (
        details_payload.get("exceptions")
        if isinstance(details_payload.get("exceptions"), dict)
        else {}
    )

    answered_this_question = _int_or_none(method.get("answered_this_question")) or 0
    questionnaire_completed = _int_or_none(method.get("questionnaire_completed")) or 0
    chart_total = _int_or_none(method.get("counted_on_chart")) or 0
    unknown_answer_count = len(exceptions.get("answer_not_a_known_choice") or [])
    unknown_gender_count = len(exceptions.get("gender_not_male_or_female") or [])

    if not stored:
        if chart_total == 0 and answered_this_question == 0:
            message = (
                f"This is the first check for {section_title}. "
                f"No one has answered yet, so every group is 0."
            )
        else:
            message = (
                f"This is the first check for {section_title}. "
                f"We saved the latest numbers and listed who is in each group."
            )
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {},
            "details": details_payload,
            "message": message,
        }

    fields: dict[str, Any] = {}

    for gender in ("male", "female"):
        expected_side = expected_data.get(gender) if isinstance(expected_data.get(gender), dict) else {}
        stored_side = stored.get(gender) if isinstance(stored.get(gender), dict) else {}

        expected_groups = list(expected_side.get("group") or [])
        stored_groups = stored_side.get("group") if "group" in stored_side else None
        fields[f"{gender}.group"] = _field_entry(
            expected=expected_groups,
            stored=stored_groups,
            reason=_qgd_gender_groups_reason(gender, expected_groups, stored_groups, bucket_labels),
        )

        expected_counts = expected_side.get("count") if isinstance(expected_side.get("count"), list) else []
        stored_counts = stored_side.get("count") if isinstance(stored_side.get("count"), list) else None
        expected_percents = (
            expected_side.get("percent") if isinstance(expected_side.get("percent"), list) else []
        )
        stored_percents = stored_side.get("percent") if isinstance(stored_side.get("percent"), list) else None

        for index, bucket in enumerate(expected_groups):
            expected_c = _int_or_none(expected_counts[index]) if index < len(expected_counts) else 0
            if stored_counts is not None and index < len(stored_counts):
                stored_c = _int_or_none(stored_counts[index])
            else:
                stored_c = None
            fields[f"count.{gender}.{bucket}"] = _field_entry(
                expected=expected_c if expected_c is not None else 0,
                stored=stored_c,
                reason=_qgd_count_reason(
                    gender=gender,
                    bucket=bucket,
                    expected=expected_c or 0,
                    stored=stored_c,
                    bucket_labels=bucket_labels,
                ),
            )

            expected_pct = (
                _float_or_none(expected_percents[index]) if index < len(expected_percents) else 0.0
            )
            if stored_percents is not None and index < len(stored_percents):
                stored_pct = _float_or_none(stored_percents[index])
            else:
                stored_pct = None
            fields[f"percent.{gender}.{bucket}"] = _field_entry(
                expected=expected_pct if expected_pct is not None else 0.0,
                stored=stored_pct,
                reason=_qgd_percent_reason(
                    gender=gender,
                    bucket=bucket,
                    expected=expected_pct or 0.0,
                    stored=stored_pct,
                    bucket_labels=bucket_labels,
                ),
            )

        expected_total = _int_or_none(expected_side.get("total_responded"))
        stored_total = (
            _int_or_none(stored_side.get("total_responded"))
            if "total_responded" in stored_side
            else None
        )
        fields[f"{gender}.total_responded"] = _field_entry(
            expected=expected_total if expected_total is not None else 0,
            stored=stored_total,
            reason=_qgd_total_reason(gender, expected_total or 0, stored_total),
        )

        count_source = (
            stored_counts if isinstance(stored_counts, list) and stored_counts else expected_counts
        )
        stored_sum = (
            sum(_int_or_none(v) or 0 for v in count_source) if isinstance(count_source, list) else 0
        )
        sum_matches = stored_sum == (expected_total or 0)
        fields[f"{gender}.counts_sum"] = {
            "match": sum_matches,
            "expected": expected_total or 0,
            "stored": stored_sum,
            "reason": None
            if sum_matches
            else _qgd_counts_sum_reason(gender, stored_sum, expected_total or 0),
        }

    question_label = str(method.get("question_label") or "this question")
    answered_match = answered_this_question == questionnaire_completed
    fields["answered_vs_questionnaire_completed"] = {
        "match": answered_match,
        "expected": questionnaire_completed,
        "stored": answered_this_question,
        "reason": None
        if answered_match
        else _qgd_answered_vs_questionnaire_reason(
            answered=answered_this_question,
            questionnaire_completed=questionnaire_completed,
            chart_total=chart_total,
            question_label=question_label,
        ),
    }

    unknown_answers_match = unknown_answer_count == 0
    fields["unknown_answers"] = {
        "match": unknown_answers_match,
        "expected": 0,
        "stored": unknown_answer_count,
        "reason": _qgd_unknown_answers_reason(unknown_answer_count, question_label),
    }

    unknown_gender_match = unknown_gender_count == 0
    fields["unknown_gender"] = {
        "match": unknown_gender_match,
        "expected": 0,
        "stored": unknown_gender_count,
        "reason": _qgd_unknown_gender_reason(unknown_gender_count),
    }

    chart_math_match = all(
        bool(entry.get("match"))
        for key, entry in fields.items()
        if key.endswith(".counts_sum") or key.startswith("count.") or key.startswith("percent.")
        or key.endswith(".total_responded") or key.endswith(".group")
    )
    cross_checks_match = answered_match and unknown_answers_match and unknown_gender_match

    if chart_math_match and cross_checks_match:
        if chart_total == 0:
            message = f"No one is on the {section_title} chart yet, so every group is 0."
        else:
            message = f"All {section_title} numbers match."
        status = "ok"
    elif chart_math_match and not cross_checks_match:
        message = (
            f"The {section_title} chart numbers are correct, but the total does not line up "
            f"with Questionnaire completed or some people were left off the chart. "
            f"See the notes below — this is often expected."
        )
        status = "mismatch"
    else:
        message = (
            f"Some {section_title} numbers do not match. See the notes below for each one."
        )
        status = "mismatch"

    return {
        "status": status,
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": details_payload,
        "message": message,
    }
