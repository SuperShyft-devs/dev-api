"""Camp report behind-the-scenes (BTS) validation payloads."""

from __future__ import annotations

from typing import Any

from modules.reports.camp_report_section_builders import AGE_GROUPS


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

    details_out: dict[str, Any] = {
        "blood": details,
        "consultations": {},
        "questionnaire": questionnaire_details,
        "bio_ai_mismatch": bio_ai_mismatch,
        "risk_groups": risk_groups,
    }

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

    simple_specs: list[tuple[str, str, Any]] = [
        ("employees_enrolled", "people enrolled", _enrolled_reason),
        ("male_enrolled", "men enrolled", _enrolled_reason),
        ("female_enrolled", "women enrolled", _enrolled_reason),
        ("high_risk_group", "high-risk people", None),
        ("caution_risk_group", "caution-risk people", None),
        ("good_risk_group", "good-risk people", None),
        ("questionnaire_completed", "questionnaire completed", None),
        ("bio_ai_report_generated", "Bio AI reports generated", None),
        ("doctor_consultation", "doctor consultations", None),
        ("nutritionist_consultation", "nutritionist consultations", None),
        (
            "doctor_and_nutritionist_consultation",
            "doctor and nutritionist consultations",
            None,
        ),
        ("blood_test_percent", "blood-test coverage", None),
    ]

    for key, label, _reason_fn in simple_specs:
        expected = _int_or_none(expected_data.get(key))
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
        elif key in (
            "doctor_consultation",
            "nutritionist_consultation",
            "doctor_and_nutritionist_consultation",
        ):
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

    expected_consultations = expected_data.get("consultations") or {}
    stored_consultations = stored.get("consultations") if isinstance(stored.get("consultations"), dict) else {}
    if not isinstance(expected_consultations, dict):
        expected_consultations = {}
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
    return {
        "status": "ok" if all_match else "mismatch",
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": details_out,
        "message": (
            "All KPI numbers match."
            if all_match
            else "Some KPI numbers do not match. See the notes below for each one."
        ),
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
