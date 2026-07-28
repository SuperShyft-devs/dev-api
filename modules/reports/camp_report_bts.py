"""Camp report behind-the-scenes (BTS) validation payloads."""

from __future__ import annotations

from typing import Any


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
) -> dict[str, Any]:
    """Compare KPI report data to freshly computed expected values.

    ``stored_data`` should be the KPI ``data`` currently saved on the report
    (after refresh, the values just written).
    """
    stored = stored_data if isinstance(stored_data, dict) else {}
    details = dict(blood_details or {})
    fields: dict[str, Any] = {}

    if not stored:
        expected_consultations = expected_data.get("consultations") or {}
        if not isinstance(expected_consultations, dict):
            expected_consultations = {}
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {},
            "details": {
                "blood": details,
                "consultations": expected_consultations,
            },
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

    all_match = all(bool(entry.get("match")) for entry in fields.values())
    return {
        "status": "ok" if all_match else "mismatch",
        "checked_at": checked_at,
        "expected": expected_data,
        "stored": stored_data,
        "fields": fields,
        "details": {
            "blood": details,
            "consultations": expected_consultations,
        },
        "message": (
            "All KPI numbers match."
            if all_match
            else "Some KPI numbers do not match. See the notes below for each one."
        ),
    }
