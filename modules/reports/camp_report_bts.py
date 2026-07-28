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


def _enrolled_reason(label: str, expected: int, stored: int | None) -> str:
    if stored is None:
        return f"The report was missing {label}. We counted {expected}."
    return (
        f"The report shows {stored} for {label}, but we counted {expected} "
        f"distinct enrolled people in this camp scope."
    )


def _blood_reason(
    *,
    expected: int,
    stored: int | None,
    blood_details: dict[str, int],
) -> str:
    parts = [
        f"The report shows {stored if stored is not None else 'no value'} blood tests, "
        f"but we found {expected}."
    ]
    parts.append(
        "Blood tests are counted when a person has a lab booking id, "
        "or when Metsights shows a sample collection for their Basic/Pro assessment."
    )
    parts.append(
        f"Breakdown: {blood_details.get('with_booking_id', 0)} with a booking id, "
        f"{blood_details.get('with_metsights_collection', 0)} with a Metsights sample collection, "
        f"{blood_details.get('missing_collection', 0)} with no sample collection, "
        f"{blood_details.get('no_record_id', 0)} with no Metsights record to check, "
        f"{blood_details.get('check_failed', 0)} we could not check."
    )
    return " ".join(parts)


def _consultation_reason(label: str, expected: int, stored: int | None) -> str:
    if stored is None:
        return f"The report was missing {label}. We counted {expected} people who requested this consultation."
    return (
        f"The report shows {stored} for {label}, but we counted {expected} "
        f"people who requested this consultation (want = yes)."
    )


def build_kpis_bts(
    *,
    expected_data: dict[str, Any],
    stored_data: dict[str, Any] | None,
    blood_details: dict[str, int] | None,
    checked_at: str,
) -> dict[str, Any]:
    """Compare KPI report data to freshly computed expected values."""
    stored = stored_data if isinstance(stored_data, dict) else {}
    details = dict(blood_details or {})
    fields: dict[str, Any] = {}

    simple_labels = {
        "employees_enrolled": "employees enrolled",
        "male_enrolled": "male enrolled",
        "female_enrolled": "female enrolled",
        "high_risk_group": "high risk group",
        "doctor_consultation": "doctor consultation",
        "nutritionist_consultation": "nutritionist consultation",
        "doctor_and_nutritionist_consultation": "doctor and nutritionist consultation",
        "blood_test_percent": "blood test percent",
    }

    for key, label in simple_labels.items():
        expected = _int_or_none(expected_data.get(key))
        stored_val = _int_or_none(stored.get(key)) if key in stored else None
        if key not in stored and not stored:
            reason = f"The report had no KPIs section yet. Expected {label} is {expected}."
        elif key == "blood_test_percent":
            reason = (
                f"The report shows {stored_val}% blood test coverage, "
                f"but based on the latest blood-test count it should be {expected}%."
            )
        elif key in (
            "doctor_consultation",
            "nutritionist_consultation",
            "doctor_and_nutritionist_consultation",
        ):
            reason = _consultation_reason(label, expected or 0, stored_val)
        elif key == "high_risk_group":
            reason = (
                f"The report shows {stored_val} high-risk people, but we counted {expected}. "
                "High risk means metabolic age is at least 3 years above chronological age."
            )
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

    all_consult_keys = sorted(set(expected_consultations) | set(stored_consultations))
    for key in all_consult_keys:
        field_key = f"consultations.{key}"
        expected = _int_or_none(expected_consultations.get(key))
        stored_val = _int_or_none(stored_consultations.get(key)) if key in stored_consultations else None
        label = key.replace("_", " + ")
        fields[field_key] = _field_entry(
            expected=expected if expected is not None else 0,
            stored=stored_val,
            reason=_consultation_reason(f"consultations ({label})", expected or 0, stored_val),
        )

    if not stored:
        return {
            "status": "ok",
            "checked_at": checked_at,
            "expected": expected_data,
            "stored": None,
            "fields": {
                key: {
                    "match": True,
                    "expected": entry["expected"],
                    "stored": None,
                    "reason": None,
                }
                for key, entry in fields.items()
            },
            "details": {
                "blood": details,
                "consultations": expected_consultations,
            },
            "message": "First validation for this section. Values were written to the report.",
        }

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
    }
