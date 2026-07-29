"""Unit tests for camp report BTS builders."""

from datetime import date

from modules.reports.camp_report_bts import (
    build_kpis_bts,
    build_not_implemented_bts,
    build_participation_by_age_bts,
)
from modules.reports.camp_report_section_builders import build_participation_by_age_details


def test_build_not_implemented_bts():
    payload = build_not_implemented_bts(checked_at="2026-07-28T00:00:00+00:00")
    assert payload["status"] == "not_implemented"
    assert "not available" in payload["message"].lower()


def test_build_kpis_bts_first_validation_ok():
    expected = {
        "employees_enrolled": 10,
        "male_enrolled": 6,
        "female_enrolled": 4,
        "total_blood_test": 8,
        "blood_test_percent": 80,
        "consultations": {"doctor": 3, "nutritionist": 2, "doctor_nutritionist": 1},
        "doctor_consultation": 3,
        "nutritionist_consultation": 2,
        "doctor_and_nutritionist_consultation": 1,
        "high_risk_group": 1,
    }
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=None,
        blood_details={"with_booking_id": 5, "with_metsights_collection": 3},
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["stored"] is None


def test_build_kpis_bts_mismatch_reason():
    expected = {
        "employees_enrolled": 10,
        "male_enrolled": 6,
        "female_enrolled": 4,
        "total_blood_test": 8,
        "blood_test_percent": 80,
        "consultations": {"doctor": 3},
        "doctor_consultation": 3,
        "nutritionist_consultation": 0,
        "doctor_and_nutritionist_consultation": 0,
        "high_risk_group": 1,
    }
    stored = {**expected, "employees_enrolled": 9, "total_blood_test": 5}
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=stored,
        blood_details={
            "with_booking_id": 4,
            "with_metsights_collection": 4,
            "missing_collection": 1,
            "no_record_id": 0,
            "check_failed": 0,
            "users_needing_metsights_check": 5,
        },
        checked_at="t",
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["employees_enrolled"]["match"] is False
    assert "counted again we got 10" in bts["fields"]["employees_enrolled"]["reason"]
    assert bts["fields"]["total_blood_test"]["match"] is False
    assert "lab booking" in bts["fields"]["total_blood_test"]["reason"].lower()


def test_build_kpis_bts_consultations_fallback_to_legacy_fields():
    """Older reports without consultations{} should not false-flag when legacy counts match."""
    expected = {
        "employees_enrolled": 140,
        "male_enrolled": 91,
        "female_enrolled": 49,
        "total_blood_test": 132,
        "blood_test_percent": 94,
        "consultations": {"doctor": 86, "nutritionist": 0, "doctor_nutritionist": 0},
        "doctor_consultation": 86,
        "nutritionist_consultation": 0,
        "doctor_and_nutritionist_consultation": 0,
        "high_risk_group": 27,
    }
    stored_legacy = {
        "employees_enrolled": 140,
        "male_enrolled": 91,
        "female_enrolled": 49,
        "total_blood_test": 132,
        "blood_test_percent": 94,
        "doctor_consultation": 86,
        "nutritionist_consultation": 0,
        "doctor_and_nutritionist_consultation": 0,
        "high_risk_group": 27,
    }
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=stored_legacy,
        blood_details={"with_booking_id": 100, "with_metsights_collection": 32},
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["fields"]["consultations.doctor"]["match"] is True
    assert bts["fields"]["consultations.doctor"]["stored"] == 86
    assert bts["fields"]["consultations.nutritionist"]["match"] is True
    assert bts["fields"]["consultations.doctor_nutritionist"]["match"] is True


def _age_expected_and_details():
    users = [
        (1, date(2004, 3, 12), 22, "Priya", "Sharma", 10),  # 22 -> 18–25
        (2, None, 16, "Kid", "One", 10),  # under 18 via profile -> 18–25
        (3, date(1990, 1, 1), 36, "Alex", "Lee", 11),  # 36 -> 36–45
    ]
    payload, details = build_participation_by_age_details(
        users,
        reference_date=date(2026, 6, 23),
        engagement_count=2,
        participant_rows=4,
        scope_label="Whole camp",
    )
    return payload["data"], details


def test_build_participation_by_age_bts_first_validation_ok():
    expected, details = _age_expected_and_details()
    bts = build_participation_by_age_bts(
        expected_data=expected,
        stored_data=None,
        details=details,
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["stored"] is None
    assert bts["fields"] == {}
    assert "first check" in bts["message"].lower()
    assert bts["details"]["method"]["distinct_people"] == 3
    assert bts["details"]["method"]["under_18_count"] == 1
    assert bts["details"]["method"]["age_from_profile"] == 1
    assert any("under 18" in note for note in bts["details"]["notes"])
    assert any("date of birth" in note for note in bts["details"]["notes"])
    assert any("already counted" in note for note in bts["details"]["notes"])
    people_18 = bts["details"]["age_groups"]["18–25"]["people"]
    assert len(people_18) == 2
    assert people_18[0]["name"] == "Kid One"
    assert people_18[0]["age_source"] == "profile_age"


def test_build_participation_by_age_bts_total_mismatch_reason():
    expected, details = _age_expected_and_details()
    stored = {**expected, "total_enrolled": 2}
    bts = build_participation_by_age_bts(
        expected_data=expected,
        stored_data=stored,
        details=details,
        checked_at="t",
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["total_enrolled"]["match"] is False
    assert "got 3" in bts["fields"]["total_enrolled"]["reason"]
    assert bts["fields"]["buckets_sum"]["match"] is False


def test_build_participation_by_age_bts_bucket_mismatch_reason():
    expected, details = _age_expected_and_details()
    stored_enrolled = list(expected["enrolled"])
    stored_enrolled[0] = stored_enrolled[0] - 1
    stored = {
        **expected,
        "enrolled": stored_enrolled,
        "percent": list(expected["percent"]),
    }
    bts = build_participation_by_age_bts(
        expected_data=expected,
        stored_data=stored,
        details=details,
        checked_at="t",
    )
    assert bts["status"] == "mismatch"
    field = bts["fields"]["enrolled.18–25"]
    assert field["match"] is False
    assert "18–25" in field["reason"]
    assert "date of birth" in field["reason"].lower()


def test_build_participation_by_age_bts_all_match():
    expected, details = _age_expected_and_details()
    bts = build_participation_by_age_bts(
        expected_data=expected,
        stored_data=expected,
        details=details,
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["fields"]["total_enrolled"]["match"] is True
    assert bts["fields"]["enrolled.18–25"]["match"] is True
    assert bts["fields"]["buckets_sum"]["match"] is True
    assert "match" in bts["message"].lower()


def test_build_participation_by_age_details_empty_camp():
    payload, details = build_participation_by_age_details(
        [],
        reference_date=date(2026, 6, 23),
        engagement_count=1,
        participant_rows=0,
        scope_label="Whole camp",
    )
    assert payload["data"]["total_enrolled"] == 0
    assert payload["data"]["enrolled"] == [0, 0, 0, 0, 0]
    assert details["method"]["distinct_people"] == 0
    bts = build_participation_by_age_bts(
        expected_data=payload["data"],
        stored_data=None,
        details=details,
        checked_at="t",
    )
    assert "no one is enrolled" in bts["message"].lower()
