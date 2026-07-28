"""Unit tests for camp report BTS builders."""

from modules.reports.camp_report_bts import build_kpis_bts, build_not_implemented_bts


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
