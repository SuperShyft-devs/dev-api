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
    assert "counted 10" in bts["fields"]["employees_enrolled"]["reason"]
    assert bts["fields"]["total_blood_test"]["match"] is False
    assert "booking id" in bts["fields"]["total_blood_test"]["reason"].lower()
