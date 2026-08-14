"""Unit tests for camp report BTS builders."""

from datetime import date

from modules.reports.camp_report_bts import (
    build_kpis_bts,
    build_not_implemented_bts,
    build_overall_risk_score_bts,
    build_participation_by_age_bts,
    build_questionnaire_gender_distribution_bts,
)
from modules.reports.camp_report_section_builders import (
    PHYSICAL_ACTIVITY_BUCKET_LABELS,
    PHYSICAL_ACTIVITY_BUCKETS,
    build_overall_risk_score,
    build_overall_risk_score_details,
    build_participation_by_age_details,
    build_questionnaire_gender_distribution_details,
    physical_activity_answer_to_bucket,
)


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
        "questionnaire_completed": 7,
        "bio_ai_report_generated": 7,
        "high_risk_group": 1,
        "caution_risk_group": 2,
        "good_risk_group": 4,
    }
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=None,
        blood_details={"with_booking_id": 5, "with_metsights_collection": 3},
        checked_at="t",
        kpi_details={
            "risk_groups": {"people": [], "counts": {"high": 1, "caution": 2, "good": 4}},
            "questionnaire": {"completed": 7, "by_engagement": [], "sum_filled_cards": 7},
            "bio_ai_mismatch": {"people": []},
        },
    )
    assert bts["status"] == "ok"
    assert bts["stored"] is None
    assert "risk_groups" in bts["details"]


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
        "questionnaire_completed": 8,
        "bio_ai_report_generated": 7,
        "high_risk_group": 1,
        "caution_risk_group": 2,
        "good_risk_group": 4,
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
        kpi_details={
            "questionnaire": {
                "completed": 8,
                "sum_filled_cards": 8,
                "by_engagement": [{"engagement_id": 1, "engagement_name": "Session A", "filled": 8}],
            },
            "bio_ai_mismatch": {
                "questionnaire_completed": 8,
                "bio_ai_report_generated": 7,
                "people": [
                    {
                        "user_id": 1,
                        "name": "Alex Lee",
                        "questionnaire_completed": True,
                        "bio_ai_report_generated": False,
                        "reasons": ["Blood report is not available yet."],
                    }
                ],
            },
            "risk_groups": {"people": []},
        },
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["employees_enrolled"]["match"] is False
    assert "counted again we got 10" in bts["fields"]["employees_enrolled"]["reason"]
    assert bts["fields"]["total_blood_test"]["match"] is False
    assert "lab booking" in bts["fields"]["total_blood_test"]["reason"].lower()
    assert bts["fields"]["risk_groups_sum"]["match"] is True
    assert bts["details"]["bio_ai_mismatch"]["people"][0]["name"] == "Alex Lee"


def test_build_kpis_bts_consultations_fallback_to_legacy_fields():
    """Older reports without consultations{} / new KPI keys should not false-flag."""
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
        "questionnaire_completed": 40,
        "bio_ai_report_generated": 81,
        "high_risk_group": 27,
        "caution_risk_group": 8,
        "good_risk_group": 46,
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
    assert "added new KPI fields" in bts["message"]
    assert bts["fields"]["consultations.doctor"]["match"] is True
    assert bts["fields"]["consultations.doctor"]["stored"] == 86
    assert bts["fields"]["consultations.nutritionist"]["match"] is True
    assert bts["fields"]["consultations.doctor_nutritionist"]["match"] is True
    # Newly introduced keys are treated as schema upgrade, not mismatches.
    assert bts["fields"]["questionnaire_completed"]["match"] is True
    assert bts["fields"]["questionnaire_completed"]["stored"] == 40
    assert bts["fields"]["bio_ai_report_generated"]["match"] is True
    assert bts["fields"]["caution_risk_group"]["match"] is True
    assert bts["fields"]["good_risk_group"]["match"] is True
    assert bts["fields"]["risk_groups_sum"]["match"] is True
    # Nested consultations{} already covers these — no duplicate flat rows.
    assert "doctor_consultation" not in bts["fields"]
    assert "nutritionist_consultation" not in bts["fields"]
    assert "doctor_and_nutritionist_consultation" not in bts["fields"]


def test_build_kpis_bts_new_field_wrong_value_still_mismatches():
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
        "questionnaire_completed": 8,
        "bio_ai_report_generated": 7,
        "high_risk_group": 1,
        "caution_risk_group": 2,
        "good_risk_group": 4,
    }
    stored = {**expected, "questionnaire_completed": 5}
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=stored,
        blood_details={},
        checked_at="t",
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["questionnaire_completed"]["match"] is False
    assert bts["fields"]["questionnaire_completed"]["stored"] == 5


def test_build_kpis_bts_risk_sum_integrity():
    expected = {
        "employees_enrolled": 3,
        "male_enrolled": 2,
        "female_enrolled": 1,
        "total_blood_test": 3,
        "blood_test_percent": 100,
        "consultations": {},
        "doctor_consultation": 0,
        "nutritionist_consultation": 0,
        "doctor_and_nutritionist_consultation": 0,
        "questionnaire_completed": 3,
        "bio_ai_report_generated": 3,
        "high_risk_group": 1,
        "caution_risk_group": 1,
        "good_risk_group": 0,
    }
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=expected,
        blood_details={},
        checked_at="t",
    )
    assert bts["fields"]["risk_groups_sum"]["match"] is False
    assert "add up" in (bts["fields"]["risk_groups_sum"]["reason"] or "").lower()
    assert bts["status"] == "mismatch"


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


def test_refresh_pattern_bts_ok_when_comparing_just_written_age_data():
    """Service validates the just-written section data, not the stale previous snapshot."""
    expected, details = _age_expected_and_details()
    stale_previous = {
        **expected,
        "total_enrolled": 99,
        "enrolled": [99, 0, 0, 0, 0],
        "percent": [100.0, 0.0, 0.0, 0.0, 0.0],
    }
    bts = build_participation_by_age_bts(
        expected_data=expected,
        stored_data=expected,
        details={**details, "previous": stale_previous},
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["details"]["previous"]["total_enrolled"] == 99


def test_refresh_pattern_bts_ok_when_comparing_just_written_kpi_data():
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
        "questionnaire_completed": 61,
        "bio_ai_report_generated": 81,
        "high_risk_group": 27,
        "caution_risk_group": 8,
        "good_risk_group": 46,
    }
    stale_previous = {
        "employees_enrolled": 1,
        "male_enrolled": 0,
        "female_enrolled": 0,
        "total_blood_test": 0,
        "blood_test_percent": 0,
        "doctor_consultation": 0,
        "nutritionist_consultation": 0,
        "doctor_and_nutritionist_consultation": 0,
        "high_risk_group": 0,
    }
    bts = build_kpis_bts(
        expected_data=expected,
        stored_data=expected,
        blood_details={},
        checked_at="t",
        kpi_details={"previous": stale_previous},
    )
    assert bts["status"] == "ok"
    assert bts["details"]["previous"]["employees_enrolled"] == 1


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


def test_build_overall_risk_score_bts_ok_and_math():
    expected = build_overall_risk_score([20.0, 30.0, 50.0, 70.0])["data"]
    _, details = build_overall_risk_score_details(
        [
            (1, "A", "One", None, 20.0, None),
            (2, "B", "Two", None, 30.0, None),
            (3, "C", "Three", None, 50.0, None),
            (4, "D", "Four", None, 70.0, None),
        ],
        total_enrolled=4,
        bio_ai_reports=4,
        scope_label="Whole camp",
    )
    bts = build_overall_risk_score_bts(
        expected_data=expected,
        stored_data=expected,
        details=details,
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["fields"]["elevated_metabolic_score"]["match"] is True
    assert bts["fields"]["counts_sum"]["match"] is True
    assert bts["fields"]["elevated_consistency"]["match"] is True
    assert bts["details"]["elevated_math"]["result_percent"] == 50.0
    assert "bands" in bts["details"]


def test_build_overall_risk_score_bts_mismatch_reasons():
    expected = {
        "group": ["optimal", "low_risk", "increased_risk", "high_risk"],
        "count": [1, 1, 1, 1],
        "percent": [25.0, 25.0, 25.0, 25.0],
        "elevated_metabolic_score": 50.0,
    }
    stored = {
        **expected,
        "count": [2, 0, 1, 1],
        "percent": [50.0, 0.0, 25.0, 25.0],
        "elevated_metabolic_score": 40.0,
        # Legacy keys should be ignored for matching.
        "total_employees": 4,
        "total_enrolled": 10,
    }
    bts = build_overall_risk_score_bts(
        expected_data=expected,
        stored_data=stored,
        details={},
        checked_at="t",
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["count.optimal"]["match"] is False
    assert "we now count 1" in bts["fields"]["count.optimal"]["reason"].lower()
    assert bts["fields"]["elevated_metabolic_score"]["match"] is False
    assert "should be 50.0%" in bts["fields"]["elevated_metabolic_score"]["reason"]


def test_build_overall_risk_score_bts_legacy_extra_keys_ignored_when_slim_matches():
    expected = build_overall_risk_score([20.0, 30.0, 50.0, 70.0])["data"]
    stored = {
        **expected,
        "total_employees": 4,
        "total_with_metabolic_score": 4,
        "total_enrolled": 10,
        "bio_ai_reports": 6,
        "missing_metabolic_score": 2,
    }
    bts = build_overall_risk_score_bts(
        expected_data=expected,
        stored_data=stored,
        details={},
        checked_at="t",
    )
    assert bts["status"] == "ok"


def test_build_overall_risk_score_bts_first_check_empty():
    expected = build_overall_risk_score([])["data"]
    bts = build_overall_risk_score_bts(
        expected_data=expected,
        stored_data=None,
        details={"elevated_math": {"steps": ["No one has a score."]}},
        checked_at="t",
    )
    assert bts["status"] == "ok"
    assert bts["stored"] is None
    assert "no one has a metabolic score" in bts["message"].lower()


def test_build_overall_risk_score_bts_elevated_consistency_mismatch():
    expected = {
        "group": ["optimal", "low_risk", "increased_risk", "high_risk"],
        "count": [1, 1, 1, 1],
        "percent": [25.0, 25.0, 25.0, 25.0],
        "elevated_metabolic_score": 50.0,
    }
    stored = {
        **expected,
        "elevated_metabolic_score": 12.0,
    }
    bts = build_overall_risk_score_bts(
        expected_data=expected,
        stored_data=stored,
        details={},
        checked_at="t",
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["elevated_consistency"]["match"] is False
    assert "Increased Risk" in bts["fields"]["elevated_consistency"]["reason"]


def _physical_activity_expected_and_details():
    roster = [
        (1, "Alex", "Lee", "male", "1"),
        (2, "Sam", "Kim", "female", "2"),
    ]
    payload, details = build_questionnaire_gender_distribution_details(
        roster,
        filled_user_ids={1, 2},
        questionnaire_completed=2,
        buckets=PHYSICAL_ACTIVITY_BUCKETS,
        answer_to_bucket=physical_activity_answer_to_bucket,
        bucket_labels=PHYSICAL_ACTIVITY_BUCKET_LABELS,
        scope_label="Whole camp",
        question_label="daily physical activity",
    )
    return payload["data"], details


def test_build_questionnaire_gender_distribution_bts_all_match():
    expected, details = _physical_activity_expected_and_details()
    bts = build_questionnaire_gender_distribution_bts(
        expected_data=expected,
        stored_data=expected,
        details=details,
        checked_at="t",
        section_title="Physical activity",
        bucket_labels=PHYSICAL_ACTIVITY_BUCKET_LABELS,
    )
    assert bts["status"] == "ok"
    assert bts["fields"]["male.total_responded"]["match"] is True
    assert bts["fields"]["answered_vs_questionnaire_completed"]["match"] is True
    assert bts["fields"]["unknown_answers"]["match"] is True


def test_build_questionnaire_gender_distribution_bts_kpi_gap():
    expected, details = _physical_activity_expected_and_details()
    details = dict(details)
    details["exceptions"] = dict(details["exceptions"])
    details["exceptions"]["answered_without_finishing_questionnaire"] = [
        {
            "user_id": 3,
            "name": "Pat Ng",
            "answer_shown": "Rarely or never",
            "reason": "Answered without finishing questionnaire.",
        }
    ]
    details["method"] = dict(details["method"])
    details["method"]["answered_this_question"] = 3
    bts = build_questionnaire_gender_distribution_bts(
        expected_data=expected,
        stored_data=expected,
        details=details,
        checked_at="t",
        section_title="Physical activity",
        bucket_labels=PHYSICAL_ACTIVITY_BUCKET_LABELS,
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["answered_vs_questionnaire_completed"]["match"] is False
    assert "without finishing" in bts["fields"]["answered_vs_questionnaire_completed"]["reason"]


def test_build_questionnaire_gender_distribution_bts_unknown_answer():
    roster = [(1, "Alex", "Lee", "male", "4")]
    payload, details = build_questionnaire_gender_distribution_details(
        roster,
        filled_user_ids=set(),
        questionnaire_completed=0,
        buckets=PHYSICAL_ACTIVITY_BUCKETS,
        answer_to_bucket=physical_activity_answer_to_bucket,
        bucket_labels=PHYSICAL_ACTIVITY_BUCKET_LABELS,
        scope_label="Whole camp",
        question_label="daily physical activity",
    )
    bts = build_questionnaire_gender_distribution_bts(
        expected_data=payload["data"],
        stored_data=payload["data"],
        details=details,
        checked_at="t",
        section_title="Physical activity",
        bucket_labels=PHYSICAL_ACTIVITY_BUCKET_LABELS,
    )
    assert bts["status"] == "mismatch"
    assert bts["fields"]["unknown_answers"]["match"] is False
    assert bts["fields"]["unknown_answers"]["stored"] == 1
