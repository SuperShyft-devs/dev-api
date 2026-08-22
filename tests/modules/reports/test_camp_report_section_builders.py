"""Unit tests for camp report section builders."""

from __future__ import annotations

from datetime import date

from modules.reports.camp_report_section_builders import (
    build_band_percent_math,
    build_category_average_math,
    build_company_average_scores,
    build_company_average_scores_details,
    build_distribution_by_gender_by_metabolic_syndrome,
    build_distribution_by_gender_by_metabolic_syndrome_details,
    build_distribution_by_oxidative_stress,
    build_distribution_by_oxidative_stress_details,
    build_distribution_by_physical_activity_frequency,
    build_distribution_by_sleeping_hours,
    build_questionnaire_gender_distribution_details,
    build_elevated_disease_risk_math,
    build_elevated_metabolic_math,
    build_elevated_oxidative_math,
    build_kpis,
    build_overall_risk_score,
    build_overall_risk_score_details,
    build_participation_by_age,
    extract_disease_risk_scores,
    extract_metabolic_age,
    extract_metabolic_score,
    extract_oxidative_stress_score,
    is_high_metabolic_risk,
    match_dashboard_disease_code,
    metabolic_risk_bucket,
    metabolic_score_to_band,
    normalize_camp_gender,
    oxidative_stress_to_band,
    PHYSICAL_ACTIVITY_BUCKETS,
    SLEEPING_HOURS_BUCKETS,
    physical_activity_answer_to_bucket,
    sleeping_hours_answer_to_bucket,
    risk_score_scaled_to_band,
)


def test_extract_metabolic_age_top_level():
    assert extract_metabolic_age({"metabolic_age": 42.5}) == 42.5


def test_extract_metabolic_age_nested_data():
    assert extract_metabolic_age({"data": {"metabolic_age": 38.0}}) == 38.0


def test_extract_metabolic_age_missing():
    assert extract_metabolic_age({}) is None


def test_is_high_metabolic_risk_boundary():
    assert is_high_metabolic_risk(metabolic_age=33.0, chronological_age=30) is True
    assert is_high_metabolic_risk(metabolic_age=32.9, chronological_age=30) is False
    assert is_high_metabolic_risk(metabolic_age=None, chronological_age=30) is False


def test_metabolic_risk_bucket_boundaries():
    assert metabolic_risk_bucket(metabolic_age=33.0, chronological_age=30) == "high"
    assert metabolic_risk_bucket(metabolic_age=32.9, chronological_age=30) == "caution"
    assert metabolic_risk_bucket(metabolic_age=30.1, chronological_age=30) == "caution"
    assert metabolic_risk_bucket(metabolic_age=30.0, chronological_age=30) == "good"
    assert metabolic_risk_bucket(metabolic_age=29.0, chronological_age=30) == "good"
    assert metabolic_risk_bucket(metabolic_age=None, chronological_age=30) == "good"


def test_build_kpis_percent():
    payload = build_kpis(
        {
            "employees_enrolled": 4,
            "male_enrolled": 2,
            "female_enrolled": 2,
            "total_blood_test": 3,
            "consultations": {
                "doctor": 2,
                "nutritionist": 1,
                "doctor_nutritionist": 1,
            },
            "doctor_consultation": 2,
            "nutritionist_consultation": 1,
            "doctor_and_nutritionist_consultation": 1,
            "questionnaire_completed": 3,
            "bio_ai_report_generated": 3,
            "high_risk_group": 1,
            "caution_risk_group": 1,
            "good_risk_group": 1,
        }
    )
    assert payload["data"]["blood_test_percent"] == 75
    assert payload["data"]["consultations"]["doctor"] == 2
    assert payload["data"]["consultations"]["doctor_nutritionist"] == 1
    assert payload["data"]["questionnaire_completed"] == 3
    assert payload["data"]["bio_ai_report_generated"] == 3
    assert payload["data"]["caution_risk_group"] == 1
    assert payload["data"]["good_risk_group"] == 1


def test_build_kpis_percent_zero_enrolled():
    payload = build_kpis(
        {
            "employees_enrolled": 0,
            "male_enrolled": 0,
            "female_enrolled": 0,
            "total_blood_test": 0,
            "consultations": {},
            "doctor_consultation": 0,
            "nutritionist_consultation": 0,
            "doctor_and_nutritionist_consultation": 0,
            "questionnaire_completed": 0,
            "bio_ai_report_generated": 0,
            "high_risk_group": 0,
            "caution_risk_group": 0,
            "good_risk_group": 0,
        }
    )
    assert payload["data"]["blood_test_percent"] == 0
    assert payload["data"]["consultations"] == {}
    assert payload["data"]["bio_ai_report_generated"] == 0


def test_build_participation_by_age_total_inside_data():
    payload = build_participation_by_age(
        [(1, date(2000, 1, 1), 25)],
        reference_date=date(2026, 6, 23),
    )
    assert "total_enrolled" not in payload
    assert payload["data"]["total_enrolled"] == 1


def test_extract_metabolic_score_top_level():
    assert extract_metabolic_score({"metabolic_score": 20.0}) == 20.0


def test_extract_metabolic_score_nested_data():
    assert extract_metabolic_score({"data": {"metabolic_score": 35.5}}) == 35.5


def test_extract_metabolic_score_missing():
    assert extract_metabolic_score({}) is None


def test_metabolic_score_to_band_boundaries():
    assert metabolic_score_to_band(25) == "optimal"
    assert metabolic_score_to_band(26) == "low_risk"
    assert metabolic_score_to_band(42) == "low_risk"
    assert metabolic_score_to_band(43) == "increased_risk"
    assert metabolic_score_to_band(58) == "increased_risk"
    assert metabolic_score_to_band(59) == "high_risk"


def test_build_overall_risk_score():
    payload = build_overall_risk_score([20.0, 30.0, 50.0, 70.0])
    data = payload["data"]
    assert data["group"] == ["optimal", "low_risk", "increased_risk", "high_risk"]
    assert data["count"] == [1, 1, 1, 1]
    assert data["percent"] == [25.0, 25.0, 25.0, 25.0]
    assert data["elevated_metabolic_score"] == 50.0
    assert "total_employees" not in data
    assert "total_with_metabolic_score" not in data
    assert "total_enrolled" not in data
    assert "bio_ai_reports" not in data
    assert "missing_metabolic_score" not in data


def test_build_overall_risk_score_empty():
    payload = build_overall_risk_score([])
    data = payload["data"]
    assert data["count"] == [0, 0, 0, 0]
    assert data["percent"] == [0.0, 0.0, 0.0, 0.0]
    assert data["elevated_metabolic_score"] == 0.0
    assert set(data.keys()) == {
        "group",
        "count",
        "percent",
        "elevated_metabolic_score",
    }


def test_build_elevated_metabolic_math_example():
    math = build_elevated_metabolic_math(
        increased_risk_count=16,
        high_risk_count=7,
        total_with_score=81,
    )
    assert math["elevated_count"] == 23
    assert math["result_percent"] == 28.4
    assert math["steps"][0].startswith("Step 1:")
    assert "28.4%" in math["steps"][-1]


def test_build_elevated_metabolic_math_zero_total():
    math = build_elevated_metabolic_math(
        increased_risk_count=0,
        high_risk_count=0,
        total_with_score=0,
    )
    assert math["result_percent"] == 0.0
    assert any("cannot calculate" in step.lower() for step in math["steps"])


def test_build_overall_risk_score_details_people_and_excluded():
    rows = [
        (1, "Ann", "Optimal", "female", 20.0, None),
        (2, "Bob", "Low", "male", 30.0, None),
        (3, "Cara", "Inc", "female", 50.0, None),
        (4, "Dan", "High", "male", 70.0, None),
        (
            5,
            "Eve",
            "Missing",
            "female",
            None,
            "Bio AI generated but metabolic_score field is missing from reports JSON",
        ),
    ]
    payload, details = build_overall_risk_score_details(
        rows,
        total_enrolled=5,
        bio_ai_reports=5,
        scope_label="Whole camp",
    )
    data = payload["data"]
    assert data["count"] == [1, 1, 1, 1]
    assert data["elevated_metabolic_score"] == 50.0
    assert "total_employees" not in data
    assert details["method"]["total_enrolled"] == 5
    assert details["method"]["bio_ai_reports"] == 5
    assert details["method"]["with_metabolic_score"] == 4
    assert details["method"]["missing_metabolic_score"] == 1
    assert details["bands"]["optimal"]["count"] == 1
    assert details["bands"]["optimal"]["people"][0]["name"] == "Ann Optimal"
    assert details["bands"]["optimal"]["people"][0]["metabolic_score"] == 20.0
    assert details["excluded"]["count"] == 1
    assert details["excluded"]["people"][0]["user_id"] == 5
    assert "metabolic score is missing" in details["excluded"]["people"][0]["reason"].lower()
    assert details["elevated_math"]["result_percent"] == 50.0
    assert len(details["elevated_math"]["steps"]) >= 5


def test_extract_oxidative_stress_score_top_level():
    reports = {
        "diseases": [
            {"code": "diabetes", "risk_score_scaled": 10},
            {"code": "oxidative_stress", "risk_score_scaled": 40},
        ]
    }
    assert extract_oxidative_stress_score(reports) == 40.0


def test_extract_oxidative_stress_score_nested_data():
    reports = {
        "data": {
            "diseases": [
                {"code": "oxidative_stress", "risk_score_scaled": 35.5},
            ]
        }
    }
    assert extract_oxidative_stress_score(reports) == 35.5


def test_extract_oxidative_stress_score_missing():
    assert extract_oxidative_stress_score({}) is None
    assert extract_oxidative_stress_score({"diseases": [{"code": "diabetes"}]}) is None
    assert extract_oxidative_stress_score(
        {"diseases": [{"code": "oxidative_stress", "risk_score_scaled": "high"}]}
    ) is None


def test_oxidative_stress_to_band_boundaries():
    assert oxidative_stress_to_band(25) == "low"
    assert oxidative_stress_to_band(26) == "moderate"
    assert oxidative_stress_to_band(42) == "moderate"
    assert oxidative_stress_to_band(43) == "high"
    assert oxidative_stress_to_band(58) == "high"
    assert oxidative_stress_to_band(59) == "very_high"


def test_build_distribution_by_oxidative_stress():
    payload = build_distribution_by_oxidative_stress([20.0, 35.0, 50.0, 65.0])
    data = payload["data"]
    assert data["group"] == ["low", "moderate", "high", "very_high"]
    assert data["count"] == [1, 1, 1, 1]
    assert data["percent"] == [25.0, 25.0, 25.0, 25.0]
    assert data["total_employees"] == 4
    assert data["elevated_oxidative_stress_percent"] == 50.0


def test_build_distribution_by_oxidative_stress_empty():
    payload = build_distribution_by_oxidative_stress([])
    data = payload["data"]
    assert data["count"] == [0, 0, 0, 0]
    assert data["percent"] == [0.0, 0.0, 0.0, 0.0]
    assert data["total_employees"] == 0
    assert data["elevated_oxidative_stress_percent"] == 0.0


def test_build_elevated_oxidative_math_sample():
    math = build_elevated_oxidative_math(
        high_count=11,
        very_high_count=7,
        total_with_score=81,
    )
    assert math["result_percent"] == 22.2
    assert math["kind"] == "oxidative_stress"
    assert "Step 1: Count people in High = 11" in math["steps"]
    assert "Step 7: Round to 1 decimal place: 22.2%" in math["steps"]


def test_build_elevated_oxidative_math_empty():
    math = build_elevated_oxidative_math(
        high_count=0,
        very_high_count=0,
        total_with_score=0,
    )
    assert math["result_percent"] == 0.0
    assert any("cannot calculate" in step.lower() for step in math["steps"])


def test_build_distribution_by_oxidative_stress_details_people_and_excluded():
    rows = [
        (1, "Ann", "Low", "female", 20.0, None),
        (2, "Bob", "Moderate", "male", 35.0, None),
        (3, "Cara", "High", "female", 50.0, None),
        (4, "Dan", "VeryHigh", "male", 65.0, None),
        (
            5,
            "Eve",
            "Missing",
            "female",
            None,
            "Bio AI generated but oxidative_stress risk_score_scaled is missing from reports JSON",
        ),
    ]
    payload, details = build_distribution_by_oxidative_stress_details(
        rows,
        total_enrolled=5,
        bio_ai_reports=5,
        scope_label="Whole camp",
    )
    data = payload["data"]
    assert data["count"] == [1, 1, 1, 1]
    assert data["total_employees"] == 4
    assert data["elevated_oxidative_stress_percent"] == 50.0
    assert details["method"]["with_oxidative_stress_score"] == 4
    assert details["method"]["missing_oxidative_stress_score"] == 1
    assert details["bands"]["low"]["people"][0]["oxidative_stress_score"] == 20.0
    assert details["excluded"]["count"] == 1
    assert "oxidative stress score is missing" in details["excluded"]["people"][0]["reason"].lower()
    assert details["elevated_math"]["result_percent"] == 50.0


def test_normalize_camp_gender():
    assert normalize_camp_gender("Male") == "male"
    assert normalize_camp_gender("f") == "female"
    assert normalize_camp_gender("1") == "male"
    assert normalize_camp_gender("2") == "female"
    assert normalize_camp_gender("other") is None
    assert normalize_camp_gender(None) is None


def test_physical_activity_answer_to_bucket():
    assert physical_activity_answer_to_bucket("1") == "less_than_30mins"
    assert physical_activity_answer_to_bucket("2") == "30_60_mins"
    assert physical_activity_answer_to_bucket("3") == "more_than_60_mins"
    assert physical_activity_answer_to_bucket("5") == "rarely_or_never"
    assert physical_activity_answer_to_bucket("4") is None
    assert physical_activity_answer_to_bucket(None) is None


def test_sleeping_hours_answer_to_bucket():
    assert sleeping_hours_answer_to_bucket("0") == "less_than_5hrs"
    assert sleeping_hours_answer_to_bucket("1") == "between_5_7_hrs"
    assert sleeping_hours_answer_to_bucket("2") == "between_7_9_hrs"
    assert sleeping_hours_answer_to_bucket("3") == "more_than_9hrs"
    assert sleeping_hours_answer_to_bucket("4") is None
    assert sleeping_hours_answer_to_bucket(None) is None


def test_build_distribution_by_physical_activity_frequency():
    rows = [
        ("male", "1"),
        ("Male", "2"),
        ("female", "3"),
        ("F", "5"),
        ("other", "1"),
        ("male", "4"),
        (None, "1"),
    ]
    payload = build_distribution_by_physical_activity_frequency(rows)
    male = payload["data"]["male"]
    female = payload["data"]["female"]
    assert male["group"] == [
        "less_than_30mins",
        "30_60_mins",
        "more_than_60_mins",
        "rarely_or_never",
    ]
    assert male["count"] == [1, 1, 0, 0]
    assert male["percent"] == [50.0, 50.0, 0.0, 0.0]
    assert male["total_responded"] == 2
    assert "unmapped_responded" not in male
    assert female["count"] == [0, 0, 1, 1]
    assert female["percent"] == [0.0, 0.0, 50.0, 50.0]


def test_build_distribution_by_sleeping_hours():
    rows = [
        ("male", "0"),
        ("male", "1"),
        ("female", "2"),
        ("female", "3"),
        ("male", "9"),
    ]
    payload = build_distribution_by_sleeping_hours(rows)
    male = payload["data"]["male"]
    female = payload["data"]["female"]
    assert male["group"] == list(SLEEPING_HOURS_BUCKETS)
    assert male["count"] == [1, 1, 0, 0]
    assert male["total_responded"] == 2
    assert female["count"] == [0, 0, 1, 1]
    assert female["total_responded"] == 2


def test_build_distribution_by_physical_activity_frequency_empty():
    payload = build_distribution_by_physical_activity_frequency([])
    for gender in ("male", "female"):
        data = payload["data"][gender]
        assert data["count"] == [0, 0, 0, 0]
        assert data["percent"] == [0.0, 0.0, 0.0, 0.0]


def test_build_questionnaire_gender_distribution_details():
    roster = [
        (1, "Alex", "Lee", "male", "1"),
        (2, "Sam", "Kim", "female", "4"),
        (3, "Pat", "Ng", "other", "2"),
        (4, "Jamie", "Fox", "female", None),
    ]
    payload, details = build_questionnaire_gender_distribution_details(
        roster,
        filled_user_ids={4},
        questionnaire_completed=1,
        buckets=PHYSICAL_ACTIVITY_BUCKETS,
        answer_to_bucket=physical_activity_answer_to_bucket,
        bucket_labels={
            "less_than_30mins": "Less than 30 minutes a day",
            "30_60_mins": "30–60 minutes a day",
            "more_than_60_mins": "More than 60 minutes a day",
            "rarely_or_never": "Rarely or never",
        },
        scope_label="Whole camp",
        question_label="daily physical activity",
    )
    assert payload["data"]["male"]["count"] == [1, 0, 0, 0]
    assert payload["data"]["male"]["total_responded"] == 1
    assert len(details["exceptions"]["answer_not_a_known_choice"]) == 1
    assert len(details["exceptions"]["gender_not_male_or_female"]) == 1
    assert len(details["exceptions"]["answered_without_finishing_questionnaire"]) == 3
    assert len(details["exceptions"]["finished_questionnaire_without_this_answer"]) == 1


def test_risk_score_scaled_to_band_boundaries():
    assert risk_score_scaled_to_band(25) == "healthy"
    assert risk_score_scaled_to_band(26) == "increased"
    assert risk_score_scaled_to_band(42) == "increased"
    assert risk_score_scaled_to_band(43) == "high"
    assert risk_score_scaled_to_band(58) == "high"
    assert risk_score_scaled_to_band(59) == "very_high"


def test_match_dashboard_disease_code_aliases():
    assert match_dashboard_disease_code("diabetes") == "type_2_diabetes"
    assert match_dashboard_disease_code("type_2_diabetes") == "type_2_diabetes"
    assert match_dashboard_disease_code("pcos/pcod") == "pcos_pcod"
    assert match_dashboard_disease_code("pcos") == "pcos_pcod"
    assert match_dashboard_disease_code("hypertension") == "hypertension"
    assert match_dashboard_disease_code("oxidative_stress") is None
    assert match_dashboard_disease_code("unknown") is None


def test_extract_disease_risk_scores():
    reports = {
        "data": {
            "diseases": [
                {"code": "diabetes", "risk_score_scaled": 15},
                {"code": "hypertension", "risk_score_scaled": 40},
                {"code": "oxidative_stress", "risk_score_scaled": 67},
                {"code": "nafld", "risk_score_scaled": "invalid"},
            ]
        }
    }
    scores = extract_disease_risk_scores(reports)
    assert scores == {"type_2_diabetes": 15.0, "hypertension": 40.0}


def test_build_distribution_by_gender_by_metabolic_syndrome():
    rows = [
        ("male", {"diseases": [{"code": "hypertension", "risk_score_scaled": 20}]}),
        ("male", {"diseases": [{"code": "hypertension", "risk_score_scaled": 50}]}),
        ("female", {"diseases": [{"code": "hypertension", "risk_score_scaled": 35}]}),
        ("female", {"diseases": [{"code": "diabetes", "risk_score_scaled": 10}]}),
        ("other", {"diseases": [{"code": "hypertension", "risk_score_scaled": 60}]}),
        ("male", {"diseases": [{"code": "oxidative_stress", "risk_score_scaled": 70}]}),
    ]
    payload = build_distribution_by_gender_by_metabolic_syndrome(rows)
    diseases = payload["data"]["diseases"]
    codes = [d["code"] for d in diseases]
    assert codes == ["type_2_diabetes", "hypertension"]

    hypertension = next(d for d in diseases if d["code"] == "hypertension")
    assert hypertension["male"]["count"] == [1, 0, 1, 0]
    assert hypertension["male"]["percent"] == [50.0, 0.0, 50.0, 0.0]
    assert hypertension["male"]["elevated_percent"] == 50.0
    assert hypertension["female"]["count"] == [0, 1, 0, 0]
    assert hypertension["female"]["elevated_percent"] == 0.0

    diabetes = next(d for d in diseases if d["code"] == "type_2_diabetes")
    assert diabetes["male"]["count"] == [0, 0, 0, 0]
    assert diabetes["female"]["count"] == [1, 0, 0, 0]


def test_build_distribution_by_gender_by_metabolic_syndrome_empty():
    payload = build_distribution_by_gender_by_metabolic_syndrome([])
    assert payload["data"]["diseases"] == []


def test_build_band_percent_math():
    math = build_band_percent_math(band_label="High", count=5, total=36)
    assert math["result_percent"] == 13.9
    assert any("5 ÷ 36" in step for step in math["steps"])


def test_build_elevated_disease_risk_math_sum_of_percents():
    math = build_elevated_disease_risk_math(
        high_count=5,
        very_high_count=7,
        high_percent=13.9,
        very_high_percent=19.4,
        total=36,
    )
    assert math["result_percent"] == 33.3
    assert math["alternate_from_counts"] == 33.3
    assert math["kind"] == "disease_risk_by_gender"

    edge = build_elevated_disease_risk_math(
        high_count=8,
        very_high_count=7,
        high_percent=22.2,
        very_high_percent=19.4,
        total=36,
    )
    assert edge["result_percent"] == 41.6
    assert edge["alternate_from_counts"] == 41.7


def test_build_distribution_by_gender_by_metabolic_syndrome_details():
    status_rows = [
        (
            1,
            "John",
            "Doe",
            "male",
            {
                "diseases": [
                    {"code": "hypertension", "risk_score_scaled": 20},
                    {"code": "diabetes", "risk_score_scaled": 10},
                ],
            },
            None,
        ),
        (
            2,
            "Jane",
            "Smith",
            "female",
            {"diseases": [{"code": "hypertension", "risk_score_scaled": 50}]},
            None,
        ),
        (
            3,
            "Other",
            "Person",
            "other",
            {"diseases": [{"code": "hypertension", "risk_score_scaled": 60}]},
            None,
        ),
        (
            4,
            "No",
            "Report",
            "male",
            None,
            "No Metsights Basic/Pro assessment instance for this camp",
        ),
    ]
    payload, details = build_distribution_by_gender_by_metabolic_syndrome_details(
        status_rows,
        total_enrolled=4,
        bio_ai_reports=3,
        scope_label="Whole camp",
    )
    diseases = payload["data"]["diseases"]
    codes = [d["code"] for d in diseases]
    assert codes == ["type_2_diabetes", "hypertension"]

    hypertension = details["diseases"]["hypertension"]
    assert hypertension["male"]["groups"]["healthy"]["count"] == 1
    assert hypertension["male"]["groups"]["healthy"]["people"][0]["name"] == "John Doe"
    assert hypertension["female"]["groups"]["increased"]["count"] == 1
    assert hypertension["female"]["elevated_math"]["result_percent"] == 0.0

    diabetes = details["diseases"]["type_2_diabetes"]
    assert diabetes["male"]["groups"]["healthy"]["count"] == 1
    assert diabetes["male"]["groups"]["healthy"]["people"][0]["report_code"] == "diabetes"
    assert len(diabetes["not_counted"]) == 1

    assert details["excluded"]["count"] == 1
    assert details["unknown_gender"]["count"] == 1
    assert details["method"]["with_bio_ai_report"] == 3


def test_build_company_average_scores_basic():
    scores = [
        {"nutrition": 60.0, "fitness": 50.0, "lifestyle": 70.0},
        {"nutrition": 80.0, "fitness": 70.0, "lifestyle": 60.0},
    ]
    payload = build_company_average_scores(scores)
    assert payload == {
        "data": {
            "nutrition": {"score": 70},
            "fitness": {"score": 60},
            "lifestyle": {"score": 65},
        },
    }


def test_build_company_average_scores_with_none_values():
    scores = [
        {"nutrition": 64.0, "fitness": None, "lifestyle": 63.0},
        {"nutrition": None, "fitness": 58.0, "lifestyle": 63.0},
        {"nutrition": 64.0, "fitness": 58.0, "lifestyle": None},
    ]
    payload = build_company_average_scores(scores)
    assert payload["data"]["nutrition"]["score"] == 64
    assert payload["data"]["fitness"]["score"] == 58
    assert payload["data"]["lifestyle"]["score"] == 63


def test_build_company_average_scores_empty():
    payload = build_company_average_scores([])
    assert payload == {
        "data": {
            "nutrition": {"score": 0},
            "fitness": {"score": 0},
            "lifestyle": {"score": 0},
        },
    }


def test_build_company_average_scores_all_none():
    scores = [
        {"nutrition": None, "fitness": None, "lifestyle": None},
        {"nutrition": None, "fitness": None, "lifestyle": None},
    ]
    payload = build_company_average_scores(scores)
    assert payload["data"]["nutrition"]["score"] == 0
    assert payload["data"]["fitness"]["score"] == 0
    assert payload["data"]["lifestyle"]["score"] == 0


def test_build_company_average_scores_rounds():
    scores = [
        {"nutrition": 10.0, "fitness": 10.0, "lifestyle": 10.0},
        {"nutrition": 11.0, "fitness": 11.0, "lifestyle": 11.0},
        {"nutrition": 12.0, "fitness": 12.0, "lifestyle": 12.0},
    ]
    payload = build_company_average_scores(scores)
    assert payload["data"]["nutrition"]["score"] == 11
    assert payload["data"]["fitness"]["score"] == 11
    assert payload["data"]["lifestyle"]["score"] == 11


def test_build_category_average_math_basic():
    math = build_category_average_math(
        category_key="nutrition",
        scores_used=[
            {"user_id": 1, "name": "Alice", "score": 60.0},
            {"user_id": 2, "name": "Bob", "score": 70.0},
            {"user_id": 3, "name": "Carol", "score": 65.0},
        ],
        has_fitprint_participants=True,
    )
    assert math["rounded_score"] == 65
    assert math["count"] == 3
    assert math["sum"] == 195.0
    assert any("195" in step for step in math["steps"])
    assert any("65" in step for step in math["steps"])


def test_build_category_average_math_rounds_half_up():
    math = build_category_average_math(
        category_key="fitness",
        scores_used=[
            {"user_id": 1, "name": "Alice", "score": 64.0},
            {"user_id": 2, "name": "Bob", "score": 65.0},
        ],
        has_fitprint_participants=True,
    )
    assert math["average_exact"] == 64.5
    assert math["rounded_score"] == round(64.5)
    assert any("64.5" in step for step in math["steps"])


def test_build_category_average_math_no_scores():
    math = build_category_average_math(
        category_key="lifestyle",
        scores_used=[],
        has_fitprint_participants=True,
    )
    assert math["rounded_score"] == 0
    assert any("Nobody had a Lifestyle score" in step for step in math["steps"])


def test_build_company_average_scores_details_basic():
    participant_rows = [
        {
            "user_id": 1,
            "name": "Alice",
            "assessment_instance_id": 101,
            "nutrition": {"score": 64.0, "status": "included", "steps": ["Nutrition 64"]},
            "fitness": {"score": 55.0, "status": "included", "steps": ["Fitness 55"]},
            "lifestyle": {"score": 65.0, "status": "included", "steps": ["Lifestyle 65"]},
        },
        {
            "user_id": 2,
            "name": "Bob",
            "assessment_instance_id": 102,
            "nutrition": {"score": 70.0, "status": "included", "steps": ["Nutrition 70"]},
            "fitness": {"score": 55.0, "status": "included", "steps": ["Fitness 55"]},
            "lifestyle": {"score": 65.0, "status": "included", "steps": ["Lifestyle 65"]},
        },
    ]
    payload, details = build_company_average_scores_details(
        participant_rows,
        scope_label="Whole camp",
        total_enrolled=3,
        excluded_no_fitprint=[{"user_id": 3, "name": "Carol", "reason": "No FitPrint"}],
        excluded_report_load_failed=[],
    )
    assert payload["data"]["nutrition"]["score"] == 67
    assert payload["data"]["fitness"]["score"] == 55
    assert payload["data"]["lifestyle"]["score"] == 65
    assert details["method"]["section_kind"] == "company_average_scores"
    assert details["summary"]["total_enrolled"] == 3
    assert details["summary"]["with_fitprint"] == 2
    assert details["summary"]["without_fitprint"] == 1
    assert len(details["participants"]) == 2
    assert details["aggregation"]["nutrition"]["rounded_score"] == 67


def test_build_company_average_scores_details_partial_none():
    participant_rows = [
        {
            "user_id": 1,
            "name": "Alice",
            "assessment_instance_id": 101,
            "nutrition": {"score": 64.0, "status": "included", "steps": []},
            "fitness": {"score": None, "status": "missing", "steps": ["No fitness"]},
            "lifestyle": {"score": 63.0, "status": "included", "steps": []},
        },
        {
            "user_id": 2,
            "name": "Bob",
            "assessment_instance_id": 102,
            "nutrition": {"score": None, "status": "missing", "steps": ["No nutrition"]},
            "fitness": {"score": 58.0, "status": "included", "steps": []},
            "lifestyle": {"score": 63.0, "status": "included", "steps": []},
        },
    ]
    payload, details = build_company_average_scores_details(
        participant_rows,
        scope_label="Department: sales",
        total_enrolled=2,
        excluded_no_fitprint=[],
        excluded_report_load_failed=[],
    )
    assert payload["data"]["nutrition"]["score"] == 64
    assert payload["data"]["fitness"]["score"] == 58
    assert payload["data"]["lifestyle"]["score"] == 63
    assert details["aggregation"]["nutrition"]["count"] == 1
    assert details["aggregation"]["fitness"]["count"] == 1


def test_build_company_average_scores_details_empty():
    payload, details = build_company_average_scores_details(
        [],
        scope_label="Whole camp",
        total_enrolled=0,
        excluded_no_fitprint=[],
        excluded_report_load_failed=[],
    )
    assert payload["data"]["nutrition"]["score"] == 0
    assert payload["data"]["fitness"]["score"] == 0
    assert payload["data"]["lifestyle"]["score"] == 0
    assert details["summary"]["with_fitprint"] == 0
