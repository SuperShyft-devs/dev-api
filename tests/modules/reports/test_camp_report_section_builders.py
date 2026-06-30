"""Unit tests for camp report section builders."""

from __future__ import annotations

from datetime import date

from modules.reports.camp_report_section_builders import (
    build_company_average_scores,
    build_distribution_by_gender_by_metabolic_syndrome,
    build_distribution_by_oxidative_stress,
    build_distribution_by_physical_activity_frequency,
    build_kpis,
    build_overall_risk_score,
    build_participation_by_age,
    extract_disease_risk_scores,
    extract_metabolic_age,
    extract_metabolic_score,
    extract_oxidative_stress_score,
    is_high_metabolic_risk,
    match_dashboard_disease_code,
    metabolic_score_to_band,
    normalize_camp_gender,
    oxidative_stress_to_band,
    physical_activity_answer_to_bucket,
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


def test_build_kpis_percent():
    payload = build_kpis(
        {
            "employees_enrolled": 4,
            "male_enrolled": 2,
            "female_enrolled": 2,
            "total_blood_test": 3,
            "doctor_consultation": 2,
            "high_risk_group": 1,
        }
    )
    assert payload["data"]["blood_test_percent"] == 75


def test_build_kpis_percent_zero_enrolled():
    payload = build_kpis(
        {
            "employees_enrolled": 0,
            "male_enrolled": 0,
            "female_enrolled": 0,
            "total_blood_test": 0,
            "doctor_consultation": 0,
            "high_risk_group": 0,
        }
    )
    assert payload["data"]["blood_test_percent"] == 0


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
    assert data["total_employees"] == 4
    assert data["elevated_metabolic_score"] == 50.0


def test_build_overall_risk_score_empty():
    payload = build_overall_risk_score([])
    data = payload["data"]
    assert data["count"] == [0, 0, 0, 0]
    assert data["percent"] == [0.0, 0.0, 0.0, 0.0]
    assert data["total_employees"] == 0
    assert data["elevated_metabolic_score"] == 0.0


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
    assert female["count"] == [0, 0, 1, 1]
    assert female["percent"] == [0.0, 0.0, 50.0, 50.0]


def test_build_distribution_by_physical_activity_frequency_empty():
    payload = build_distribution_by_physical_activity_frequency([])
    for gender in ("male", "female"):
        data = payload["data"][gender]
        assert data["count"] == [0, 0, 0, 0]
        assert data["percent"] == [0.0, 0.0, 0.0, 0.0]


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
