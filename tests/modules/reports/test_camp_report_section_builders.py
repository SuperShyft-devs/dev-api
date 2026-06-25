"""Unit tests for camp report section builders."""

from __future__ import annotations

from datetime import date

from modules.reports.camp_report_section_builders import (
    build_kpis,
    build_overall_risk_score,
    build_participation_by_age,
    extract_metabolic_age,
    extract_metabolic_score,
    is_high_metabolic_risk,
    metabolic_score_to_band,
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
    assert data["avg_metabolic_score"] == 42.5


def test_build_overall_risk_score_empty():
    payload = build_overall_risk_score([])
    data = payload["data"]
    assert data["count"] == [0, 0, 0, 0]
    assert data["percent"] == [0.0, 0.0, 0.0, 0.0]
    assert data["total_employees"] == 0
    assert data["avg_metabolic_score"] == 0.0
