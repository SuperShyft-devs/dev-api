"""Unit tests for positive_wins camp report aggregation helpers."""

from __future__ import annotations

from modules.reports.camp_report_section_builders import (
    aggregate_top_healthy_habits,
    aggregate_top_healthy_profiles,
    aggregate_top_low_risk,
    build_chart_risk_score_scaled_math,
    build_per_person_low_risk_math,
    build_positive_wins,
    build_positive_wins_details,
    build_risk_score_scaled_read_math,
    build_top_n_frequency_math,
)


def test_aggregate_top_healthy_habits_frequency_and_tie_break():
    participant_habits = [
        [{"habit_key": "no_alcohol", "habit_label": "No Alcohol"}],
        [{"habit_key": "no_alcohol", "habit_label": "No Alcohol"}],
        [{"habit_key": "walk", "habit_label": "Daily Walk"}],
        [{"habit_key": "sleep", "habit_label": "Good Sleep"}],
    ]
    result = aggregate_top_healthy_habits(participant_habits, limit=3)
    assert result == [
        {"habit_key": "no_alcohol", "habit_label": "No Alcohol"},
        {"habit_key": "walk", "habit_label": "Daily Walk"},
        {"habit_key": "sleep", "habit_label": "Good Sleep"},
    ]


def test_aggregate_top_healthy_habits_label_tie_break_alphabetical():
    participant_habits = [
        [{"habit_key": "z", "habit_label": "Zebra"}],
        [{"habit_key": "a", "habit_label": "Alpha"}],
    ]
    result = aggregate_top_healthy_habits(participant_habits, limit=3)
    assert result[0]["habit_label"] == "Alpha"
    assert result[1]["habit_label"] == "Zebra"


def test_aggregate_top_healthy_profiles_frequency_and_limit():
    participant_profiles = [
        ["Beta", "Alpha"],
        ["Beta"],
        ["Gamma"],
        ["Alpha"],
    ]
    assert aggregate_top_healthy_profiles(participant_profiles, limit=3) == [
        "Alpha",
        "Beta",
        "Gamma",
    ]


def test_aggregate_top_low_risk_frequency_and_tie_break():
    participant_low_risk = [
        [
            {"code": "low_a", "name": "Low A", "risk_status": "Healthy", "risk_score_scaled": 12},
            {"code": "low_b", "name": "Low B", "risk_status": "Healthy", "risk_score_scaled": 15},
        ],
        [
            {"code": "low_a", "name": "Low A", "risk_status": "Healthy", "risk_score_scaled": 12},
        ],
        [
            {"code": "low_c", "name": "Low C", "risk_status": "Healthy", "risk_score_scaled": 10},
        ],
    ]
    result = aggregate_top_low_risk(participant_low_risk, limit=3)
    assert [item["code"] for item in result] == ["low_a", "low_b", "low_c"]
    assert result[0]["risk_status"] == "Healthy"


def test_build_positive_wins_shape():
    payload = build_positive_wins(
        low_risk=[{"code": "a", "name": "A", "risk_status": "Healthy", "risk_score_scaled": 10}],
        healthy_habits=[{"habit_key": "k", "habit_label": "Label"}],
        healthy_profiles=["Group A"],
    )
    assert payload == {
        "data": {
            "low_risk": [{"code": "a", "name": "A", "risk_status": "Healthy", "risk_score_scaled": 10}],
            "healthy_habits": [{"habit_key": "k", "habit_label": "Label"}],
            "healthy_profiles": ["Group A"],
        },
    }


def test_build_top_n_frequency_math_tie_break():
    math = build_top_n_frequency_math(
        category_label="healthy habit",
        counts={"Alpha": 2, "Beta": 2, "Gamma": 1},
        labels={"Alpha": "Alpha", "Beta": "Beta", "Gamma": "Gamma"},
        people_by_key={
            "Alpha": [{"user_id": 1, "name": "A"}],
            "Beta": [{"user_id": 2, "name": "B"}],
            "Gamma": [{"user_id": 3, "name": "C"}],
        },
        limit=2,
    )
    assert math["selected_keys"] == ["Alpha", "Beta"]
    assert any("alphabetically" in step.lower() for step in math["steps"])


def test_build_per_person_low_risk_math():
    all_healthy = [
        {"code": "a", "name": "A", "risk_score_scaled": 5},
        {"code": "b", "name": "B", "risk_score_scaled": 10},
        {"code": "c", "name": "C", "risk_score_scaled": 15},
        {"code": "d", "name": "D", "risk_score_scaled": 20},
    ]
    selected = all_healthy[:3]
    math = build_per_person_low_risk_math(all_healthy, selected)
    assert math["healthy_found"] == 4
    assert math["selected_count"] == 3
    assert len(math["steps"]) >= 3
    assert "a" in math["by_disease"]
    assert math["by_disease"]["a"]["result"] == 5
    assert any("risk_score_scaled = 5" in step for step in math["by_disease"]["a"]["steps"])


def test_build_risk_score_scaled_read_math():
    math = build_risk_score_scaled_read_math(
        code="thyroid_health",
        name="Thyroid Health",
        risk_score_scaled=5,
    )
    assert math["result"] == 5
    assert math["band"] == "healthy"
    assert any("Step 5:" in step for step in math["steps"])
    assert any("Result: risk_score_scaled = 5" in step for step in math["steps"])


def test_build_chart_risk_score_scaled_math_same_scores():
    people = [
        {"user_id": 1, "name": "John", "risk_score_scaled": 5},
        {"user_id": 2, "name": "Jane", "risk_score_scaled": 5},
    ]
    math = build_chart_risk_score_scaled_math(
        code="thyroid_health",
        name="Thyroid Health",
        chart_score=5,
        people=people,
        source_user_id=1,
        source_user_name="John",
    )
    assert math["result"] == 5
    assert any("Every contributor has the same score" in step for step in math["steps"])


def test_build_chart_risk_score_scaled_math_different_scores():
    people = [
        {"user_id": 1, "name": "John", "risk_score_scaled": 5},
        {"user_id": 2, "name": "Jane", "risk_score_scaled": 8},
    ]
    math = build_chart_risk_score_scaled_math(
        code="thyroid_health",
        name="Thyroid Health",
        chart_score=5,
        people=people,
        source_user_id=1,
        source_user_name="John",
    )
    assert math["result"] == 5
    assert any("Scores are not all the same" in step for step in math["steps"])
    assert any("John (ID 1)" in step for step in math["steps"])


def test_build_positive_wins_details():
    participant_rows = [
        {
            "user_id": 1,
            "name": "John Doe",
            "low_risk": [
                {
                    "code": "thyroid_health",
                    "name": "Thyroid Health",
                    "risk_status": "Healthy",
                    "risk_score_scaled": 5,
                }
            ],
            "healthy_habits": [{"habit_key": None, "habit_label": "Improved Sleep"}],
            "healthy_profiles": ["Complete Hemogram"],
            "notes": {"low_risk": None, "healthy_habits": None, "healthy_profiles": None},
            "low_risk_math": {"steps": ["picked 1"]},
        },
        {
            "user_id": 2,
            "name": "Jane Smith",
            "low_risk": [
                {
                    "code": "hypertension",
                    "name": "Hypertension",
                    "risk_status": "Healthy",
                    "risk_score_scaled": 17,
                }
            ],
            "healthy_habits": [{"habit_key": "walk", "habit_label": "Improved Sleep"}],
            "healthy_profiles": ["Liver Profile"],
            "notes": {"low_risk": None, "healthy_habits": None, "healthy_profiles": None},
            "low_risk_math": None,
        },
    ]
    payload, details = build_positive_wins_details(
        participant_rows,
        scope_label="Whole camp",
    )
    assert len(payload["data"]["low_risk"]) == 2
    assert payload["data"]["healthy_habits"][0]["habit_label"] == "Improved Sleep"
    assert details["method"]["section_kind"] == "positive_wins"
    assert len(details["participants"]) == 2
    assert details["healthy_habits"]["selection_math"]["selected_keys"] == ["Improved Sleep"]
    low_risk_selected = details["low_risk"]["selected"]
    thyroid_math = next(
        item["risk_score_scaled_math"]
        for item in low_risk_selected
        if item["code"] == "thyroid_health"
    )
    assert thyroid_math["result"] == 5
    assert len(thyroid_math["steps"]) >= 3
