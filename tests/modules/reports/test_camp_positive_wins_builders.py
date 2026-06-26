"""Unit tests for positive_wins camp report aggregation helpers."""

from __future__ import annotations

from modules.reports.camp_report_section_builders import (
    aggregate_top_healthy_habits,
    aggregate_top_healthy_profiles,
    build_positive_wins,
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


def test_build_positive_wins_shape():
    payload = build_positive_wins(
        healthy_habits=[{"habit_key": "k", "habit_label": "Label"}],
        healthy_profiles=["Group A"],
    )
    assert payload == {
        "data": {
            "healthy_habits": [{"habit_key": "k", "habit_label": "Label"}],
            "healthy_profiles": ["Group A"],
        },
    }
