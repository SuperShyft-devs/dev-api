"""Phase 4 tests: Nutrition Intelligence Engine goal resolver."""

from __future__ import annotations

from modules.reports.nutrition_intelligence.goals import resolve_goal_codes, resolve_goals
from modules.reports.nutrition_intelligence.models import NormalizedAnswers


def _answers(*codes: str) -> NormalizedAnswers:
    return NormalizedAnswers(health_priority_codes=tuple(codes))


def test_each_of_six_individual_goal_codes():
    expected = {
        "0": "weight_loss",
        "1": "muscle_gain",
        "2": "metabolic_health",
        "3": "energy_levels",
        "4": "strength",
        "5": "endurance",
    }
    for code, goal_id in expected.items():
        assert resolve_goals(_answers(code)) == [goal_id]
        assert resolve_goal_codes((code,)) == [goal_id]


def test_one_valid_goal():
    assert resolve_goals(_answers("1")) == ["muscle_gain"]


def test_two_valid_goals():
    assert resolve_goals(_answers("0", "5")) == ["weight_loss", "endurance"]


def test_duplicate_goals_preserve_first_only():
    assert resolve_goals(_answers("1", "1", "1")) == ["muscle_gain"]
    assert resolve_goals(_answers("3", "4", "3")) == ["energy_levels", "strength"]


def test_invalid_goal_ignored():
    assert resolve_goals(_answers("9")) == []
    assert resolve_goals(_answers("not-a-goal")) == []


def test_mixed_valid_and_invalid_goals():
    assert resolve_goals(_answers("9", "2", "x", "4")) == ["metabolic_health", "strength"]


def test_empty_list_returns_empty():
    assert resolve_goals(NormalizedAnswers(health_priority_codes=())) == []
    assert resolve_goal_codes(()) == []
    assert resolve_goal_codes([]) == []


def test_missing_value_returns_empty():
    assert resolve_goal_codes(None) == []
    # NormalizedAnswers always has a tuple; empty is the missing equivalent.
    assert resolve_goals(NormalizedAnswers(health_priority_codes=())) == []


def test_more_than_two_legacy_goals_keeps_first_two_valid_unique():
    assert resolve_goals(_answers("0", "1", "2", "3")) == ["weight_loss", "muscle_gain"]
    assert resolve_goals(_answers("9", "5", "5", "1", "0")) == ["endurance", "muscle_gain"]


def test_ordering_preservation():
    assert resolve_goals(_answers("5", "0")) == ["endurance", "weight_loss"]
    assert resolve_goals(_answers("4", "3")) == ["strength", "energy_levels"]


def test_deterministic_repeated_calls():
    answers = _answers("2", "4")
    assert resolve_goals(answers) == resolve_goals(answers) == ["metabolic_health", "strength"]
    codes = ("0", "1", "0")
    assert resolve_goal_codes(codes) == resolve_goal_codes(codes) == ["weight_loss", "muscle_gain"]


def test_goal_selection_has_no_scoring_side_effect():
    answers = NormalizedAnswers(
        health_priority_codes=("1", "4"),
        diet_preference="0",
        weight=None,
        exercise_level="2",
    )
    goals = resolve_goals(answers)
    assert goals == ["muscle_gain", "strength"]
    # Resolver returns GoalIds only — no score / alignment / target attributes.
    assert not hasattr(goals, "nutrition_score")
    assert isinstance(goals, list)
    assert all(isinstance(g, str) for g in goals)
    # Other questionnaire fields must not influence resolution.
    assert resolve_goals(
        NormalizedAnswers(
            health_priority_codes=("1", "4"),
            diet_preference="1",
            exercise_level="0",
            gender="male",
        )
    ) == goals
