"""Phase 9 tests: Goal Alignment."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from modules.reports.nutrition_intelligence.scoring import calculate_goal_alignment
from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import (
    CombinedNutritionProfile,
    IndicatorScore,
    NutritionTargets,
    PriorityLevel,
)


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def _score(
    indicator_id: str,
    score: float | None,
    *,
    config,
) -> IndicatorScore:
    definition = config.indicators[indicator_id]
    return IndicatorScore(
        indicator_id=indicator_id,
        score=score,
        source_fields=definition.source_fields,
        is_behavioural_proxy=True,
        general_quality_priority=definition.general_quality_priority,
        goal_relevance=dict(definition.goal_relevance),
    )


def _profile_for(goal_ids: tuple[str, ...], *, config) -> CombinedNutritionProfile:
    profiles = load_goal_profiles(goal_ids, config=config)
    return combine_goal_profiles(profiles, config=config)


def _expected_alignment(
    indicators: dict[str, IndicatorScore],
    priority_levels: dict[str, PriorityLevel],
    *,
    config,
) -> float:
    level_weights = config.scoring.priority_level_weights
    weighted_sum = 0.0
    total_weight = 0.0
    for indicator_id, item in indicators.items():
        if item.score is None:
            continue
        priority = priority_levels.get(indicator_id)
        if priority is None:
            continue
        weight = float(level_weights.get(priority, 0.0))
        if weight <= 0:
            continue
        weighted_sum += weight * float(item.score)
        total_weight += weight
    assert total_weight > 0
    return weighted_sum / total_weight


def test_single_goal_alignment_weighted_average(config):
    combined = _profile_for(("muscle_gain",), config=config)
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 80.0, config=config),
        "food_diversity": _score("food_diversity", 70.0, config=config),
        "hydration": _score("hydration", 60.0, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    expected = _expected_alignment(indicators, combined.priority_levels, config=config)
    assert result.goal_alignment == pytest.approx(expected)
    assert result.goal_alignment is not None
    assert set(result.indicators_used) == set(indicators.keys())


def test_two_goal_alignment_uses_combined_priority_levels(config):
    combined = _profile_for(("muscle_gain", "endurance"), config=config)
    assert combined.goals == ("muscle_gain", "endurance") or set(combined.goals) == {
        "muscle_gain",
        "endurance",
    }
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 90.0, config=config),
        "hydration": _score("hydration", 40.0, config=config),
        "food_diversity": _score("food_diversity", 50.0, config=config),
        "vegetable_intake": _score("vegetable_intake", 70.0, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    expected = _expected_alignment(indicators, combined.priority_levels, config=config)
    assert result.goal_alignment == pytest.approx(expected)
    # Weights must come from Phase 6 combined priorities, not a single goal.
    for indicator_id in result.weights_used:
        priority = combined.priority_levels[indicator_id]
        assert result.weights_used[indicator_id] == config.scoring.priority_level_weights[priority]


def test_higher_priority_indicators_have_greater_influence(config):
    combined = _profile_for(("muscle_gain",), config=config)
    # protein_supporting_foods is very_high for muscle_gain; sodium_control is low.
    protein_priority = combined.priority_levels["protein_supporting_foods"]
    sodium_priority = combined.priority_levels["sodium_control"]
    assert (
        config.scoring.priority_level_weights[protein_priority]
        > config.scoring.priority_level_weights[sodium_priority]
    )

    high_protein = {
        "protein_supporting_foods": _score("protein_supporting_foods", 100.0, config=config),
        "sodium_control": _score("sodium_control", 0.0, config=config),
    }
    high_sodium = {
        "protein_supporting_foods": _score("protein_supporting_foods", 0.0, config=config),
        "sodium_control": _score("sodium_control", 100.0, config=config),
    }
    a_high_protein = calculate_goal_alignment(high_protein, combined, config=config)
    a_high_sodium = calculate_goal_alignment(high_sodium, combined, config=config)
    assert a_high_protein.goal_alignment is not None
    assert a_high_sodium.goal_alignment is not None
    assert a_high_protein.goal_alignment > a_high_sodium.goal_alignment


def test_missing_indicators_excluded_and_renormalized(config):
    combined = _profile_for(("weight_loss",), config=config)
    protein_w = config.scoring.priority_level_weights[
        combined.priority_levels["protein_supporting_foods"]
    ]
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 80.0, config=config),
        "food_diversity": _score("food_diversity", None, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    assert result.goal_alignment == pytest.approx(80.0)
    assert result.indicators_used == ("protein_supporting_foods",)
    assert result.indicators_missing == ("food_diversity",)
    assert result.total_weight == pytest.approx(protein_w)
    assert "food_diversity" not in result.weights_used


def test_missing_indicators_not_treated_as_zero(config):
    # Force equal priorities so a zero-fill would yield 40 rather than 80.
    base = _profile_for(("muscle_gain",), config=config)
    priority_levels = dict(base.priority_levels)
    priority_levels["food_diversity"] = "high"
    priority_levels["hydration"] = "high"
    combined = replace(base, priority_levels=priority_levels)
    indicators = {
        "food_diversity": _score("food_diversity", 80.0, config=config),
        "hydration": _score("hydration", None, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    assert result.goal_alignment == pytest.approx(80.0)
    assert result.goal_alignment != pytest.approx(40.0)


def test_all_relevant_indicators_missing_returns_none(config):
    combined = _profile_for(("endurance",), config=config)
    indicators = {
        indicator_id: _score(indicator_id, None, config=config)
        for indicator_id in combined.priority_levels
        if indicator_id in config.indicators
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    assert result.goal_alignment is None
    assert result.total_weight == 0.0


def test_zero_goals_returns_none(config):
    combined = combine_goal_profiles([], config=config)
    assert combined.goals == ()
    indicators = {
        "fruit_intake": _score("fruit_intake", 90.0, config=config),
        "hydration": _score("hydration", 80.0, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    assert result.goal_alignment is None
    assert result.weights_used == {}
    assert result.total_weight == 0.0


def test_priority_none_weight_zero_does_not_affect_result(config):
    # Build a profile where one indicator is forced to priority "none".
    base = _profile_for(("muscle_gain",), config=config)
    priority_levels = dict(base.priority_levels)
    priority_levels["sodium_control"] = "none"
    combined = replace(base, priority_levels=priority_levels)

    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 50.0, config=config),
        "sodium_control": _score("sodium_control", 100.0, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    assert "sodium_control" not in result.weights_used
    assert result.goal_alignment == pytest.approx(50.0)


def test_result_between_0_and_100(config):
    combined = _profile_for(("metabolic_health",), config=config)
    indicators = {
        indicator_id: _score(indicator_id, float((i * 17) % 101), config=config)
        for i, indicator_id in enumerate(sorted(combined.priority_levels))
        if indicator_id in config.indicators
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    assert result.goal_alignment is not None
    assert 0.0 <= result.goal_alignment <= 100.0


def test_same_behaviour_different_alignment_for_different_goals(config):
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 100.0, config=config),
        "vegetable_intake": _score("vegetable_intake", 20.0, config=config),
        "dessert_sugar_control": _score("dessert_sugar_control", 20.0, config=config),
        "hydration": _score("hydration", 50.0, config=config),
        "food_diversity": _score("food_diversity", 50.0, config=config),
    }
    muscle = calculate_goal_alignment(
        indicators, _profile_for(("muscle_gain",), config=config), config=config
    )
    metabolic = calculate_goal_alignment(
        indicators, _profile_for(("metabolic_health",), config=config), config=config
    )
    assert muscle.goal_alignment is not None
    assert metabolic.goal_alignment is not None
    assert muscle.goal_alignment != metabolic.goal_alignment
    # Muscle prioritizes protein very_high; metabolic prioritizes vegetables very_high.
    assert muscle.goal_alignment > metabolic.goal_alignment


def test_selecting_goal_does_not_modify_indicator_scores(config):
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 80.0, config=config),
        "food_diversity": _score("food_diversity", 70.0, config=config),
    }
    snapshot = deepcopy(indicators)
    calculate_goal_alignment(
        indicators, _profile_for(("muscle_gain",), config=config), config=config
    )
    calculate_goal_alignment(
        indicators, _profile_for(("weight_loss",), config=config), config=config
    )
    assert indicators == snapshot
    for indicator_id, item in indicators.items():
        assert item.score == snapshot[indicator_id].score


def test_no_target_values_used_in_alignment(config):
    combined = _profile_for(("muscle_gain",), config=config)
    # Inject targets that must be ignored if somehow present on the profile.
    poisoned = replace(
        combined,
        merged_targets=NutritionTargets(
            protein_g_per_kg=None,
            notes=("should_not_affect_alignment",),
        ),
        target_keys=("protein_g_per_kg_muscle_gain",),
    )
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 80.0, config=config),
        "hydration": _score("hydration", 40.0, config=config),
    }
    with_targets = calculate_goal_alignment(indicators, poisoned, config=config)
    without = calculate_goal_alignment(indicators, combined, config=config)
    assert with_targets.goal_alignment == without.goal_alignment
    assert with_targets.weights_used == without.weights_used
    # Alignment API has no target fields.
    assert not hasattr(with_targets, "protein_g_per_kg")
    assert not hasattr(with_targets, "merged_targets")


def test_two_goal_uses_phase6_priorities_not_re_merge(config):
    """Alignment must consume CombinedNutritionProfile.priority_levels as-is."""
    combined = _profile_for(("muscle_gain", "endurance"), config=config)
    # Tamper priorities after Phase 6 — alignment must follow the profile dict,
    # not re-derive from goal ids.
    custom_levels = {
        "protein_supporting_foods": "very_high",
        "hydration": "none",
        "food_diversity": "none",
    }
    custom = replace(combined, priority_levels=custom_levels)
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 90.0, config=config),
        "hydration": _score("hydration", 10.0, config=config),
        "food_diversity": _score("food_diversity", 10.0, config=config),
    }
    result = calculate_goal_alignment(indicators, custom, config=config)
    assert result.goal_alignment == pytest.approx(90.0)
    assert result.weights_used == {
        "protein_supporting_foods": config.scoring.priority_level_weights["very_high"]
    }


def test_weights_come_from_scoring_priority_level_weights(config):
    combined = _profile_for(("strength",), config=config)
    indicators = {
        "protein_supporting_foods": _score("protein_supporting_foods", 100.0, config=config),
    }
    result = calculate_goal_alignment(indicators, combined, config=config)
    priority = combined.priority_levels["protein_supporting_foods"]
    assert result.weights_used["protein_supporting_foods"] == (
        config.scoring.priority_level_weights[priority]
    )


def test_repeated_evaluation_is_deterministic(config):
    combined = _profile_for(("energy_levels",), config=config)
    indicators = {
        "meal_regularity": _score("meal_regularity", 60.0, config=config),
        "hydration": _score("hydration", 80.0, config=config),
        "fruit_intake": _score("fruit_intake", 40.0, config=config),
    }
    assert calculate_goal_alignment(indicators, combined, config=config) == (
        calculate_goal_alignment(indicators, combined, config=config)
    )
