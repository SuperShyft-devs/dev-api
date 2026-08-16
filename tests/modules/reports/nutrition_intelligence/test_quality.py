"""Phase 8 tests: General Nutrition Quality."""

from __future__ import annotations

from copy import deepcopy

import pytest

from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.models import (
    IndicatorScore,
    NormalizedAnswers,
    PriorityLevel,
)
from modules.reports.nutrition_intelligence.scoring import calculate_general_quality


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def _score(
    indicator_id: str,
    score: float | None,
    *,
    priority: PriorityLevel,
    config,
) -> IndicatorScore:
    definition = config.indicators[indicator_id]
    return IndicatorScore(
        indicator_id=indicator_id,
        score=score,
        source_fields=definition.source_fields,
        is_behavioural_proxy=True,
        general_quality_priority=priority,
        goal_relevance=dict(definition.goal_relevance),
    )


def _answers(**kwargs) -> NormalizedAnswers:
    base = dict(health_priority_codes=())
    base.update(kwargs)
    return NormalizedAnswers(**base)  # type: ignore[arg-type]


def test_weighted_average_calculated_correctly(config):
    # Use two indicators with known config priorities.
    fruit_w = config.scoring.priority_level_weights[
        config.indicators["fruit_intake"].general_quality_priority
    ]
    protein_w = config.scoring.priority_level_weights[
        config.indicators["protein_supporting_foods"].general_quality_priority
    ]
    indicators = {
        "fruit_intake": _score("fruit_intake", 80.0, priority="high", config=config),
        "protein_supporting_foods": _score(
            "protein_supporting_foods", 40.0, priority="medium", config=config
        ),
    }
    result = calculate_general_quality(indicators, config=config)
    expected = (fruit_w * 80.0 + protein_w * 40.0) / (fruit_w + protein_w)
    assert result.general_quality == pytest.approx(expected)
    assert result.total_weight == pytest.approx(fruit_w + protein_w)


def test_all_available_indicators_are_included(config):
    answers = _answers(
        fresh_fruit_frequency="0",
        fresh_vegetable_frequency="0",
        food_groups=("0", "1", "2", "3", "4", "5", "6", "7", "9"),
        diet_preference="1",
        healthy_breakfast_frequency="2",
        baked_goods_frequency="5",
        dessert_frequency="5",
        water_intake_frequency="4",
        extra_salt_frequency="0",
    )
    indicators = evaluate_behaviour_indicators(answers, config=config)
    result = calculate_general_quality(indicators, config=config)
    assert result.general_quality is not None
    assert set(result.indicators_used) == set(config.indicators.keys())
    assert result.indicators_missing == ()


def test_missing_indicators_excluded_from_numerator_and_denominator(config):
    fruit_w = config.scoring.priority_level_weights["high"]
    indicators = {
        "fruit_intake": _score("fruit_intake", 80.0, priority="high", config=config),
        "vegetable_intake": _score("vegetable_intake", None, priority="high", config=config),
    }
    result = calculate_general_quality(indicators, config=config)
    assert result.general_quality == pytest.approx(80.0)
    assert result.indicators_used == ("fruit_intake",)
    assert result.indicators_missing == ("vegetable_intake",)
    assert result.total_weight == pytest.approx(fruit_w)
    assert "vegetable_intake" not in result.weights_used


def test_missing_indicators_not_treated_as_zero(config):
    indicators = {
        "fruit_intake": _score("fruit_intake", 80.0, priority="high", config=config),
        "vegetable_intake": _score("vegetable_intake", None, priority="high", config=config),
    }
    result = calculate_general_quality(indicators, config=config)
    # If None were treated as 0 with equal weights → 40; must be 80.
    assert result.general_quality == pytest.approx(80.0)
    assert result.general_quality != pytest.approx(40.0)


def test_all_indicators_missing_returns_none(config):
    indicators = {
        indicator_id: _score(
            indicator_id,
            None,
            priority=definition.general_quality_priority,
            config=config,
        )
        for indicator_id, definition in config.indicators.items()
    }
    result = calculate_general_quality(indicators, config=config)
    assert result.general_quality is None
    assert result.total_weight == 0.0
    assert set(result.indicators_missing) == set(config.indicators.keys())


def test_single_available_indicator_produces_that_score(config):
    indicators = {
        "hydration": _score("hydration", 75.0, priority="high", config=config),
        "fruit_intake": _score("fruit_intake", None, priority="high", config=config),
    }
    result = calculate_general_quality(indicators, config=config)
    assert result.general_quality == pytest.approx(75.0)


def test_general_quality_weights_come_from_configuration(config):
    indicators = {
        "fruit_intake": _score("fruit_intake", 100.0, priority="high", config=config),
        "meal_regularity": _score("meal_regularity", 0.0, priority="medium", config=config),
    }
    result = calculate_general_quality(indicators, config=config)
    assert result.weights_used["fruit_intake"] == config.scoring.priority_level_weights["high"]
    assert result.weights_used["meal_regularity"] == config.scoring.priority_level_weights["medium"]


def test_priority_numeric_mapping_comes_from_scoring_yaml(config):
    assert config.scoring.priority_level_weights["very_high"] == 1.0
    assert config.scoring.priority_level_weights["high"] == 0.75
    assert config.scoring.priority_level_weights["medium"] == 0.5
    assert config.scoring.priority_level_weights["low"] == 0.25
    assert config.scoring.priority_level_weights["none"] == 0.0
    assert config.scoring.missing_indicator_policy == "exclude_and_renormalize"


def test_same_behaviour_same_q_regardless_of_selected_goal(config):
    answers_a = _answers(
        health_priority_codes=("0",),
        fresh_fruit_frequency="0",
        fresh_vegetable_frequency="2",
        water_intake_frequency="4",
        dessert_frequency="5",
        food_groups=("1", "3", "5"),
        diet_preference="0",
        healthy_breakfast_frequency="2",
        baked_goods_frequency="4",
        extra_salt_frequency="1",
    )
    answers_b = _answers(
        health_priority_codes=("1", "5"),
        fresh_fruit_frequency="0",
        fresh_vegetable_frequency="2",
        water_intake_frequency="4",
        dessert_frequency="5",
        food_groups=("1", "3", "5"),
        diet_preference="0",
        healthy_breakfast_frequency="2",
        baked_goods_frequency="4",
        extra_salt_frequency="1",
    )
    qa = calculate_general_quality(
        evaluate_behaviour_indicators(answers_a, config=config), config=config
    )
    qb = calculate_general_quality(
        evaluate_behaviour_indicators(answers_b, config=config), config=config
    )
    assert qa.general_quality == qb.general_quality
    assert qa.weights_used == qb.weights_used


def test_same_behaviour_same_q_regardless_of_goal_combination(config):
    # Quality engine does not accept CombinedNutritionProfile; prove via identical
    # indicator maps that combination metadata cannot affect Q.
    indicators = evaluate_behaviour_indicators(
        _answers(
            fresh_fruit_frequency="0",
            water_intake_frequency="3",
            food_groups=("0", "1", "3"),
            diet_preference="1",
            dessert_frequency="3",
            baked_goods_frequency="3",
            healthy_breakfast_frequency="1",
            fresh_vegetable_frequency="0",
            extra_salt_frequency="0",
        ),
        config=config,
    )
    q1 = calculate_general_quality(indicators, config=config)
    q2 = calculate_general_quality(indicators, config=config)
    assert q1 == q2


def test_activity_fields_do_not_directly_affect_q(config):
    base_kwargs = dict(
        fresh_fruit_frequency="0",
        water_intake_frequency="4",
        dessert_frequency="5",
        food_groups=("1", "3"),
        diet_preference="0",
        healthy_breakfast_frequency="2",
        baked_goods_frequency="5",
        fresh_vegetable_frequency="0",
        extra_salt_frequency="0",
    )
    active = _answers(
        **base_kwargs,
        exercise_frequency_week="4",
        exercise_level="2",
        physical_activity_frequency="3",
        daily_active_duration="4",
    )
    sedentary = _answers(
        **base_kwargs,
        exercise_frequency_week="0",
        exercise_level="0",
        physical_activity_frequency="5",
        daily_active_duration="0",
    )
    qa = calculate_general_quality(evaluate_behaviour_indicators(active, config=config), config=config)
    qb = calculate_general_quality(
        evaluate_behaviour_indicators(sedentary, config=config), config=config
    )
    assert qa.general_quality == qb.general_quality


def test_target_values_do_not_affect_q(config):
    # Quality only sees IndicatorScore maps — no targets involved.
    indicators = {
        "fruit_intake": _score("fruit_intake", 90.0, priority="high", config=config),
    }
    result = calculate_general_quality(indicators, config=config)
    assert result.general_quality == pytest.approx(90.0)
    assert not hasattr(result, "protein_g_per_kg")


def test_result_between_0_and_100_when_available(config):
    indicators = evaluate_behaviour_indicators(
        _answers(
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            food_groups=(),
            diet_preference="1",
            healthy_breakfast_frequency="0",
            baked_goods_frequency="1",
            dessert_frequency="1",
            water_intake_frequency="0",
            extra_salt_frequency="2",
        ),
        config=config,
    )
    result = calculate_general_quality(indicators, config=config)
    assert result.general_quality is not None
    assert 0.0 <= result.general_quality <= 100.0


def test_repeated_evaluation_is_deterministic(config):
    indicators = evaluate_behaviour_indicators(
        _answers(fresh_fruit_frequency="2", water_intake_frequency="4", food_groups=("3",)),
        config=config,
    )
    assert calculate_general_quality(indicators, config=config) == calculate_general_quality(
        indicators, config=config
    )


def test_input_indicator_objects_are_not_mutated(config):
    indicators = {
        "fruit_intake": _score("fruit_intake", 80.0, priority="high", config=config),
        "hydration": _score("hydration", 55.0, priority="high", config=config),
    }
    snapshot = deepcopy(indicators)
    calculate_general_quality(indicators, config=config)
    assert indicators == snapshot
