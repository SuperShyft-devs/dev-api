"""Phase 13 tests: user-facing nutrition result composer."""

from __future__ import annotations

import pytest

from modules.reports.nutrition_intelligence.scoring import calculate_goal_alignment
from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import NormalizedAnswers, ScaleValue
from modules.reports.nutrition_intelligence.scoring import calculate_general_quality
from modules.reports.nutrition_intelligence.result import compose_user_result, format_user_result
from modules.reports.nutrition_intelligence.scoring import calculate_final_score
from modules.reports.nutrition_intelligence.targets import generate_nutrition_targets


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def _answers(**kwargs) -> NormalizedAnswers:
    base = dict(health_priority_codes=())
    base.update(kwargs)
    return NormalizedAnswers(**base)  # type: ignore[arg-type]


def _realistic(**kwargs) -> NormalizedAnswers:
    payload = dict(
        health_priority_codes=("0",),
        diet_preference="1",
        food_groups=("0", "1", "3", "5"),
        healthy_breakfast_frequency="2",
        fresh_fruit_frequency="2",
        fresh_vegetable_frequency="0",
        baked_goods_frequency="2",
        dessert_frequency="3",
        butter_dish_frequency="3",
        red_meat_frequency="3",
        water_intake_frequency="4",
        extra_salt_frequency="1",
        gender="female",
        weight=ScaleValue(value=62, unit="kg"),
        exercise_frequency_week="2",
        exercise_level="1",
    )
    payload.update(kwargs)
    return _answers(**payload)


def test_nutrition_score_is_integer_for_display(config):
    result = compose_user_result(_realistic(), config=config)
    assert result.nutrition_score is None or isinstance(result.nutrition_score, int)


def test_underlying_score_remains_unchanged(config):
    answers = _realistic()
    result = compose_user_result(answers, config=config)
    indicators = evaluate_behaviour_indicators(answers, config=config)
    quality = calculate_general_quality(indicators, config=config)
    combined = combine_goal_profiles(
        load_goal_profiles(result.goals, config=config), config=config
    )
    alignment = calculate_goal_alignment(indicators, combined, config=config)
    final = calculate_final_score(quality.general_quality, alignment.goal_alignment, config=config)
    assert result.nutrition_score_raw == pytest.approx(final.final_score)
    if result.nutrition_score is not None:
        assert result.nutrition_score == int(round(final.final_score))


def test_current_nutrition_is_present(config):
    result = compose_user_result(_realistic(), config=config)
    assert result.current_nutrition.water.available is True
    assert result.current_nutrition.fibre.available is True
    assert result.current_nutrition.carbohydrate.pattern is not None
    assert result.current_nutrition.protein.adequacy_tier is not None
    assert result.current_nutrition.fat.tendency is not None


def test_goal_targets_are_present(config):
    result = compose_user_result(_realistic(), config=config)
    assert result.goal_based_ideal is not None
    assert result.goal_based_ideal.carbohydrate_percent is not None
    assert result.goal_based_ideal.protein_percent is not None
    assert result.goal_based_ideal.fat_percent is not None
    assert result.goal_based_ideal.fibre_g is not None
    assert result.goal_based_ideal.water_l is not None


def test_one_goal_result(config):
    result = compose_user_result(_realistic(health_priority_codes=("1",)), config=config)
    assert result.goals == ("muscle_gain",)
    assert result.goal_based_ideal.protein_percent.low == 25


def test_two_goal_result(config):
    result = compose_user_result(
        _realistic(health_priority_codes=("1", "5")),
        config=config,
    )
    assert result.goals == ("muscle_gain", "endurance")
    assert result.goal_based_ideal.carbohydrate_percent.low == pytest.approx(50)
    assert result.goal_based_ideal.carbohydrate_percent.high == pytest.approx(55)


def test_zero_goal_result(config):
    result = compose_user_result(_realistic(health_priority_codes=()), config=config)
    assert result.goals == ()
    assert result.nutrition_score is not None
    assert result.current_nutrition.water.available is True
    assert result.goal_based_ideal is not None
    assert result.goal_based_ideal.carbohydrate_percent is not None


def test_current_intake_independent_of_goal(config):
    a = compose_user_result(_realistic(health_priority_codes=("0",)), config=config)
    b = compose_user_result(_realistic(health_priority_codes=("1",)), config=config)
    assert a.current_nutrition == b.current_nutrition


def test_ideal_targets_change_with_goal(config):
    wl = compose_user_result(_realistic(health_priority_codes=("0",)), config=config)
    en = compose_user_result(_realistic(health_priority_codes=("5",)), config=config)
    assert wl.goal_based_ideal.carbohydrate_percent != en.goal_based_ideal.carbohydrate_percent


def test_no_current_macro_percentages(config):
    result = compose_user_result(_realistic(), config=config)
    current = result.current_nutrition
    assert not hasattr(current.carbohydrate, "percent")
    assert not hasattr(current.protein, "percent")
    assert not hasattr(current.fat, "percent")
    text = format_user_result(result, config=config)
    current_section = text.split("YOUR GOAL-BASED IDEAL")[0]
    assert "Carbohydrates\n40–45%" not in current_section
    assert "Protein\n25–30%" not in current_section


def test_fibre_current_range_appears(config):
    result = compose_user_result(_realistic(), config=config)
    text = format_user_result(result, config=config)
    assert "g/day" in text
    assert result.current_nutrition.fibre.low_g is not None
    assert "~" in text.split("Fibre")[1].split("Water")[0]


def test_water_current_range_appears(config):
    result = compose_user_result(_realistic(), config=config)
    text = format_user_result(result, config=config)
    assert "~1.6–2 L/day" in text or "~1.6–2.0 L/day" in text


def test_user_facing_result_does_not_expose_qa(config):
    text = format_user_result(compose_user_result(_realistic(), config=config), config=config)
    assert "General Quality" not in text
    assert "Goal Alignment" not in text
    assert "GENERAL NUTRITION QUALITY" not in text
    assert "indicators_used" not in text
    result = compose_user_result(_realistic(), config=config)
    assert not hasattr(result, "general_quality")
    assert not hasattr(result, "goal_alignment")


def test_existing_target_values_are_reused(config):
    answers = _realistic()
    composed = compose_user_result(answers, config=config)
    combined = combine_goal_profiles(
        load_goal_profiles(composed.goals, config=config), config=config
    )
    expected = generate_nutrition_targets(combined, answers, config=config)
    assert composed.goal_based_ideal.carbohydrate_percent == expected.carbohydrate_percent
    assert composed.goal_based_ideal.protein_percent == expected.protein_percent
    assert composed.goal_based_ideal.fat_percent == expected.fat_percent
    assert composed.goal_based_ideal.fibre_g == expected.fibre_g
    assert composed.goal_based_ideal.water_l == expected.water_l


def test_existing_scoring_values_are_reused(config):
    answers = _realistic()
    composed = compose_user_result(answers, config=config)
    indicators = evaluate_behaviour_indicators(answers, config=config)
    quality = calculate_general_quality(indicators, config=config)
    combined = combine_goal_profiles(
        load_goal_profiles(composed.goals, config=config), config=config
    )
    alignment = calculate_goal_alignment(indicators, combined, config=config)
    final = calculate_final_score(quality.general_quality, alignment.goal_alignment, config=config)
    assert composed.nutrition_score_raw == final.final_score


def test_missing_water_displays_not_enough_information(config):
    result = compose_user_result(_realistic(water_intake_frequency=None), config=config)
    text = format_user_result(result, config=config)
    water_block = text.split("Water")[1].split("YOUR GOAL-BASED IDEAL")[0]
    assert "Not enough information" in water_block
