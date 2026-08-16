"""Questionnaire-based estimated C/P/F distribution (not measured intake)."""

from __future__ import annotations

from dataclasses import replace

import pytest

from modules.reports.nutrition_intelligence.scoring import calculate_goal_alignment
from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.nutrition import estimate_current_nutrition
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import NormalizedAnswers
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


def _healthy(**kwargs) -> NormalizedAnswers:
    payload = dict(
        diet_preference="1",
        food_groups=("0", "1", "2", "3", "4", "5", "6", "7", "9"),
        healthy_breakfast_frequency="2",
        fresh_fruit_frequency="0",
        fresh_vegetable_frequency="0",
        baked_goods_frequency="5",
        dessert_frequency="5",
        butter_dish_frequency="5",
        red_meat_frequency="4",
        water_intake_frequency="4",
        extra_salt_frequency="0",
    )
    payload.update(kwargs)
    return _answers(**payload)


def _centrals(est):
    assert est.estimated_carbohydrate_percent is not None
    assert est.estimated_protein_percent is not None
    assert est.estimated_fat_percent is not None
    return (
        est.estimated_carbohydrate_percent.central,
        est.estimated_protein_percent.central,
        est.estimated_fat_percent.central,
    )


def _bounds(config):
    spec = (config.consumption or {}).get("macro_distribution") or {}
    raw = spec.get("bounds") or {}
    return {
        "carbohydrate": tuple(raw["carbohydrate"]),
        "protein": tuple(raw["protein"]),
        "fat": tuple(raw["fat"]),
    }


def test_healthy_balanced_non_vegetarian(config):
    est = estimate_current_nutrition(_healthy(), config=config)
    c, p, f = _centrals(est)
    assert c == pytest.approx(52, abs=1.5)
    assert p == pytest.approx(16, abs=1.5)
    assert f == pytest.approx(32, abs=1.5)
    assert est.estimated_carbohydrate_percent.confidence == "HIGH"
    assert est.estimated_protein_percent.confidence == "HIGH"
    assert est.estimated_fat_percent.confidence == "HIGH"
    assert est.estimated_carbohydrate_percent.upper - est.estimated_carbohydrate_percent.lower == 6
    assert est.carbohydrate.pattern == "Whole-grain oriented"
    assert est.protein.adequacy_tier == "Rich source variety"
    assert est.estimated_protein_percent.pattern == "Rich source variety"


def test_refined_carb_heavy_user(config):
    heavy = estimate_current_nutrition(
        _healthy(
            food_groups=("2", "6", "7"),
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            baked_goods_frequency="1",
            dessert_frequency="1",
            butter_dish_frequency="1",
            red_meat_frequency="1",
        ),
        config=config,
    )
    balanced = estimate_current_nutrition(_healthy(), config=config)
    assert heavy.estimated_carbohydrate_percent.central > balanced.estimated_carbohydrate_percent.central
    assert heavy.estimated_protein_percent.central < balanced.estimated_protein_percent.central
    assert heavy.estimated_fat_percent.central > balanced.estimated_fat_percent.central
    assert heavy.carbohydrate.pattern == "Refined-carb frequent"


def test_balanced_vegetarian_not_penalized(config):
    veg = estimate_current_nutrition(
        _healthy(
            diet_preference="0",
            food_groups=("0", "1", "2", "3", "4", "5", "9"),
            red_meat_frequency=None,
        ),
        config=config,
    )
    balanced_nonveg = estimate_current_nutrition(_healthy(), config=config)
    assert veg.estimated_protein_percent is not None
    assert balanced_nonveg.estimated_protein_percent is not None
    assert veg.estimated_protein_percent.central == pytest.approx(
        balanced_nonveg.estimated_protein_percent.central, abs=0.5
    )
    assert veg.protein.adequacy_tier == "Rich source variety"


def test_high_protein_pattern_increases_only_modestly(config):
    est = estimate_current_nutrition(
        _healthy(food_groups=("1", "2", "5", "6", "7"), red_meat_frequency="1"),
        config=config,
    )
    protein = est.estimated_protein_percent
    assert protein is not None
    assert protein.central <= 20
    bounds = _bounds(config)
    assert protein.central <= bounds["protein"][1]
    assert protein.central >= 13


def test_high_dessert_baked_moves_carb_and_fat_up(config):
    base = estimate_current_nutrition(_healthy(), config=config)
    high_carb = estimate_current_nutrition(
        _healthy(dessert_frequency="1", baked_goods_frequency="1"),
        config=config,
    )
    high_fat = estimate_current_nutrition(
        _healthy(
            dessert_frequency="1",
            baked_goods_frequency="1",
            butter_dish_frequency="1",
        ),
        config=config,
    )
    assert high_carb.estimated_carbohydrate_percent.central > base.estimated_carbohydrate_percent.central
    assert high_fat.estimated_fat_percent.central > base.estimated_fat_percent.central
    bounds = _bounds(config)
    assert high_carb.estimated_carbohydrate_percent.central <= bounds["carbohydrate"][1]
    assert high_fat.estimated_fat_percent.central <= bounds["fat"][1]
    assert high_fat.fat.tendency == "Higher added/saturated-fat tendency"


def test_incomplete_questionnaire_withholds_distribution(config):
    est = estimate_current_nutrition(_answers(), config=config)
    assert est.estimated_carbohydrate_percent is None
    assert est.estimated_protein_percent is None
    assert est.estimated_fat_percent is None
    text = format_user_result(compose_user_result(_answers(), config=config), config=config)
    assert "Not enough information to estimate your dietary distribution." in text


def test_one_goal_vs_different_goal_same_current_macros(config):
    diet = _healthy()
    wl = estimate_current_nutrition(replace(diet, health_priority_codes=("0",)), config=config)
    mg = estimate_current_nutrition(replace(diet, health_priority_codes=("1",)), config=config)
    assert _centrals(wl) == _centrals(mg)
    composed_wl = compose_user_result(replace(diet, health_priority_codes=("0",)), config=config)
    composed_mg = compose_user_result(replace(diet, health_priority_codes=("1",)), config=config)
    assert composed_wl.current_nutrition == composed_mg.current_nutrition
    assert (
        composed_wl.goal_based_ideal.carbohydrate_percent
        != composed_mg.goal_based_ideal.carbohydrate_percent
    )


def test_two_goals_same_current_macros(config):
    diet = _healthy()
    one = estimate_current_nutrition(replace(diet, health_priority_codes=("0",)), config=config)
    two = estimate_current_nutrition(replace(diet, health_priority_codes=("0", "1")), config=config)
    assert _centrals(one) == _centrals(two)
    composed_one = compose_user_result(replace(diet, health_priority_codes=("0",)), config=config)
    composed_two = compose_user_result(replace(diet, health_priority_codes=("0", "1")), config=config)
    assert composed_one.current_nutrition == composed_two.current_nutrition
    assert composed_two.goals == ("weight_loss", "muscle_gain")


def test_no_goal_does_not_affect_current_macros(config):
    diet = _healthy()
    none = estimate_current_nutrition(replace(diet, health_priority_codes=()), config=config)
    one = estimate_current_nutrition(replace(diet, health_priority_codes=("0",)), config=config)
    assert _centrals(none) == _centrals(one)


def test_missing_food_groups_is_not_zero(config):
    missing = estimate_current_nutrition(
        _healthy(food_groups=None),
        config=config,
    )
    empty = estimate_current_nutrition(
        _healthy(food_groups=()),
        config=config,
    )
    assert missing.estimated_carbohydrate_percent is None
    assert empty.estimated_protein_percent is not None
    assert empty.estimated_protein_percent.central < estimate_current_nutrition(
        _healthy(), config=config
    ).estimated_protein_percent.central


def test_missing_dessert_baked_is_not_zero_carb_signal(config):
    missing = estimate_current_nutrition(
        _healthy(dessert_frequency=None, baked_goods_frequency=None),
        config=config,
    )
    rare = estimate_current_nutrition(
        _healthy(dessert_frequency="5", baked_goods_frequency="5"),
        config=config,
    )
    assert missing.estimated_carbohydrate_percent is None
    assert rare.estimated_carbohydrate_percent is not None


def test_display_ranges_respect_configured_bounds(config):
    bounds = _bounds(config)
    sparse = estimate_current_nutrition(
        _answers(
            diet_preference="1",
            food_groups=("1",),
            dessert_frequency="5",
            baked_goods_frequency="5",
            butter_dish_frequency="5",
        ),
        config=config,
    )
    rich = estimate_current_nutrition(
        _healthy(
            food_groups=("1",),
            dessert_frequency="1",
            baked_goods_frequency="1",
            butter_dish_frequency="1",
            red_meat_frequency="1",
        ),
        config=config,
    )
    for est in (sparse, rich):
        for name, estimate in (
            ("carbohydrate", est.estimated_carbohydrate_percent),
            ("protein", est.estimated_protein_percent),
            ("fat", est.estimated_fat_percent),
        ):
            assert estimate is not None
            low, high = bounds[name]
            assert estimate.lower >= low
            assert estimate.upper <= high
            assert estimate.lower <= estimate.upper
    assert sparse.estimated_protein_percent is not None
    assert sparse.estimated_protein_percent.lower >= bounds["protein"][0]
    unclamped = sparse.estimated_protein_percent.central - 5
    if unclamped < bounds["protein"][0]:
        assert sparse.estimated_protein_percent.lower == bounds["protein"][0]


def test_internal_centrals_sum_to_100(config):
    cases = [
        _healthy(),
        _healthy(dessert_frequency="1", baked_goods_frequency="1"),
        _healthy(diet_preference="0", food_groups=("1", "2", "5"), red_meat_frequency=None),
        _healthy(food_groups=("1", "2", "5", "6", "7"), red_meat_frequency="1"),
    ]
    for answers in cases:
        c, p, f = _centrals(estimate_current_nutrition(answers, config=config))
        assert c + p + f == pytest.approx(100.0, abs=1e-9)


def test_phase12_pattern_labels_preserved(config):
    est = estimate_current_nutrition(_healthy(), config=config)
    assert est.carbohydrate.pattern == "Whole-grain oriented"
    assert est.protein.adequacy_tier == "Rich source variety"
    assert est.fat.tendency == "Lower added/saturated-fat tendency"
    assert est.estimated_carbohydrate_percent.pattern == est.carbohydrate.pattern
    assert est.estimated_protein_percent.pattern == est.protein.adequacy_tier
    assert est.estimated_fat_percent.pattern == est.fat.tendency


def test_fruit_veg_breakfast_do_not_move_carb_quantity(config):
    base = estimate_current_nutrition(_healthy(), config=config)
    shifted_quality = estimate_current_nutrition(
        _healthy(
            food_groups=("1", "2", "5", "6", "7"),
            healthy_breakfast_frequency="5",
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
        ),
        config=config,
    )
    assert base.estimated_carbohydrate_percent.central == pytest.approx(
        shifted_quality.estimated_carbohydrate_percent.central
    )


def test_user_facing_shows_range_not_exact_central(config):
    result = compose_user_result(_healthy(health_priority_codes=("0",)), config=config)
    text = format_user_result(result, config=config)
    current = text.split("YOUR GOAL-BASED IDEAL")[0]
    carb = result.current_nutrition.estimated_carbohydrate_percent
    assert carb is not None
    assert "ESTIMATED DIETARY DISTRIBUTION" in current
    assert "From your questionnaire" in current
    assert f"~{carb.lower}–{carb.upper}%" in current
    assert f"Your carbohydrate intake is {carb.central:.1f}%" not in text
    assert f"Protein: {result.current_nutrition.estimated_protein_percent.central:.1f}%" not in text
    assert "not a measured intake" in text.lower()
    assert "Actual intake may vary." in text


def test_score_quality_alignment_fibre_water_targets_unchanged(config):
    answers = _healthy(
        health_priority_codes=("1",),
        exercise_frequency_week="2",
        exercise_level="1",
        weight=None,
    )
    indicators = evaluate_behaviour_indicators(answers, config=config)
    quality = calculate_general_quality(indicators, config=config)
    combined = combine_goal_profiles(
        load_goal_profiles(("muscle_gain",), config=config), config=config
    )
    alignment = calculate_goal_alignment(indicators, combined, config=config)
    final = calculate_final_score(quality.general_quality, alignment.goal_alignment, config=config)
    targets = generate_nutrition_targets(combined, answers, config=config)
    composed = compose_user_result(answers, config=config)
    current = estimate_current_nutrition(answers, config=config)

    assert composed.nutrition_score_raw == pytest.approx(final.final_score)
    assert quality.general_quality == pytest.approx(
        calculate_general_quality(indicators, config=config).general_quality
    )
    assert alignment.goal_alignment == pytest.approx(
        calculate_goal_alignment(indicators, combined, config=config).goal_alignment
    )
    assert composed.goal_based_ideal.carbohydrate_percent == targets.carbohydrate_percent
    assert composed.goal_based_ideal.protein_percent == targets.protein_percent
    assert composed.goal_based_ideal.fat_percent == targets.fat_percent
    assert composed.goal_based_ideal.fibre_g == targets.fibre_g
    assert composed.goal_based_ideal.water_l == targets.water_l
    assert current.fibre.low_g == composed.current_nutrition.fibre.low_g
    assert current.water.low_l == composed.current_nutrition.water.low_l
    assert current.fibre.tier == "High" or current.fibre.low_g is not None
