"""Phase 12 tests: estimated current nutrition consumption."""

from __future__ import annotations

from dataclasses import replace

import pytest

from modules.reports.nutrition_intelligence.scoring import calculate_goal_alignment
from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.nutrition import estimate_current_nutrition
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import EstimatedNutritionIntake, NormalizedAnswers
from modules.reports.nutrition_intelligence.scoring import calculate_general_quality
from modules.reports.nutrition_intelligence.scoring import calculate_final_score


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


def test_healthy_balanced_user(config):
    est = estimate_current_nutrition(_healthy(), config=config)
    assert est.water.available is True
    assert est.water.low_l == pytest.approx(1.6)
    assert est.water.high_l == pytest.approx(2.0)
    assert est.fibre.available is True
    assert est.fibre.low_g is not None and est.fibre.high_g is not None
    assert est.fibre.high_g >= est.fibre.low_g
    assert est.carbohydrate.available is True
    assert est.carbohydrate.pattern == "Whole-grain oriented"
    assert est.protein.adequacy_tier == "Rich source variety"
    assert est.fat.tendency == "Lower added/saturated-fat tendency"
    assert not hasattr(est, "carbohydrate_percent")
    assert not hasattr(est.carbohydrate, "percent")
    assert not hasattr(est.protein, "grams")


def test_poor_nutrition_user(config):
    est = estimate_current_nutrition(
        _answers(
            diet_preference="1",
            food_groups=(),
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            baked_goods_frequency="1",
            dessert_frequency="1",
            butter_dish_frequency="1",
            red_meat_frequency="5",
            water_intake_frequency="0",
        ),
        config=config,
    )
    assert est.water.low_l == pytest.approx(0.2)
    assert est.water.high_l == pytest.approx(0.5)
    assert est.carbohydrate.pattern == "Refined-carb frequent"
    assert est.protein.adequacy_tier == "Limited source variety"
    assert est.fat.tendency == "Higher added/saturated-fat tendency"
    assert est.fibre.tier in {"Low", "Low–Moderate"}


def test_vegetarian_not_automatically_penalized(config):
    veg = estimate_current_nutrition(
        _healthy(
            diet_preference="0",
            food_groups=("0", "1", "2", "3", "4", "5", "9"),
            red_meat_frequency=None,
        ),
        config=config,
    )
    nonveg = estimate_current_nutrition(
        _healthy(diet_preference="1", food_groups=("0", "1", "2", "3", "4", "5", "9")),
        config=config,
    )
    assert veg.protein.adequacy_tier == "Rich source variety"
    assert veg.protein.adequacy_tier == nonveg.protein.adequacy_tier


def test_high_protein_source_user(config):
    est = estimate_current_nutrition(
        _answers(
            diet_preference="1",
            food_groups=("1", "2", "5", "6", "7"),
            red_meat_frequency="1",
        ),
        config=config,
    )
    assert est.protein.adequacy_tier == "Rich source variety"
    assert est.protein.available is True


def test_low_water_user(config):
    est = estimate_current_nutrition(_answers(water_intake_frequency="0"), config=config)
    assert est.water.available is True
    assert est.water.low_l == pytest.approx(0.2)
    assert est.water.high_l == pytest.approx(0.5)


def test_more_than_eight_glasses(config):
    est = estimate_current_nutrition(_answers(water_intake_frequency="5"), config=config)
    assert est.water.low_l == pytest.approx(2.0)
    assert est.water.high_l == pytest.approx(2.6)


def test_missing_water_not_zero(config):
    est = estimate_current_nutrition(_answers(), config=config)
    assert est.water.available is False
    assert est.water.low_l is None
    assert est.water.high_l is None
    assert est.water.confidence == "INSUFFICIENT"


def test_missing_all_fibre_inputs(config):
    est = estimate_current_nutrition(_answers(), config=config)
    assert est.fibre.available is False
    assert est.fibre.low_g is None
    assert est.fibre.high_g is None
    assert est.fibre.confidence == "INSUFFICIENT"


def test_partial_fibre_inputs_widen_confidence(config):
    full = estimate_current_nutrition(
        _answers(
            food_groups=("1",),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
        ),
        config=config,
    )
    partial = estimate_current_nutrition(
        _answers(fresh_fruit_frequency="0"),
        config=config,
    )
    assert full.fibre.available is True
    assert partial.fibre.available is True
    assert partial.fibre.confidence in {"LOW", "MEDIUM"}
    conf_rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "INSUFFICIENT": 0}
    assert conf_rank[partial.fibre.confidence] < conf_rank[full.fibre.confidence]


def test_fruit_vegetable_not_double_counted(config):
    fruit_veg_only = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=(),
        ),
        config=config,
    )
    with_produce_groups = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=("3", "4"),
        ),
        config=config,
    )
    assert fruit_veg_only.fibre.low_g == pytest.approx(with_produce_groups.fibre.low_g)
    assert fruit_veg_only.fibre.high_g == pytest.approx(with_produce_groups.fibre.high_g)


def test_pulses_add_fibre_from_food_groups(config):
    without = estimate_current_nutrition(
        _answers(fresh_fruit_frequency="5", fresh_vegetable_frequency="5", food_groups=()),
        config=config,
    )
    with_pulses = estimate_current_nutrition(
        _answers(fresh_fruit_frequency="5", fresh_vegetable_frequency="5", food_groups=("1",)),
        config=config,
    )
    assert with_pulses.fibre.low_g > without.fibre.low_g
    assert with_pulses.fibre.high_g > without.fibre.high_g


def test_protein_diet_preference_filters_inapplicable_sources(config):
    veg_with_meat_group = estimate_current_nutrition(
        _answers(diet_preference="0", food_groups=("7",)),
        config=config,
    )
    nonveg_with_meat_group = estimate_current_nutrition(
        _answers(diet_preference="1", food_groups=("7",)),
        config=config,
    )
    assert veg_with_meat_group.protein.adequacy_tier == "Limited source variety"
    assert nonveg_with_meat_group.protein.adequacy_tier == "Adequate source variety"


def test_carbohydrate_pattern_classification(config):
    refined = estimate_current_nutrition(
        _answers(
            food_groups=(),
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            baked_goods_frequency="1",
            dessert_frequency="1",
        ),
        config=config,
    )
    whole = estimate_current_nutrition(
        _answers(
            food_groups=("0", "1"),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            baked_goods_frequency="5",
            dessert_frequency="5",
        ),
        config=config,
    )
    assert refined.carbohydrate.pattern == "Refined-carb frequent"
    assert whole.carbohydrate.pattern == "Whole-grain oriented"


def test_fat_tendency_classification(config):
    higher = estimate_current_nutrition(
        _answers(
            butter_dish_frequency="1",
            baked_goods_frequency="1",
            dessert_frequency="1",
        ),
        config=config,
    )
    lower = estimate_current_nutrition(
        _answers(
            butter_dish_frequency="5",
            baked_goods_frequency="5",
            dessert_frequency="5",
            red_meat_frequency="5",
        ),
        config=config,
    )
    assert higher.fat.tendency == "Higher added/saturated-fat tendency"
    assert lower.fat.tendency == "Lower added/saturated-fat tendency"


def test_missing_data_patterns_insufficient(config):
    est = estimate_current_nutrition(_answers(), config=config)
    assert est.carbohydrate.available is False
    assert est.protein.available is False
    assert est.fat.available is False
    assert est.carbohydrate.confidence == "INSUFFICIENT"


def test_confidence_changes_with_evidence(config):
    sparse = estimate_current_nutrition(_answers(baked_goods_frequency="5"), config=config)
    rich = estimate_current_nutrition(
        _answers(
            food_groups=("0",),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            baked_goods_frequency="5",
            dessert_frequency="5",
        ),
        config=config,
    )
    rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "INSUFFICIENT": 0}
    assert rank[rich.carbohydrate.confidence] > rank[sparse.carbohydrate.confidence]


def test_fibre_range_not_midpoint_only(config):
    est = estimate_current_nutrition(
        _answers(
            food_groups=("1", "0", "5", "9"),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
        ),
        config=config,
    )
    assert est.fibre.low_g is not None
    assert est.fibre.high_g is not None
    assert est.fibre.high_g > est.fibre.low_g


def test_no_macro_percentages_or_grams_generated(config):
    est = estimate_current_nutrition(_healthy(), config=config)
    assert isinstance(est, EstimatedNutritionIntake)
    dumped = str(est)
    assert "%" not in (est.carbohydrate.pattern or "")
    assert "g" not in (est.protein.adequacy_tier or "").lower() or "variety" in (
        est.protein.adequacy_tier or ""
    ).lower()
    for field in ("percent", "grams", "carbohydrate_percent", "protein_g", "fat_g"):
        assert not hasattr(est.carbohydrate, field)
        assert not hasattr(est.protein, field)
        assert not hasattr(est.fat, field)
    assert "50–55" not in dumped


def test_consumption_does_not_modify_nutrition_score(config):
    answers = _healthy(health_priority_codes=("1",), exercise_frequency_week="2", exercise_level="1")
    indicators = evaluate_behaviour_indicators(answers, config=config)
    quality = calculate_general_quality(indicators, config=config)
    combined = combine_goal_profiles(
        load_goal_profiles(("muscle_gain",), config=config), config=config
    )
    alignment = calculate_goal_alignment(indicators, combined, config=config)
    final = calculate_final_score(quality.general_quality, alignment.goal_alignment, config=config)

    _ = estimate_current_nutrition(answers, config=config)
    _ = estimate_current_nutrition(replace(answers, water_intake_frequency="0"), config=config)

    indicators2 = evaluate_behaviour_indicators(answers, config=config)
    quality2 = calculate_general_quality(indicators2, config=config)
    alignment2 = calculate_goal_alignment(indicators2, combined, config=config)
    final2 = calculate_final_score(quality2.general_quality, alignment2.goal_alignment, config=config)
    assert quality2.general_quality == quality.general_quality
    assert alignment2.goal_alignment == alignment.goal_alignment
    assert final2.final_score == final.final_score


def test_goals_and_activity_do_not_change_current_intake(config):
    base = _healthy()
    with_goal = replace(base, health_priority_codes=("1", "5"))
    active = replace(base, exercise_frequency_week="4", exercise_level="2")
    a = estimate_current_nutrition(base, config=config)
    b = estimate_current_nutrition(with_goal, config=config)
    c = estimate_current_nutrition(active, config=config)
    assert a == b == c


def test_defaulted_red_meat_ignored_for_fat_and_protein(config):
    real = estimate_current_nutrition(
        _answers(diet_preference="1", food_groups=(), red_meat_frequency="1"),
        config=config,
    )
    defaulted = estimate_current_nutrition(
        _answers(
            diet_preference="0",
            food_groups=(),
            red_meat_frequency="5",
            red_meat_frequency_defaulted=True,
            butter_dish_frequency="5",
            baked_goods_frequency="5",
            dessert_frequency="5",
        ),
        config=config,
    )
    assert real.protein.adequacy_tier == "Adequate source variety"
    assert defaulted.protein.adequacy_tier == "Limited source variety"
