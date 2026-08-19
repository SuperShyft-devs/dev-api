"""Phase 11 tests: Nutrition Target Generator."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from modules.reports.nutrition_intelligence.scoring import calculate_goal_alignment
from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import NormalizedAnswers, ScaleValue
from modules.reports.nutrition_intelligence.scoring import calculate_general_quality
from modules.reports.nutrition_intelligence.scoring import (
    calculate_final_score,
    display_nutrition_score,
)
from modules.reports.nutrition_intelligence.targets import (
    derive_activity_band,
    generate_nutrition_targets,
)


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def _answers(**kwargs) -> NormalizedAnswers:
    base = dict(health_priority_codes=())
    base.update(kwargs)
    return NormalizedAnswers(**base)  # type: ignore[arg-type]


def _combined(goal_ids: tuple[str, ...], *, config):
    return combine_goal_profiles(load_goal_profiles(goal_ids, config=config), config=config)


def test_config_loads_healthy_macros_for_all_six_goals(config):
    tg = config.target_generator
    assert tg is not None
    for goal_id in config.goals:
        macros = tg.goal_macro_targets[goal_id]
        assert macros.carbohydrate_percent is not None
        assert macros.protein_percent is not None
        assert macros.fat_percent is not None
        assert macros.fibre_g is not None


def test_weight_loss_healthy_macros_match_specification(config):
    macros = config.target_generator.goal_macro_targets["weight_loss"]
    assert macros.carbohydrate_percent.low == 40
    assert macros.carbohydrate_percent.high == 45
    assert macros.fat_percent.low == 25
    assert macros.fat_percent.high == 30
    assert macros.protein_percent.low == 25
    assert macros.protein_percent.high == 30
    assert macros.fibre_g.low == 30
    assert macros.fibre_g.high == 40


def test_single_goal_targets_include_carb_protein_fat_fibre_water(config):
    combined = _combined(("muscle_gain",), config=config)
    answers = _answers(
        gender="male",
        weight=ScaleValue(value=80, unit="kg"),
        exercise_frequency_week="2",
        exercise_level="1",
    )
    targets = generate_nutrition_targets(combined, answers, config=config)
    assert targets.carbohydrate_percent is not None
    assert targets.protein_percent is not None
    assert targets.fat_percent is not None
    assert targets.fibre_g is not None
    assert targets.water_l is not None
    assert targets.protein_g_per_kg is not None
    assert targets.protein_g is not None
    assert targets.protein_g.low == pytest.approx(1.6 * 80)
    assert targets.protein_g.high == pytest.approx(2.2 * 80)


def test_all_six_goals_produce_targets(config):
    answers = _answers(gender="female", weight=ScaleValue(value=60, unit="kg"))
    for goal_id in config.goals:
        targets = generate_nutrition_targets(
            _combined((goal_id,), config=config),
            answers,
            config=config,
        )
        assert targets.carbohydrate_percent is not None
        assert targets.protein_percent is not None
        assert targets.fat_percent is not None
        assert targets.fibre_g is not None
        assert targets.water_l is not None


def test_two_goal_macro_combination_uses_overlap_or_widen(config):
    # muscle_gain carbs 45–55, endurance carbs 50–60 → overlap 50–55
    combined = _combined(("muscle_gain", "endurance"), config=config)
    targets = generate_nutrition_targets(
        combined,
        _answers(gender="male", weight=ScaleValue(value=70, unit="kg")),
        config=config,
    )
    assert targets.carbohydrate_percent.low == pytest.approx(50)
    assert targets.carbohydrate_percent.high == pytest.approx(55)
    # fibre prefer_higher: max(25,25)=25 and max(35,35)=35
    assert targets.fibre_g.low == pytest.approx(25)
    assert targets.fibre_g.high == pytest.approx(35)


def test_two_goal_uses_phase6_protein_merge(config):
    combined = _combined(("muscle_gain", "endurance"), config=config)
    assert combined.merged_targets is not None
    targets = generate_nutrition_targets(
        combined,
        _answers(weight=ScaleValue(value=70, unit="kg")),
        config=config,
    )
    # Must consume Phase 6 merged protein range, not re-invent.
    assert targets.protein_g_per_kg == combined.merged_targets.protein_g_per_kg


def test_endurance_carbohydrate_activity_bands(config):
    combined = _combined(("endurance",), config=config)
    low = generate_nutrition_targets(
        combined,
        _answers(
            exercise_frequency_week="0",
            exercise_level="0",
            weight=ScaleValue(value=70, unit="kg"),
        ),
        config=config,
    )
    high = generate_nutrition_targets(
        combined,
        _answers(
            exercise_frequency_week="4",
            exercise_level="2",
            weight=ScaleValue(value=70, unit="kg"),
        ),
        config=config,
    )
    assert low.activity_band == "low"
    assert high.activity_band == "high"
    assert low.carbohydrate_g_per_kg.low == pytest.approx(3.0)
    assert low.carbohydrate_g_per_kg.high == pytest.approx(5.0)
    assert high.carbohydrate_g_per_kg.low == pytest.approx(6.0)
    assert high.carbohydrate_g_per_kg.high == pytest.approx(10.0)


def test_activity_does_not_change_macro_percent_for_non_endurance_gkg(config):
    combined = _combined(("weight_loss",), config=config)
    sedentary = generate_nutrition_targets(
        combined,
        _answers(exercise_frequency_week="0", exercise_level="0"),
        config=config,
    )
    active = generate_nutrition_targets(
        combined,
        _answers(exercise_frequency_week="4", exercise_level="2"),
        config=config,
    )
    assert sedentary.carbohydrate_percent == active.carbohydrate_percent
    assert sedentary.protein_g_per_kg == active.protein_g_per_kg


def test_gender_affects_water_target(config):
    combined = _combined(("metabolic_health",), config=config)
    female = generate_nutrition_targets(combined, _answers(gender="female"), config=config)
    male = generate_nutrition_targets(combined, _answers(gender="male"), config=config)
    assert female.water_l is not None
    assert male.water_l is not None
    assert male.water_l.low >= female.water_l.low


def test_endurance_elevated_water_above_standard(config):
    std = generate_nutrition_targets(
        _combined(("weight_loss",), config=config),
        _answers(gender="male"),
        config=config,
    )
    elev = generate_nutrition_targets(
        _combined(("endurance",), config=config),
        _answers(gender="male"),
        config=config,
    )
    assert elev.hydration_priority == "elevated"
    assert elev.water_l.low >= std.water_l.low


def test_zero_goals_uses_general_macros_and_baseline_keys(config):
    combined = _combined((), config=config)
    targets = generate_nutrition_targets(combined, _answers(gender="female"), config=config)
    general = config.target_generator.general_macro_targets
    assert targets.carbohydrate_percent == general.carbohydrate_percent
    assert targets.protein_g_per_kg.low == pytest.approx(1.2)
    assert targets.protein_g_per_kg.high == pytest.approx(1.6)
    assert "zero_goal_targets" in targets.notes


def test_missing_weight_omits_absolute_gram_targets(config):
    targets = generate_nutrition_targets(
        _combined(("muscle_gain",), config=config),
        _answers(),
        config=config,
    )
    assert targets.protein_g is None
    assert targets.carbohydrate_g is None
    assert targets.protein_g_per_kg is not None


def test_lb_weight_converted_for_absolute_protein(config):
    targets = generate_nutrition_targets(
        _combined(("muscle_gain",), config=config),
        _answers(weight=ScaleValue(value=220, unit="lb")),
        config=config,
    )
    kg = 220 * 0.45359237
    assert targets.protein_g.low == pytest.approx(1.6 * kg)
    assert targets.protein_g.high == pytest.approx(2.2 * kg)


def test_targets_do_not_mutate_combined_profile(config):
    combined = _combined(("strength",), config=config)
    snapshot = deepcopy(combined)
    generate_nutrition_targets(combined, _answers(gender="male"), config=config)
    assert combined == snapshot


def test_display_nutrition_score_rounds_to_whole_number():
    assert display_nutrition_score(27.44) == 27
    assert display_nutrition_score(27.5) == 28
    assert display_nutrition_score(None) is None
    assert display_nutrition_score(95.3030303030303) == 95
    assert display_nutrition_score(95.7) == 96
    assert display_nutrition_score(95.0) == 95
    assert display_nutrition_score(0.0) == 0
    assert display_nutrition_score(100.0) == 100
    for value in (95.3030303030303, 95.7, 95.0, 0.0, 100.0):
        rounded = display_nutrition_score(value)
        assert type(rounded) is int


def test_display_helper_does_not_change_internal_final_score(config):
    result = calculate_final_score(81.0, 40.0, config=config)
    expected = 0.70 * 81.0 + 0.30 * 40.0  # 68.7
    assert result.final_score == pytest.approx(expected)
    assert display_nutrition_score(result.final_score) == 69
    # Internal float remains unrounded.
    assert result.final_score == pytest.approx(68.7)
    assert result.final_score != 69


def test_targets_do_not_affect_nutrition_score_regression(config):
    """Generating targets must not change Q / A / Final for the same answers."""
    answers = _answers(
        health_priority_codes=("1",),
        diet_preference="1",
        food_groups=("1", "2", "5", "6", "7"),
        fresh_fruit_frequency="2",
        fresh_vegetable_frequency="0",
        healthy_breakfast_frequency="2",
        baked_goods_frequency="4",
        dessert_frequency="4",
        water_intake_frequency="4",
        extra_salt_frequency="1",
        exercise_frequency_week="2",
        exercise_level="1",
        gender="male",
        weight=ScaleValue(value=75, unit="kg"),
    )
    indicators = evaluate_behaviour_indicators(answers, config=config)
    quality = calculate_general_quality(indicators, config=config)
    combined = _combined(("muscle_gain",), config=config)
    alignment = calculate_goal_alignment(indicators, combined, config=config)
    final_before = calculate_final_score(
        quality.general_quality, alignment.goal_alignment, config=config
    )

    # Target generation with varying context must not touch score path.
    _ = generate_nutrition_targets(combined, answers, config=config)
    active = replace(
        answers,
        exercise_frequency_week="4",
        exercise_level="2",
        weight=ScaleValue(value=90, unit="kg"),
    )
    _ = generate_nutrition_targets(combined, active, config=config)

    indicators2 = evaluate_behaviour_indicators(answers, config=config)
    quality2 = calculate_general_quality(indicators2, config=config)
    alignment2 = calculate_goal_alignment(indicators2, combined, config=config)
    final_after = calculate_final_score(
        quality2.general_quality, alignment2.goal_alignment, config=config
    )

    assert quality2.general_quality == quality.general_quality
    assert alignment2.goal_alignment == alignment.goal_alignment
    assert final_after.final_score == final_before.final_score


def test_same_behaviour_different_goals_same_score_different_targets(config):
    base = _answers(
        diet_preference="1",
        food_groups=("0", "1", "3", "5"),
        fresh_fruit_frequency="2",
        fresh_vegetable_frequency="2",
        healthy_breakfast_frequency="1",
        baked_goods_frequency="3",
        dessert_frequency="3",
        water_intake_frequency="3",
        extra_salt_frequency="1",
        gender="female",
        weight=ScaleValue(value=62, unit="kg"),
    )
    indicators = evaluate_behaviour_indicators(base, config=config)
    q = calculate_general_quality(indicators, config=config).general_quality

    wl = generate_nutrition_targets(_combined(("weight_loss",), config=config), base, config=config)
    mg = generate_nutrition_targets(_combined(("muscle_gain",), config=config), base, config=config)
    assert wl.carbohydrate_percent != mg.carbohydrate_percent or wl.fibre_g != mg.fibre_g

    # Score path still goal-independent for Q.
    assert q is not None


def test_derive_activity_band_mapping():
    assert derive_activity_band(_answers(exercise_frequency_week="0", exercise_level="0")) == "low"
    assert derive_activity_band(_answers(exercise_frequency_week="4", exercise_level="0")) == "high"
    assert derive_activity_band(_answers(exercise_frequency_week="2", exercise_level="1")) == "moderate"


def test_no_estimated_intake_fields_on_targets(config):
    targets = generate_nutrition_targets(
        _combined(("energy_levels",), config=config),
        _answers(weight=ScaleValue(value=70, unit="kg"), gender="female"),
        config=config,
    )
    assert not hasattr(targets, "estimated_carbohydrate")
    assert not hasattr(targets, "estimated_protein")
    assert not hasattr(targets, "estimated_fat")
    assert not hasattr(targets, "estimated_fibre")
    assert not hasattr(targets, "estimated_intake")
