"""Phase 7 tests: Nutrition Intelligence Engine behaviour indicators."""

from __future__ import annotations

from dataclasses import replace

import pytest

from modules.reports.nutrition_intelligence.behaviour import evaluate_behaviour_indicators
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.models import IndicatorScore, NormalizedAnswers


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def _answers(**kwargs) -> NormalizedAnswers:
    base = dict(health_priority_codes=())
    base.update(kwargs)
    return NormalizedAnswers(**base)  # type: ignore[arg-type]


def test_every_v1_indicator_can_be_calculated(config):
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
    results = evaluate_behaviour_indicators(answers, config=config)
    assert set(results.keys()) == set(config.indicators.keys())
    for indicator_id, score in results.items():
        assert isinstance(score, IndicatorScore)
        assert score.indicator_id == indicator_id
        assert score.score is not None
        assert 0.0 <= score.score <= 100.0
        assert score.is_behavioural_proxy is True


def test_indicator_scores_are_within_0_100_when_present(config):
    answers = _answers(
        fresh_fruit_frequency="3",
        fresh_vegetable_frequency="2",
        water_intake_frequency="5",
        dessert_frequency="1",
        baked_goods_frequency="2",
        healthy_breakfast_frequency="1",
        extra_salt_frequency="1",
        food_groups=("1", "3"),
        diet_preference="0",
    )
    results = evaluate_behaviour_indicators(answers, config=config)
    for score in results.values():
        if score.score is None:
            continue
        assert 0.0 <= score.score <= 100.0


def test_better_fruit_frequency_higher_or_equal_score(config):
    ordinal = config.indicators["fruit_intake"].ordinal_scores
    codes_by_score = sorted(ordinal.items(), key=lambda kv: kv[1])
    previous = None
    for code, expected in codes_by_score:
        result = evaluate_behaviour_indicators(
            _answers(fresh_fruit_frequency=code),
            config=config,
        )["fruit_intake"]
        assert result.score == expected
        if previous is not None:
            assert result.score >= previous
        previous = result.score


def test_better_vegetable_frequency_higher_or_equal_score(config):
    ordinal = config.indicators["vegetable_intake"].ordinal_scores
    for code, expected in ordinal.items():
        result = evaluate_behaviour_indicators(
            _answers(fresh_vegetable_frequency=code),
            config=config,
        )["vegetable_intake"]
        assert result.score == expected
    assert (
        evaluate_behaviour_indicators(_answers(fresh_vegetable_frequency="0"), config=config)[
            "vegetable_intake"
        ].score
        >= evaluate_behaviour_indicators(_answers(fresh_vegetable_frequency="5"), config=config)[
            "vegetable_intake"
        ].score
    )


def test_food_group_diversity_is_deterministic(config):
    quality = config.indicators["food_diversity"].quality_group_codes
    assert quality
    half = quality[: len(quality) // 2]
    answers = _answers(food_groups=half)
    a = evaluate_behaviour_indicators(answers, config=config)["food_diversity"]
    b = evaluate_behaviour_indicators(answers, config=config)["food_diversity"]
    assert a == b
    assert a.score == pytest.approx(100.0 * len(half) / len(quality))
    full = evaluate_behaviour_indicators(_answers(food_groups=quality), config=config)["food_diversity"]
    assert full.score == 100.0
    empty = evaluate_behaviour_indicators(_answers(food_groups=()), config=config)["food_diversity"]
    assert empty.score == 0.0


def test_protein_supporting_foods_uses_diet_context(config):
    codes = config.indicators["protein_supporting_foods"].protein_supporting_group_codes
    assert "1" in codes and "7" in codes
    # Vegetarian with pulses+dairy+nuts → full applicable set (3/3)
    veg = evaluate_behaviour_indicators(
        _answers(food_groups=("1", "2", "5"), diet_preference="0"),
        config=config,
    )["protein_supporting_foods"]
    assert veg.score == 100.0
    # Same food_groups without meat is not penalised vs needing meat
    veg_partial = evaluate_behaviour_indicators(
        _answers(food_groups=("1",), diet_preference="0"),
        config=config,
    )["protein_supporting_foods"]
    assert veg_partial.score == pytest.approx(100.0 / 3.0)


def test_vegetarian_jain_not_automatically_penalized(config):
    plant_evidence = ("1", "2", "5")
    veg = evaluate_behaviour_indicators(
        _answers(food_groups=plant_evidence, diet_preference="0"),
        config=config,
    )["protein_supporting_foods"]
    jain = evaluate_behaviour_indicators(
        _answers(food_groups=plant_evidence, diet_preference="5"),
        config=config,
    )["protein_supporting_foods"]
    nonveg_same_plants = evaluate_behaviour_indicators(
        _answers(food_groups=plant_evidence, diet_preference="1"),
        config=config,
    )["protein_supporting_foods"]
    assert veg.score == 100.0
    assert jain.score == 100.0
    # Non-veg with only plant evidence scores lower because meat/eggs remain applicable.
    assert nonveg_same_plants.score is not None
    assert nonveg_same_plants.score < veg.score


def test_better_breakfast_frequency_higher_or_equal(config):
    ordinal = config.indicators["meal_regularity"].ordinal_scores
    assert (
        evaluate_behaviour_indicators(_answers(healthy_breakfast_frequency="2"), config=config)[
            "meal_regularity"
        ].score
        == ordinal["2"]
    )
    assert (
        evaluate_behaviour_indicators(_answers(healthy_breakfast_frequency="2"), config=config)[
            "meal_regularity"
        ].score
        >= evaluate_behaviour_indicators(_answers(healthy_breakfast_frequency="0"), config=config)[
            "meal_regularity"
        ].score
    )


def test_lower_baked_goods_frequency_higher_or_equal(config):
    # YAML lower_better: code "5" (rarely) → 100, "1" (4+/wk) → 0
    high = evaluate_behaviour_indicators(_answers(baked_goods_frequency="5"), config=config)[
        "baked_goods_control"
    ]
    low = evaluate_behaviour_indicators(_answers(baked_goods_frequency="1"), config=config)[
        "baked_goods_control"
    ]
    assert high.score == 100.0
    assert low.score == 0.0
    assert high.score >= low.score


def test_lower_dessert_frequency_higher_or_equal(config):
    high = evaluate_behaviour_indicators(_answers(dessert_frequency="5"), config=config)[
        "dessert_sugar_control"
    ]
    low = evaluate_behaviour_indicators(_answers(dessert_frequency="1"), config=config)[
        "dessert_sugar_control"
    ]
    assert high.score >= low.score


def test_butter_indicator_absent_from_yaml(config):
    # saturated_fat_control / butter not in indicators.yaml — must not invent.
    assert "saturated_fat_control" not in config.indicators
    results = evaluate_behaviour_indicators(
        _answers(butter_dish_frequency="5"),
        config=config,
    )
    assert "saturated_fat_control" not in results


def test_hydration_mapping_follows_yaml_exactly(config):
    ordinal = config.indicators["hydration"].ordinal_scores
    for code, expected in ordinal.items():
        score = evaluate_behaviour_indicators(
            _answers(water_intake_frequency=code),
            config=config,
        )["hydration"].score
        assert score == expected


def test_missing_values_are_none_not_automatic_zero(config):
    results = evaluate_behaviour_indicators(_answers(), config=config)
    assert results["fruit_intake"].score is None
    assert results["vegetable_intake"].score is None
    assert results["food_diversity"].score is None
    assert results["protein_supporting_foods"].score is None
    assert results["meal_regularity"].score is None
    assert results["baked_goods_control"].score is None
    assert results["dessert_sugar_control"].score is None
    assert results["hydration"].score is None
    assert results["sodium_control"].score is None


def test_invalid_ordinal_values_do_not_crash(config):
    results = evaluate_behaviour_indicators(
        _answers(fresh_fruit_frequency="99", water_intake_frequency="xyz"),
        config=config,
    )
    assert results["fruit_intake"].score is None
    assert results["hydration"].score is None


def test_repeated_evaluation_is_deterministic(config):
    answers = _answers(
        fresh_fruit_frequency="0",
        food_groups=("1", "3", "5"),
        diet_preference="0",
        water_intake_frequency="3",
        dessert_frequency="4",
    )
    assert evaluate_behaviour_indicators(answers, config=config) == evaluate_behaviour_indicators(
        answers, config=config
    )


def test_does_not_mutate_normalized_answers(config):
    answers = _answers(food_groups=("1", "2"), diet_preference="0", fresh_fruit_frequency="0")
    before_groups = answers.food_groups
    before_fruit = answers.fresh_fruit_frequency
    evaluate_behaviour_indicators(answers, config=config)
    assert answers.food_groups == before_groups
    assert answers.fresh_fruit_frequency == before_fruit


def test_goal_selection_does_not_change_raw_indicator_scores(config):
    base = _answers(
        fresh_fruit_frequency="0",
        food_groups=("1", "2", "5"),
        diet_preference="0",
        healthy_breakfast_frequency="2",
    )
    with_muscle = replace(base, health_priority_codes=("1",))
    with_weight_loss = replace(base, health_priority_codes=("0",))
    a = evaluate_behaviour_indicators(with_muscle, config=config)
    b = evaluate_behaviour_indicators(with_weight_loss, config=config)
    assert a["fruit_intake"].score == b["fruit_intake"].score
    assert a["protein_supporting_foods"].score == b["protein_supporting_foods"].score
    assert a["meal_regularity"].score == b["meal_regularity"].score


def test_activity_fields_do_not_change_nutrition_behaviour_scores(config):
    base = _answers(
        water_intake_frequency="4",
        dessert_frequency="5",
        food_groups=("0", "1", "3"),
        diet_preference="1",
    )
    active = replace(
        base,
        exercise_frequency_week="4",
        exercise_level="2",
        physical_activity_frequency="3",
        daily_active_duration="4",
    )
    sedentary = replace(
        base,
        exercise_frequency_week="0",
        exercise_level="0",
        physical_activity_frequency="5",
        daily_active_duration="0",
    )
    assert evaluate_behaviour_indicators(active, config=config) == evaluate_behaviour_indicators(
        sedentary, config=config
    )


def test_sodium_control_follows_yaml_when_present(config):
    assert "sodium_control" in config.indicators
    never = evaluate_behaviour_indicators(_answers(extra_salt_frequency="0"), config=config)[
        "sodium_control"
    ]
    usually = evaluate_behaviour_indicators(_answers(extra_salt_frequency="2"), config=config)[
        "sodium_control"
    ]
    assert never.score == 100.0
    assert usually.score == 0.0


def test_indicator_score_carries_priorities_without_using_them_to_alter_score(config):
    result = evaluate_behaviour_indicators(
        _answers(fresh_fruit_frequency="0"),
        config=config,
    )["fruit_intake"]
    assert result.general_quality_priority == config.indicators["fruit_intake"].general_quality_priority
    assert result.goal_relevance == config.indicators["fruit_intake"].goal_relevance
    assert result.score == 100.0
