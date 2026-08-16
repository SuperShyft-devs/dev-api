"""Questionnaire-based fibre range estimation (not measured intake)."""

from __future__ import annotations

import pytest

from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.nutrition import estimate_current_nutrition
from modules.reports.nutrition_intelligence.models import NormalizedAnswers
from modules.reports.nutrition_intelligence.result import compose_user_result, format_user_result


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def _answers(**kwargs) -> NormalizedAnswers:
    base = dict(health_priority_codes=())
    base.update(kwargs)
    return NormalizedAnswers(**base)  # type: ignore[arg-type]


def _display(low: float, high: float) -> tuple[int, int]:
    return int(round(low)), int(round(high))


def test_very_low_fruit_veg_without_fibre_groups(config):
    est = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            food_groups=(),
        ),
        config=config,
    )
    assert est.fibre.available is True
    assert est.fibre.low_g is not None and est.fibre.high_g is not None
    low, high = _display(est.fibre.low_g, est.fibre.high_g)
    assert high > low
    assert (low, high) != (1, 1)
    assert high <= 15


def test_regular_fruit_and_vegetables(config):
    low_case = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            food_groups=(),
        ),
        config=config,
    )
    regular = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=(),
        ),
        config=config,
    )
    assert regular.fibre.available is True
    assert regular.fibre.low_g > low_case.fibre.low_g
    assert regular.fibre.high_g > low_case.fibre.high_g
    low, high = _display(regular.fibre.low_g, regular.fibre.high_g)
    assert high > low
    assert high - low >= 4


def test_multiple_fibre_rich_food_groups(config):
    produce_only = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=(),
        ),
        config=config,
    )
    with_groups = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=("0", "1", "5", "9"),
        ),
        config=config,
    )
    assert with_groups.fibre.low_g > produce_only.fibre.low_g
    assert with_groups.fibre.high_g > produce_only.fibre.high_g
    low, high = _display(with_groups.fibre.low_g, with_groups.fibre.high_g)
    assert high > low


def test_food_groups_do_not_double_count_fruit_and_vegetables(config):
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


def test_non_fibre_food_groups_do_not_add_fibre(config):
    empty = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=(),
        ),
        config=config,
    )
    dairy_eggs = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=("2", "6", "7"),
        ),
        config=config,
    )
    assert empty.fibre.low_g == pytest.approx(dairy_eggs.fibre.low_g)
    assert empty.fibre.high_g == pytest.approx(dairy_eggs.fibre.high_g)


def test_vegetarian_with_pulses_nuts_whole_grains(config):
    veg = estimate_current_nutrition(
        _answers(
            diet_preference="0",
            food_groups=("0", "1", "2", "5"),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
        ),
        config=config,
    )
    without_groups = estimate_current_nutrition(
        _answers(
            diet_preference="0",
            food_groups=(),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
        ),
        config=config,
    )
    assert veg.fibre.available is True
    assert veg.fibre.low_g > without_groups.fibre.low_g
    assert veg.fibre.high_g > without_groups.fibre.high_g


def test_missing_fruit_vegetable_fields_not_zero(config):
    missing_produce = estimate_current_nutrition(
        _answers(food_groups=("1", "0", "5")),
        config=config,
    )
    rare_produce = estimate_current_nutrition(
        _answers(
            food_groups=("1", "0", "5"),
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
        ),
        config=config,
    )
    assert missing_produce.fibre.available is True
    assert missing_produce.fibre.high_g > rare_produce.fibre.high_g
    assert missing_produce.fibre.low_g == pytest.approx(rare_produce.fibre.low_g, abs=1.0)


def test_missing_food_groups_not_zero(config):
    missing_groups = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=None,
        ),
        config=config,
    )
    empty_groups = estimate_current_nutrition(
        _answers(
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            food_groups=(),
        ),
        config=config,
    )
    assert missing_groups.fibre.available is True
    assert empty_groups.fibre.available is True
    assert missing_groups.fibre.low_g == pytest.approx(empty_groups.fibre.low_g)
    assert missing_groups.fibre.high_g > empty_groups.fibre.high_g


def test_all_fibre_inputs_missing_is_not_enough_information(config):
    est = estimate_current_nutrition(_answers(), config=config)
    assert est.fibre.available is False
    assert est.fibre.low_g is None
    assert est.fibre.high_g is None
    assert est.fibre.confidence == "INSUFFICIENT"
    text = format_user_result(compose_user_result(_answers(), config=config), config=config)
    fibre_block = text.split("YOUR GOAL-BASED IDEAL")[0].split("Fibre")[1].split("Water")[0]
    assert "Not enough information" in fibre_block


def test_empty_food_groups_alone_is_not_enough_information(config):
    est = estimate_current_nutrition(_answers(food_groups=()), config=config)
    assert est.fibre.available is False
    assert est.fibre.low_g is None


def test_user_facing_fibre_has_no_tier_label(config):
    result = compose_user_result(
        _answers(
            health_priority_codes=("0",),
            diet_preference="1",
            food_groups=("0", "1", "5", "9"),
            fresh_fruit_frequency="0",
            fresh_vegetable_frequency="0",
            water_intake_frequency="4",
        ),
        config=config,
    )
    text = format_user_result(result, config=config)
    fibre_block = text.split("YOUR GOAL-BASED IDEAL")[0].split("Fibre")[1].split("Water")[0]
    assert "g/day" in fibre_block
    assert "Low" not in fibre_block
    assert "Moderate" not in fibre_block
    assert "High" not in fibre_block
    assert result.current_nutrition.fibre.tier is not None


def test_no_collapsed_one_to_one_display_when_evidence_is_weak(config):
    cases = [
        _answers(fresh_fruit_frequency="3", fresh_vegetable_frequency="3", food_groups=()),
        _answers(fresh_fruit_frequency="4", fresh_vegetable_frequency="4", food_groups=()),
        _answers(fresh_fruit_frequency="5", fresh_vegetable_frequency="5", food_groups=()),
        _answers(fresh_fruit_frequency="3", fresh_vegetable_frequency="5"),
        _answers(food_groups=("1",)),
    ]
    for answers in cases:
        est = estimate_current_nutrition(answers, config=config)
        if not est.fibre.available:
            continue
        low, high = _display(est.fibre.low_g, est.fibre.high_g)
        assert high > low
        assert (low, high) != (1, 1)
        text = format_user_result(compose_user_result(answers, config=config), config=config)
        fibre_block = text.split("YOUR GOAL-BASED IDEAL")[0].split("Fibre")[1].split("Water")[0]
        assert "~1–1 g/day" not in fibre_block
        assert "Not enough information" not in fibre_block or high > low


def test_occasional_fruit_veg_is_between_rare_and_daily(config):
    rare = estimate_current_nutrition(
        _answers(fresh_fruit_frequency="5", fresh_vegetable_frequency="5", food_groups=()),
        config=config,
    )
    occasional = estimate_current_nutrition(
        _answers(fresh_fruit_frequency="2", fresh_vegetable_frequency="2", food_groups=()),
        config=config,
    )
    daily = estimate_current_nutrition(
        _answers(fresh_fruit_frequency="0", fresh_vegetable_frequency="0", food_groups=()),
        config=config,
    )
    assert rare.fibre.high_g < occasional.fibre.high_g < daily.fibre.high_g
