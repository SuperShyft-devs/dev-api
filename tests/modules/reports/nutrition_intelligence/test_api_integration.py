"""API/integration contract: frontend option codes → engine → Health Span payload."""

from __future__ import annotations

import pytest

from modules.reports.nutrition_intelligence.engine import (
    run_nutrition_intelligence_from_lookup,
    serialize_health_span_nutrition,
)
from modules.reports.nutrition_intelligence.questionnaire import normalize_questionnaire_lookup


def _base_lookup(**overrides):
    payload = {
        "health_priorities": ["0"],
        "diet_preference": "1",
        "food_groups": ["0", "1", "2", "3", "4", "5", "6", "9"],
        "healthy_breakfast_frequency": "2",
        "fresh_fruit_frequency": "0",
        "fresh_vegetable_frequency": "0",
        "baked_goods_frequency": "5",
        "dessert_frequency": "5",
        "butter_dish_frequency": "5",
        "red_meat_frequency": "4",
        "extra_salt_frequency": "0",
        "water_intake_frequency": "4",
        "caffeine_frequency": "0",
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    "diet_code,label",
    [
        ("0", "Vegetarian"),
        ("1", "Non-Vegetarian"),
        ("2", "Eggetarian"),
        ("3", "Pescatarian"),
        ("4", "Flexitarian"),
        ("5", "Jain"),
    ],
)
def test_diet_preference_uses_stable_option_codes(diet_code, label):
    lookup = _base_lookup(diet_preference=diet_code, red_meat_frequency=None)
    answers = normalize_questionnaire_lookup(lookup)
    assert answers.diet_preference == diet_code
    result = run_nutrition_intelligence_from_lookup(lookup)
    payload = serialize_health_span_nutrition(result)
    assert payload["nutrition_score"] is not None
    assert payload["carbs"] is not None or payload["fibre"] is not None


def test_frontend_sends_codes_not_display_indexes():
    # Eggetarian is display position 3 in some UIs, but stable code is "2".
    lookup = _base_lookup(diet_preference="2")
    answers = normalize_questionnaire_lookup(lookup)
    assert answers.diet_preference == "2"
    assert answers.diet_preference != "3"


def test_multi_select_food_groups_are_backend_codes():
    lookup = _base_lookup(food_groups=["0", "1", "2", "5"])
    answers = normalize_questionnaire_lookup(lookup)
    assert answers.food_groups == ("0", "1", "2", "5")


def test_one_goal_vs_two_goals_vs_no_goal_same_current_macros():
    diet = _base_lookup(health_priorities=[])
    one = serialize_health_span_nutrition(
        run_nutrition_intelligence_from_lookup(_base_lookup(health_priorities=["0"]))
    )
    two = serialize_health_span_nutrition(
        run_nutrition_intelligence_from_lookup(_base_lookup(health_priorities=["0", "1"]))
    )
    none = serialize_health_span_nutrition(run_nutrition_intelligence_from_lookup(diet))

    for key in ("carbs", "protein", "fats"):
        assert one[key]["estimated_low"] == two[key]["estimated_low"] == none[key]["estimated_low"]
        assert one[key]["estimated_high"] == two[key]["estimated_high"] == none[key]["estimated_high"]

    assert one["protein"]["ideal_low"] != none["protein"]["ideal_low"] or one["carbs"][
        "ideal_low"
    ] != none["carbs"]["ideal_low"]
    assert two["goals"] == ["weight_loss", "muscle_gain"]
    assert none["goals"] == []


def test_missing_optional_answers_do_not_crash():
    result = run_nutrition_intelligence_from_lookup({"diet_preference": "0"})
    payload = serialize_health_span_nutrition(result)
    assert "nutrition_score" in payload


def test_invalid_option_code_is_not_rewritten_to_display_index():
    answers = normalize_questionnaire_lookup({"diet_preference": "99"})
    # Unknown codes pass through as the raw code — never remapped to UI position 1/2/3.
    assert answers.diet_preference == "99"


def test_health_span_payload_shape():
    payload = serialize_health_span_nutrition(run_nutrition_intelligence_from_lookup(_base_lookup()))
    assert set(payload.keys()) >= {
        "nutrition_score",
        "carbs",
        "protein",
        "fats",
        "fibre",
        "water",
        "disclaimer",
        "goals",
    }
    for key in ("carbs", "protein", "fats", "fibre"):
        block = payload[key]
        assert set(block.keys()) == {
            "estimated_low",
            "estimated_high",
            "ideal_low",
            "ideal_high",
            "status",
        }
    water = payload["water"]
    assert "estimated_litres" in water
    assert "estimated_low_litres" in water
    assert "estimated_high_litres" in water
    assert "Questionnaire-based estimate" in payload["disclaimer"]
    assert payload["nutrition_score"] is None or type(payload["nutrition_score"]) is int


def test_vegetarian_not_penalized_vs_comparable_nonveg():
    veg = run_nutrition_intelligence_from_lookup(
        _base_lookup(
            diet_preference="0",
            food_groups=["1", "2", "5"],
            red_meat_frequency=None,
        )
    )
    # Comparable variety: fill the non-veg applicable set to the same rich tier.
    nonveg = run_nutrition_intelligence_from_lookup(
        _base_lookup(
            diet_preference="1",
            food_groups=["1", "2", "5", "6"],
            red_meat_frequency="5",
        )
    )
    veg_p = serialize_health_span_nutrition(veg)["protein"]["estimated_low"]
    nonveg_p = serialize_health_span_nutrition(nonveg)["protein"]["estimated_low"]
    assert veg_p == pytest.approx(nonveg_p, abs=1.0)
