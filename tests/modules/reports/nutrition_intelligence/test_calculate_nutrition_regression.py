"""Regression: calculate_nutrition() must match the existing engine output."""

from __future__ import annotations

from copy import deepcopy

import pytest

from modules.reports.nutrition_intelligence.engine import (
    run_nutrition_intelligence_from_lookup,
    serialize_health_span_nutrition,
)
from nutrition_engine import _to_questionnaire_lookup, calculate_nutrition


def _base(**overrides):
    payload = {
        "gender": "female",
        "height": 5.4,
        "hight_unit": "ft",
        "exercise_frequency_week": "4",
        "exercise_level": "2",
        "healthy_breakfast_frequency": "2",
        "diet_preference": "1",
        "food_groups": ["0", "1", "2", "3", "4", "5", "7", "9"],
        "fresh_fruit_frequency": "0",
        "fresh_vegetable_frequency": "0",
        "baked_goods_frequency": "5",
        "red_meat_frequency": "5",
        "butter_dish_frequency": "5",
        "dessert_frequency": "5",
        "caffeine_frequency": "1",
        "water_intake_frequency": "5",
        "tobacco_frequency": "0",
        "alcohol_frequency": "0",
        "sickness_frequency": "0",
    }
    payload.update(overrides)
    return payload


def _via_current_engine(payload: dict) -> dict:
    lookup = _to_questionnaire_lookup(payload)
    return serialize_health_span_nutrition(run_nutrition_intelligence_from_lookup(lookup))


def _assert_equivalent(payload: dict) -> dict:
    current = _via_current_engine(payload)
    consolidated = calculate_nutrition(payload)
    assert consolidated == current
    return consolidated


def test_healthy_balanced_user():
    result = _assert_equivalent(_base())
    assert result["nutrition_score"] is not None
    assert "nutrition_score" in result
    assert set(result.keys()) >= {
        "nutrition_score",
        "carbs",
        "protein",
        "fats",
        "fibre",
        "water",
        "disclaimer",
        "goals",
    }


def test_poor_diet():
    _assert_equivalent(
        _base(
            gender="male",
            height=5.11,
            exercise_frequency_week="0",
            exercise_level="0",
            healthy_breakfast_frequency="0",
            food_groups=["7"],
            fresh_fruit_frequency="5",
            fresh_vegetable_frequency="5",
            baked_goods_frequency="1",
            red_meat_frequency="1",
            butter_dish_frequency="1",
            dessert_frequency="1",
            caffeine_frequency="2",
            water_intake_frequency="0",
            tobacco_frequency="4",
            alcohol_frequency="3",
            sickness_frequency="4",
        )
    )


def test_vegetarian():
    result = _assert_equivalent(
        _base(
            diet_preference="0",
            food_groups=["0", "1", "2", "3", "4", "5", "9"],
            red_meat_frequency=None,
        )
    )
    assert result["nutrition_score"] is not None


def test_high_protein_active_user():
    _assert_equivalent(
        _base(
            gender="male",
            height=180,
            hight_unit="cm",
            exercise_frequency_week="4",
            exercise_level="2",
            diet_preference="1",
            food_groups=["1", "2", "5", "6", "7"],
            health_priorities=["1"],
        )
    )


def test_weight_loss_goal():
    result = _assert_equivalent(_base(health_priorities=["0"]))
    assert result["goals"] == ["weight_loss"]


def test_missing_questionnaire_fields():
    result = _assert_equivalent({"diet_preference": "0"})
    assert "nutrition_score" in result


def test_multiple_goals():
    result = _assert_equivalent(_base(health_priorities=["0", "1"]))
    assert result["goals"] == ["weight_loss", "muscle_gain"]


def test_no_goals():
    result = _assert_equivalent(_base(health_priorities=[]))
    assert result["goals"] == []


def test_frequent_dessert_baked_goods():
    _assert_equivalent(
        _base(
            baked_goods_frequency="1",
            dessert_frequency="1",
        )
    )


def test_frequent_butter_red_meat():
    _assert_equivalent(
        _base(
            butter_dish_frequency="1",
            red_meat_frequency="1",
        )
    )


def test_tobacco_alcohol_present():
    _assert_equivalent(
        _base(
            tobacco_frequency="3",
            alcohol_frequency="2",
        )
    )


@pytest.mark.parametrize("exercise_level", ["0", "1", "2"])
def test_different_exercise_levels(exercise_level):
    _assert_equivalent(_base(exercise_level=exercise_level))


@pytest.mark.parametrize(
    "gender,height,hight_unit",
    [
        ("female", 5.4, "ft"),
        ("male", 5.9, "ft"),
        ("male", 180, "cm"),
        ("female", 64, "in"),
    ],
)
def test_different_gender_height_inputs(gender, height, hight_unit):
    _assert_equivalent(_base(gender=gender, height=height, hight_unit=hight_unit))


def test_hight_unit_field_is_preserved_on_input():
    payload = _base()
    assert "hight_unit" in payload
    original = deepcopy(payload)
    calculate_nutrition(payload)
    assert payload == original
    assert payload["hight_unit"] == "ft"


def test_height_unit_alias_from_reports_service():
    payload = _base()
    payload.pop("hight_unit")
    payload["height_unit"] = "ft"
    _assert_equivalent(payload)


def test_public_nutrition_score_is_whole_number_not_float():
    import json

    result = calculate_nutrition(_base())
    score = result["nutrition_score"]
    assert type(score) is int
    assert score == 95

    encoded = json.dumps(result, separators=(",", ":"))
    parsed = json.loads(encoded)
    assert parsed["nutrition_score"] == 95
    assert type(parsed["nutrition_score"]) is int
    assert "95.303" not in encoded
    assert '"nutrition_score":95' in encoded


def test_calculate_endpoint_json_nutrition_score_is_integer():
    import json

    from fastapi.testclient import TestClient

    from nutrition_main import app

    client = TestClient(app)
    response = client.post("/calculate", json=_base())
    assert response.status_code == 200
    body = response.json()
    assert type(body["nutrition_score"]) is int
    assert body["nutrition_score"] == 95
    raw = response.content.decode("utf-8")
    compact = json.dumps(json.loads(raw), separators=(",", ":"))
    assert '"nutrition_score":95' in compact
    assert "95.303" not in raw


@pytest.mark.parametrize(
    "raw,expected",
    [
        (95.3030303030303, 95),
        (95.7, 96),
        (95.0, 95),
        (0.0, 0),
        (100.0, 100),
    ],
)
def test_serializer_rounds_only_public_nutrition_score(raw, expected):
    from dataclasses import replace

    from modules.reports.nutrition_intelligence.scoring import display_nutrition_score

    result = run_nutrition_intelligence_from_lookup(_to_questionnaire_lookup(_base()))
    assert isinstance(result.nutrition_score_raw, float)
    assert result.nutrition_score_raw != result.nutrition_score
    stubbed = replace(result, nutrition_score_raw=raw, nutrition_score=display_nutrition_score(raw))
    payload = serialize_health_span_nutrition(stubbed)
    assert payload["nutrition_score"] == expected
    assert type(payload["nutrition_score"]) is int
    # Internal raw on the result object is unchanged.
    assert stubbed.nutrition_score_raw == raw
