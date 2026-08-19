"""Phase 3 tests: Nutrition Intelligence Engine input normalizer."""

from __future__ import annotations

from modules.reports.nutrition_intelligence import normalize_questionnaire_lookup
from modules.reports.nutrition_intelligence.models import NormalizedAnswers


def _exercise_level_map() -> dict[str, str]:
    return {
        "0": "0",
        "1": "1",
        "2": "2",
        "Low": "0",
        "Moderate": "1",
        "High": "2",
        "low": "0",
        "moderate": "1",
        "high": "2",
        "lowintensity": "0",
        "moderateintensity": "1",
        "highintensity": "2",
    }


def _food_groups_map() -> dict[str, str]:
    return {
        "0": "0",
        "1": "1",
        "3": "3",
        "4": "4",
        "Fresh vegetables": "3",
        "freshvegetables": "3",
        "Whole grains": "0",
        "wholegrains": "0",
        "Pulses/ Legumes": "1",
        "pulseslegumes": "1",
        "Fresh fruits": "4",
        "freshfruits": "4",
    }


def _health_priorities_map() -> dict[str, str]:
    return {
        "0": "0",
        "1": "1",
        "2": "2",
        "3": "3",
        "4": "4",
        "5": "5",
        "Weight Loss": "0",
        "weightloss": "0",
        "Building Muscle Mass": "1",
        "buildingmusclemass": "1",
        "Improving Metabolic Health": "2",
        "improvingmetabolichealth": "2",
        "Increasing Energy Levels": "3",
        "increasingenergylevels": "3",
        "Increasing Strength": "4",
        "increasingstrength": "4",
        "Improving Physical Endurance": "5",
        "improvingphysicalendurance": "5",
    }


def test_canonical_codes_pass_through():
    lookup = {
        "diet_preference": "1",
        "fresh_fruit_frequency": "0",
        "water_intake_frequency": "4",
        "exercise_level": "1",
    }
    result = normalize_questionnaire_lookup(lookup, option_reverse_map={
        "diet_preference": {"1": "1"},
        "fresh_fruit_frequency": {"0": "0"},
        "water_intake_frequency": {"4": "4"},
        "exercise_level": _exercise_level_map(),
    })
    assert result.diet_preference == "1"
    assert result.fresh_fruit_frequency == "0"
    assert result.water_intake_frequency == "4"
    assert result.exercise_level == "1"


def test_labels_resolve_to_option_codes_via_reverse_map():
    lookup = {
        "exercise_level": "Moderate-intensity",
        "food_groups": ["Fresh vegetables", "Whole grains"],
    }
    result = normalize_questionnaire_lookup(
        lookup,
        option_reverse_map={
            "exercise_level": _exercise_level_map(),
            "food_groups": _food_groups_map(),
        },
    )
    assert result.exercise_level == "1"
    assert result.food_groups == ("3", "0")


def test_scalar_health_priorities_code():
    result = normalize_questionnaire_lookup(
        {"health_priorities": "1"},
        option_reverse_map={"health_priorities": _health_priorities_map()},
    )
    assert result.health_priority_codes == ("1",)


def test_scalar_health_priorities_label():
    result = normalize_questionnaire_lookup(
        {"health_priorities": "Building Muscle Mass"},
        option_reverse_map={"health_priorities": _health_priorities_map()},
    )
    assert result.health_priority_codes == ("1",)


def test_legacy_list_health_priorities_preserves_up_to_two():
    result = normalize_questionnaire_lookup(
        {"health_priorities": ["0", "5", "1"]},
        option_reverse_map={"health_priorities": _health_priorities_map()},
    )
    assert result.health_priority_codes == ("0", "5")


def test_duplicate_health_priorities_are_deduplicated():
    result = normalize_questionnaire_lookup(
        {"health_priorities": ["1", "1", "Building Muscle Mass"]},
        option_reverse_map={"health_priorities": _health_priorities_map()},
    )
    assert result.health_priority_codes == ("1",)


def test_invalid_and_missing_health_priorities_yield_empty():
    missing = normalize_questionnaire_lookup({})
    assert missing.health_priority_codes == ()

    invalid = normalize_questionnaire_lookup(
        {"health_priorities": ["not-a-goal", None, ""]},
        option_reverse_map={"health_priorities": _health_priorities_map()},
    )
    assert invalid.health_priority_codes == ()


def test_health_priorities_label_map_works_without_reverse_map():
    # Uses questionnaire_field_config HEALTH_PRIORITIES_LABEL_TO_VALUE.
    result = normalize_questionnaire_lookup({"health_priorities": "weight loss"})
    assert result.health_priority_codes == ("0",)


def test_never_invents_second_goal_from_scalar():
    result = normalize_questionnaire_lookup({"health_priorities": "2"})
    assert result.health_priority_codes == ("2",)
    assert len(result.health_priority_codes) == 1


def test_scale_height_and_weight_normalization():
    lookup = {
        "height": {"value": 175.0, "unit": "0"},
        "weight": {"value": 70, "unit": "0"},
    }
    result = normalize_questionnaire_lookup(lookup, user_gender="female")
    assert result.height is not None
    assert result.height.value == 175
    assert result.height.unit == "cm"
    assert result.weight is not None
    assert result.weight.value == 70
    assert result.weight.unit == "kg"
    assert result.gender == "female"


def test_gender_from_lookup_overrides_user_gender():
    result = normalize_questionnaire_lookup(
        {"gender": "M"},
        user_gender="female",
    )
    assert result.gender == "male"


def test_multi_select_food_groups_dedupes_and_preserves_order():
    result = normalize_questionnaire_lookup(
        {"food_groups": ["3", "0", "3", "Fresh fruits"]},
        option_reverse_map={"food_groups": _food_groups_map()},
    )
    assert result.food_groups == ("3", "0", "4")


def test_missing_food_groups_is_none_not_empty_tuple():
    result = normalize_questionnaire_lookup({})
    assert result.food_groups is None


def test_empty_food_groups_list_is_empty_tuple():
    result = normalize_questionnaire_lookup({"food_groups": []})
    assert result.food_groups == ()


def test_vegetarian_red_meat_fallback_when_missing():
    # diet_preference 0 = Vegetarian → fallback "5"
    result = normalize_questionnaire_lookup(
        {"diet_preference": "0"},
        option_reverse_map={"diet_preference": {"0": "0"}},
    )
    assert result.diet_preference == "0"
    assert result.red_meat_frequency == "5"
    assert result.red_meat_frequency_defaulted is True


def test_jain_pescatarian_flexitarian_red_meat_fallback():
    for diet_code in ("3", "4", "5"):
        result = normalize_questionnaire_lookup(
            {"diet_preference": diet_code},
            option_reverse_map={"diet_preference": {diet_code: diet_code}},
        )
        assert result.red_meat_frequency == "5"
        assert result.red_meat_frequency_defaulted is True


def test_non_vegetarian_missing_red_meat_stays_none():
    result = normalize_questionnaire_lookup(
        {"diet_preference": "1"},
        option_reverse_map={"diet_preference": {"1": "1"}},
    )
    assert result.red_meat_frequency is None
    assert result.red_meat_frequency_defaulted is False


def test_explicit_red_meat_not_overridden_for_vegetarian():
    result = normalize_questionnaire_lookup(
        {"diet_preference": "0", "red_meat_frequency": "4"},
        option_reverse_map={
            "diet_preference": {"0": "0"},
            "red_meat_frequency": {"4": "4"},
        },
    )
    assert result.red_meat_frequency == "4"
    assert result.red_meat_frequency_defaulted is False


def test_missing_fields_remain_none():
    result = normalize_questionnaire_lookup({})
    assert result.diet_preference is None
    assert result.fresh_fruit_frequency is None
    assert result.water_intake_frequency is None
    assert result.height is None
    assert result.weight is None
    assert result.sickness_frequency is None
    assert result.exercise_frequency_week is None
    assert result.caffeine_type is None
    assert result.health_priority_codes == ()


def test_deterministic_output_for_same_input():
    lookup = {
        "health_priorities": ["Weight Loss", "5", "0"],
        "food_groups": ["Fresh vegetables", "1", "Fresh vegetables"],
        "exercise_level": "Moderate-intensity",
        "diet_preference": "0",
        "height": {"value": 170, "unit": "cm"},
        "gender": "female",
    }
    reverse = {
        "health_priorities": _health_priorities_map(),
        "food_groups": _food_groups_map(),
        "exercise_level": _exercise_level_map(),
        "diet_preference": {"0": "0"},
    }
    a = normalize_questionnaire_lookup(lookup, option_reverse_map=reverse)
    b = normalize_questionnaire_lookup(lookup, option_reverse_map=reverse)
    assert a == b
    assert isinstance(a, NormalizedAnswers)
    assert a.health_priority_codes == ("0", "5")
    assert a.food_groups == ("3", "1")
    assert a.exercise_level == "1"
    assert a.red_meat_frequency == "5"
    assert a.red_meat_frequency_defaulted is True


def test_normalizer_does_not_expose_score_or_intake_fields():
    result = normalize_questionnaire_lookup({"diet_preference": "1"})
    assert not hasattr(result, "nutrition_score")
    assert not hasattr(result, "risk_band")
    assert not hasattr(result, "protein_intake")
    assert not hasattr(result, "general_quality")
