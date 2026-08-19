"""Phase 2 tests: Nutrition Intelligence Engine configuration loading."""

from __future__ import annotations

from itertools import combinations

import pytest

from modules.reports.nutrition_intelligence import (
    default_config_dir,
    load_nutrition_engine_config,
)
from modules.reports.nutrition_intelligence.models import GOAL_IDS


EXPECTED_INDICATORS = (
    "vegetable_intake",
    "fruit_intake",
    "food_diversity",
    "dessert_sugar_control",
    "baked_goods_control",
    "hydration",
    "protein_supporting_foods",
    "meal_regularity",
    "sodium_control",
)


def test_default_config_dir_exists_and_contains_required_files():
    root = default_config_dir()
    assert root.is_dir()
    for name in (
        "goals.yaml",
        "combinations.yaml",
        "indicators.yaml",
        "scoring.yaml",
        "targets.yaml",
    ):
        assert (root / name).is_file(), name


def test_load_nutrition_engine_config_succeeds():
    config = load_nutrition_engine_config()
    assert config.config_dir == default_config_dir()
    assert set(config.goals.keys()) == set(GOAL_IDS)
    assert set(config.indicators.keys()) == set(EXPECTED_INDICATORS)


def test_scoring_blend_is_configurable_seventy_thirty_default():
    scoring = load_nutrition_engine_config().scoring
    assert scoring.general_quality_weight == pytest.approx(0.70)
    assert scoring.goal_alignment_weight == pytest.approx(0.30)
    assert scoring.zero_goal_mode == "renormalize_quality_only"
    assert scoring.missing_indicator_policy == "exclude_and_renormalize"
    assert scoring.priority_level_weights["very_high"] == 1.0
    assert scoring.priority_level_weights["none"] == 0.0
    # Higher score = better: bands sorted descending by min_score.
    assert scoring.score_bands[0].min_score >= scoring.score_bands[-1].min_score
    labels = [b.label for b in scoring.score_bands]
    assert "Healthy" in labels
    assert "At Risk" in labels


def test_goals_map_questionnaire_codes_uniquely():
    goals = load_nutrition_engine_config().goals
    codes = [g.questionnaire_code for g in goals.values()]
    assert sorted(codes) == ["0", "1", "2", "3", "4", "5"]
    assert goals["weight_loss"].questionnaire_code == "0"
    assert goals["muscle_gain"].questionnaire_code == "1"
    assert goals["metabolic_health"].questionnaire_code == "2"
    assert goals["energy_levels"].questionnaire_code == "3"
    assert goals["strength"].questionnaire_code == "4"
    assert goals["endurance"].questionnaire_code == "5"


def test_indicator_table_matches_approved_general_and_goal_priorities():
    config = load_nutrition_engine_config()
    indicators = config.indicators

    assert indicators["vegetable_intake"].general_quality_priority == "high"
    assert indicators["vegetable_intake"].goal_relevance["metabolic_health"] == "very_high"

    assert indicators["fruit_intake"].general_quality_priority == "high"
    assert indicators["fruit_intake"].goal_relevance["metabolic_health"] == "very_high"
    assert indicators["fruit_intake"].goal_relevance["weight_loss"] == "medium"

    assert indicators["food_diversity"].general_quality_priority == "high"
    for goal_id in GOAL_IDS:
        assert indicators["food_diversity"].goal_relevance[goal_id] == "high"

    assert indicators["dessert_sugar_control"].general_quality_priority == "high"
    assert indicators["dessert_sugar_control"].goal_relevance["weight_loss"] == "high"
    assert indicators["dessert_sugar_control"].goal_relevance["metabolic_health"] == "high"

    assert indicators["baked_goods_control"].general_quality_priority == "high"
    assert indicators["baked_goods_control"].goal_relevance["weight_loss"] == "high"

    assert indicators["hydration"].general_quality_priority == "high"
    assert indicators["hydration"].goal_relevance["endurance"] == "very_high"
    assert "NOT scientifically measured litres" in indicators["hydration"].description

    assert indicators["protein_supporting_foods"].general_quality_priority == "medium"
    assert indicators["protein_supporting_foods"].uses_diet_preference_context is True
    assert indicators["protein_supporting_foods"].goal_relevance["muscle_gain"] == "very_high"
    assert indicators["protein_supporting_foods"].goal_relevance["strength"] == "very_high"
    assert "NOT protein adequacy" in indicators["protein_supporting_foods"].description

    assert indicators["meal_regularity"].general_quality_priority == "medium"
    assert indicators["meal_regularity"].goal_relevance["energy_levels"] == "very_high"

    assert indicators["sodium_control"].general_quality_priority == "medium"
    assert indicators["sodium_control"].goal_relevance["metabolic_health"] == "very_high"
    assert indicators["sodium_control"].goal_relevance["strength"] == "low"


def test_goal_priority_levels_align_with_indicator_goal_relevance():
    config = load_nutrition_engine_config()
    for indicator_id, indicator in config.indicators.items():
        for goal_id, goal in config.goals.items():
            assert goal.priority_levels[indicator_id] == indicator.goal_relevance[goal_id]


def test_goal_base_target_keys_resolve_in_targets_yaml():
    config = load_nutrition_engine_config()
    for goal in config.goals.values():
        for key in goal.base_target_keys:
            assert key in config.targets
            assert config.targets[key].kind


def test_targets_are_marked_as_targets_not_intake():
    targets = load_nutrition_engine_config().targets
    protein = targets["protein_g_per_kg_muscle_gain"]
    assert protein.range is not None
    assert protein.range.low == 1.6
    assert protein.range.high == 2.2
    assert any("TARGET" in note or "Not measured" in note for note in protein.notes)

    endurance_carb = targets["carbohydrate_endurance_activity_dependent"]
    assert endurance_carb.range is not None
    assert endurance_carb.range.mode == "activity_dependent"
    assert "high" in endurance_carb.activity_bands
    assert any("must not inflate" in note.lower() for note in endurance_carb.notes)


def test_combination_pairs_cover_all_fifteen_two_goal_combinations():
    config = load_nutrition_engine_config()
    expected = {tuple(sorted(pair)) for pair in combinations(GOAL_IDS, 2)}
    assert len(expected) == 15
    actual = {(rule.goal_a, rule.goal_b) for rule in config.combination_pairs}
    assert actual == expected


def test_combination_defaults_exist_for_generic_engine():
    defaults = load_nutrition_engine_config().combination_defaults
    assert defaults.compatibility in {"high", "moderate", "low"}
    assert defaults.priority_resolution
    assert "protein_g_per_kg" in defaults.target_resolution


def test_protein_supporting_and_hydration_indicators_exclude_intake_claims():
    indicators = load_nutrition_engine_config().indicators
    protein = indicators["protein_supporting_foods"]
    assert protein.is_behavioural_proxy is True
    assert "diet_preference" in protein.source_fields
    assert "food_groups" in protein.source_fields

    hydration = indicators["hydration"]
    assert hydration.source_fields == ("water_intake_frequency",)
    assert "litres" in hydration.description.lower()


def test_missing_config_dir_raises():
    with pytest.raises(FileNotFoundError):
        load_nutrition_engine_config(config_dir=default_config_dir() / "does-not-exist")
