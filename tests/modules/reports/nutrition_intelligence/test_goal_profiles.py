"""Phase 5 tests: Nutrition Intelligence Engine goal profile loader."""

from __future__ import annotations

import pytest

from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import GOAL_IDS, GoalProfile


def test_all_six_goals_load_successfully():
    profiles = load_goal_profiles(list(GOAL_IDS))
    assert len(profiles) == 6
    assert [p.id for p in profiles] == list(GOAL_IDS)
    assert all(isinstance(p, GoalProfile) for p in profiles)


def test_one_goal_returns_one_profile():
    profiles = load_goal_profiles(["muscle_gain"])
    assert len(profiles) == 1
    assert profiles[0].id == "muscle_gain"
    assert isinstance(profiles[0], GoalProfile)


def test_two_goals_return_two_profiles_in_same_order():
    profiles = load_goal_profiles(["weight_loss", "endurance"])
    assert [p.id for p in profiles] == ["weight_loss", "endurance"]


def test_zero_goals_returns_empty_list():
    assert load_goal_profiles([]) == []
    assert load_goal_profiles(None) == []


def test_profile_values_match_goals_yaml():
    config = load_nutrition_engine_config()
    profiles = load_goal_profiles(["weight_loss", "strength"], config=config)

    wl = profiles[0]
    assert wl.id == "weight_loss"
    assert wl.questionnaire_code == "0"
    assert wl.display_name == "Weight Loss"
    assert "protein_g_per_kg_weight_loss" in wl.base_target_keys
    assert wl.priority_levels["protein_supporting_foods"] == "high"
    assert wl.priority_levels["dessert_sugar_control"] == "high"
    assert "vegetables" in wl.food_quality_priorities
    assert "energy_surplus_sensitive" in wl.conflict_tags
    assert wl.activity_dependencies == {}

    strength = profiles[1]
    assert strength.questionnaire_code == "4"
    assert strength.priority_levels["protein_supporting_foods"] == "very_high"
    assert strength.priority_levels["hydration"] == "high"
    assert "protein_g_per_kg_strength" in strength.base_target_keys

    # Matches config object loaded from goals.yaml
    assert wl.base_target_keys == config.goals["weight_loss"].base_target_keys
    assert strength.priority_levels == config.goals["strength"].priority_levels


def test_endurance_activity_dependencies_come_from_yaml():
    profile = load_goal_profiles(["endurance"])[0]
    assert "carbohydrate_g_per_kg" in profile.activity_dependencies
    dep = profile.activity_dependencies["carbohydrate_g_per_kg"]
    assert isinstance(dep, dict)
    assert dep.get("modifies_target_key") == "carbohydrate_endurance_activity_dependent"


def test_unknown_goal_ids_are_skipped():
    profiles = load_goal_profiles(["weight_loss", "not_a_goal", "endurance", "???"])
    assert [p.id for p in profiles] == ["weight_loss", "endurance"]


def test_no_mutation_of_loaded_configuration():
    config = load_nutrition_engine_config()
    original_priority = dict(config.goals["metabolic_health"].priority_levels)
    original_activity = dict(config.goals["endurance"].activity_dependencies)

    profiles = load_goal_profiles(["metabolic_health", "endurance"], config=config)
    profiles[0].priority_levels["vegetable_intake"] = "none"
    profiles[1].activity_dependencies.clear()

    assert config.goals["metabolic_health"].priority_levels == original_priority
    assert config.goals["endurance"].activity_dependencies == original_activity
    assert config.goals["metabolic_health"].priority_levels["vegetable_intake"] == "very_high"


def test_returned_objects_have_expected_goal_profile_type():
    profiles = load_goal_profiles(["energy_levels"])
    assert len(profiles) == 1
    profile = profiles[0]
    assert type(profile) is GoalProfile
    assert profile.id == "energy_levels"
    assert profile.questionnaire_code == "3"
    assert profile.priority_levels["meal_regularity"] == "very_high"


def test_does_not_score_or_combine_goals():
    profiles = load_goal_profiles(["weight_loss", "muscle_gain"])
    # Two separate profiles — no merged CombinedNutritionProfile / score fields.
    assert len(profiles) == 2
    assert not hasattr(profiles[0], "nutrition_score")
    assert not hasattr(profiles[0], "combined_priorities")
    with pytest.raises(AttributeError):
        _ = profiles[0].general_quality  # type: ignore[attr-defined]
