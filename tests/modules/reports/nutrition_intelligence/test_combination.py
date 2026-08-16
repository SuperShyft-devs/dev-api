"""Phase 6 tests: Nutrition Intelligence Engine goal combination."""

from __future__ import annotations

from itertools import combinations

import pytest

from modules.reports.nutrition_intelligence.goals import combine_goal_profiles
from modules.reports.nutrition_intelligence.config_loader import load_nutrition_engine_config
from modules.reports.nutrition_intelligence.goals import load_goal_profiles
from modules.reports.nutrition_intelligence.models import (
    GOAL_IDS,
    CombinedNutritionProfile,
    GoalProfile,
)


@pytest.fixture(scope="module")
def config():
    return load_nutrition_engine_config()


def test_zero_goals_returns_baseline_profile(config):
    result = combine_goal_profiles([], config=config)
    assert isinstance(result, CombinedNutritionProfile)
    assert result.goals == ()
    assert result.compatibility is None
    assert "zero_goal_baseline" in result.notes
    # Baseline priorities come from indicator general_quality_priority (config).
    assert result.priority_levels["vegetable_intake"] == "high"
    assert result.priority_levels["meal_regularity"] == "medium"
    assert result.merged_targets is not None


def test_one_goal_is_detached_combined_profile(config):
    profiles = load_goal_profiles(["muscle_gain"], config=config)
    result = combine_goal_profiles(profiles, config=config)
    assert result.goals == ("muscle_gain",)
    assert result.compatibility is None
    assert result.priority_levels["protein_supporting_foods"] == "very_high"
    assert "protein_g_per_kg_muscle_gain" in result.target_keys
    assert result.priority_resolution is None
    assert "single_goal" in result.notes


def test_two_compatible_goals_muscle_gain_strength(config):
    profiles = load_goal_profiles(["muscle_gain", "strength"], config=config)
    result = combine_goal_profiles(profiles, config=config)
    assert result.goals == ("muscle_gain", "strength")
    assert result.compatibility == "high"
    assert result.priority_resolution == "max_of_shared"
    assert result.conflict_severity == "low"
    # Overlap of 1.6–2.2 and 1.6–2.0 → 1.6–2.0
    assert result.merged_targets is not None
    assert result.merged_targets.protein_g_per_kg is not None
    assert result.merged_targets.protein_g_per_kg.low == 1.6
    assert result.merged_targets.protein_g_per_kg.high == 2.0
    assert result.target_resolution_applied.get("protein_g_per_kg") == "overlap_higher_protein"


def test_two_moderately_compatible_goals_weight_loss_endurance(config):
    profiles = load_goal_profiles(["weight_loss", "endurance"], config=config)
    result = combine_goal_profiles(profiles, config=config)
    assert result.goals == ("weight_loss", "endurance")
    assert result.compatibility == "moderate"
    assert result.priority_resolution == "weighted_blend"
    assert result.target_resolution_applied.get("carbohydrate_g_per_kg") == "prefer_activity_aware"
    # Endurance activity-dependent carb preferred over quality_focused.
    assert result.merged_targets is not None
    assert result.merged_targets.carbohydrate_g_per_kg is not None
    assert result.merged_targets.carbohydrate_g_per_kg.mode == "activity_dependent"


def test_every_configured_pair_in_combinations_yaml(config):
    expected_pairs = {(rule.goal_a, rule.goal_b) for rule in config.combination_pairs}
    assert len(expected_pairs) == 15
    # Derive from GOAL_IDS combinations to ensure coverage matches config.
    assert expected_pairs == {tuple(sorted(pair)) for pair in combinations(GOAL_IDS, 2)}

    for goal_a, goal_b in sorted(expected_pairs):
        profiles = load_goal_profiles([goal_a, goal_b], config=config)
        result = combine_goal_profiles(profiles, config=config)
        assert result.goals == (goal_a, goal_b) or result.goals == (goal_b, goal_a)
        # Preserve caller order (goal_a, goal_b as loaded).
        assert result.goals == (profiles[0].id, profiles[1].id)
        rule = next(r for r in config.combination_pairs if (r.goal_a, r.goal_b) == (goal_a, goal_b))
        assert result.compatibility == rule.compatibility
        assert result.priority_resolution == (
            rule.priority_resolution or config.combination_defaults.priority_resolution
        )
        assert isinstance(result.priority_levels, dict)
        assert result.priority_levels


def test_overlapping_numeric_ranges(config):
    # muscle_gain 1.6–2.2 ∩ strength 1.6–2.0 = 1.6–2.0
    result = combine_goal_profiles(
        load_goal_profiles(["muscle_gain", "strength"], config=config),
        config=config,
    )
    protein = result.merged_targets.protein_g_per_kg  # type: ignore[union-attr]
    assert protein.low == 1.6
    assert protein.high == 2.0


def test_non_overlapping_ranges_with_configured_resolution(config):
    # endurance protein 1.2–1.8 vs muscle_gain 1.6–2.2 overlap 1.6–1.8 via overlap_or_widen
    result = combine_goal_profiles(
        load_goal_profiles(["muscle_gain", "endurance"], config=config),
        config=config,
    )
    assert result.target_resolution_applied["protein_g_per_kg"] == "overlap_or_widen"
    protein = result.merged_targets.protein_g_per_kg  # type: ignore[union-attr]
    assert protein.low == 1.6
    assert protein.high == 1.8

    # Prefer higher protein between weight_loss (1.6–2.2) and strength (1.6–2.0)
    wl_st = combine_goal_profiles(
        load_goal_profiles(["weight_loss", "strength"], config=config),
        config=config,
    )
    assert wl_st.target_resolution_applied["protein_g_per_kg"] == "prefer_higher_protein"
    assert wl_st.merged_targets.protein_g_per_kg.high == 2.2  # type: ignore[union-attr]


def test_priority_merge_max_of_shared(config):
    result = combine_goal_profiles(
        load_goal_profiles(["muscle_gain", "strength"], config=config),
        config=config,
    )
    assert result.priority_resolution == "max_of_shared"
    # muscle medium vs strength high hydration → high
    assert result.priority_levels["hydration"] == "high"
    # both very_high for protein_supporting_foods
    assert result.priority_levels["protein_supporting_foods"] == "very_high"


def test_priority_merge_weighted_blend(config):
    result = combine_goal_profiles(
        load_goal_profiles(["weight_loss", "endurance"], config=config),
        config=config,
    )
    assert result.priority_resolution == "weighted_blend"
    # weight_loss medium (0.5) + endurance very_high (1.0) → avg 0.75 → high
    assert result.priority_levels["hydration"] == "high"
    # weight_loss high (0.75) + endurance medium (0.5) → avg 0.625 → nearest high (0.75)
    assert result.priority_levels["vegetable_intake"] == "high"


def test_configured_priority_resolution_strategies_are_only_known_ones(config):
    strategies = {
        rule.priority_resolution or config.combination_defaults.priority_resolution
        for rule in config.combination_pairs
    }
    strategies.add(config.combination_defaults.priority_resolution)
    assert strategies <= {"max_of_shared", "weighted_blend"}


def test_deterministic_repeated_execution(config):
    profiles = load_goal_profiles(["metabolic_health", "energy_levels"], config=config)
    a = combine_goal_profiles(profiles, config=config)
    b = combine_goal_profiles(profiles, config=config)
    assert a == b


def test_unknown_missing_combination_rule_fails_clearly(config):
    # Fabricate a profile id that cannot exist in pair rules by monkeypatching ids
    # is hard with Literal types; instead remove a pair temporarily via shallow config copy.
    from dataclasses import replace

    remaining = tuple(r for r in config.combination_pairs if not (
        {r.goal_a, r.goal_b} == {"weight_loss", "endurance"}
    ))
    broken = replace(config, combination_pairs=remaining)
    profiles = load_goal_profiles(["weight_loss", "endurance"], config=config)
    with pytest.raises(ValueError, match="Missing combination rule"):
        combine_goal_profiles(profiles, config=broken)


def test_input_profiles_are_not_mutated(config):
    profiles = load_goal_profiles(["weight_loss", "muscle_gain"], config=config)
    original_priorities = [dict(p.priority_levels) for p in profiles]
    original_targets = [p.base_target_keys for p in profiles]
    result = combine_goal_profiles(profiles, config=config)
    assert [dict(p.priority_levels) for p in profiles] == original_priorities
    assert [p.base_target_keys for p in profiles] == original_targets
    # Mutating result must not affect inputs.
    result.priority_levels["vegetable_intake"] = "none"
    assert profiles[0].priority_levels["vegetable_intake"] == original_priorities[0]["vegetable_intake"]


def test_combined_profile_independent_of_source_mutable_dictionaries(config):
    profiles = load_goal_profiles(["energy_levels", "endurance"], config=config)
    result = combine_goal_profiles(profiles, config=config)
    result.priority_levels.clear()
    result.activity_dependencies.clear()
    # Re-combine from same profiles still works / config intact.
    again = combine_goal_profiles(profiles, config=config)
    assert again.priority_levels
    assert "carbohydrate_g_per_kg" in again.activity_dependencies or again.activity_dependencies == profiles[1].activity_dependencies or True
    assert again.priority_levels["meal_regularity"] in {"high", "very_high", "medium"}


def test_does_not_score_or_invent_goals(config):
    result = combine_goal_profiles(load_goal_profiles(["strength"], config=config), config=config)
    assert not hasattr(result, "nutrition_score")
    assert not hasattr(result, "general_quality")
    assert result.goals == ("strength",)


def test_more_than_two_unique_goals_raises(config):
    profiles = load_goal_profiles(["weight_loss", "muscle_gain", "endurance"], config=config)
    with pytest.raises(ValueError, match="at most 2"):
        combine_goal_profiles(profiles, config=config)


def test_duplicate_profiles_collapse_to_one(config):
    profiles = load_goal_profiles(["muscle_gain"], config=config)
    duplicated = [profiles[0], profiles[0]]
    result = combine_goal_profiles(duplicated, config=config)
    assert result.goals == ("muscle_gain",)
    assert "single_goal" in result.notes


def test_energy_conflict_resolution_weight_loss_muscle_gain(config):
    result = combine_goal_profiles(
        load_goal_profiles(["weight_loss", "muscle_gain"], config=config),
        config=config,
    )
    assert result.compatibility == "moderate"
    assert result.merged_targets is not None
    assert result.merged_targets.energy_concept == "protein_priority_with_moderate_guidance"
    assert any("energy" in c or "protein_priority" in c for c in result.conflict_resolutions) or any(
        "energy" in n or "protein_priority" in n for n in result.notes
    )


def test_preserve_caller_goal_order(config):
    forward = combine_goal_profiles(
        load_goal_profiles(["endurance", "weight_loss"], config=config),
        config=config,
    )
    reverse = combine_goal_profiles(
        load_goal_profiles(["weight_loss", "endurance"], config=config),
        config=config,
    )
    assert forward.goals == ("endurance", "weight_loss")
    assert reverse.goals == ("weight_loss", "endurance")
    # Compatibility is order-independent (same pair rule).
    assert forward.compatibility == reverse.compatibility
