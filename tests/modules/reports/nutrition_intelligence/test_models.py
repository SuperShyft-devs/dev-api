"""Phase 1 tests: Nutrition Intelligence Engine models."""

from __future__ import annotations

import pytest

from modules.reports.nutrition_intelligence.models import (
    GOAL_IDS,
    PRIORITY_LEVELS,
    QUESTIONNAIRE_GOAL_CODE_TO_ID,
    CombinedNutritionProfile,
    GoalProfile,
    IndicatorDefinition,
    IndicatorScore,
    NutritionResult,
    NutritionTargets,
    ScoreBand,
    ScoringConfig,
    TargetRange,
)


def test_goal_ids_cover_six_product_goals():
    assert GOAL_IDS == (
        "weight_loss",
        "muscle_gain",
        "energy_levels",
        "metabolic_health",
        "endurance",
        "strength",
    )


def test_questionnaire_goal_code_mapping_is_bijective_for_six_codes():
    assert set(QUESTIONNAIRE_GOAL_CODE_TO_ID.keys()) == {"0", "1", "2", "3", "4", "5"}
    assert set(QUESTIONNAIRE_GOAL_CODE_TO_ID.values()) == set(GOAL_IDS)
    # Codes match seed option_values for health_priorities.
    assert QUESTIONNAIRE_GOAL_CODE_TO_ID["0"] == "weight_loss"
    assert QUESTIONNAIRE_GOAL_CODE_TO_ID["1"] == "muscle_gain"
    assert QUESTIONNAIRE_GOAL_CODE_TO_ID["2"] == "metabolic_health"
    assert QUESTIONNAIRE_GOAL_CODE_TO_ID["3"] == "energy_levels"
    assert QUESTIONNAIRE_GOAL_CODE_TO_ID["4"] == "strength"
    assert QUESTIONNAIRE_GOAL_CODE_TO_ID["5"] == "endurance"


def test_priority_levels_include_approved_labels():
    assert PRIORITY_LEVELS == ("very_high", "high", "medium", "low", "none")


def test_target_range_is_target_not_intake_container():
    tr = TargetRange(low=1.6, high=2.2, unit="g_per_kg_day", mode="target_only")
    assert tr.low == 1.6
    assert tr.high == 2.2
    assert tr.unit == "g_per_kg_day"


def test_goal_profile_defaults_are_immutable_friendly():
    profile = GoalProfile(
        id="weight_loss",
        questionnaire_code="0",
        display_name="Weight Loss",
        base_target_keys=("protein_g_per_kg_weight_loss",),
        priority_levels={"hydration": "medium"},
        food_quality_priorities=("vegetables",),
        conflict_tags=("energy_surplus_sensitive",),
        notes=("Energy deficit is a target concept only.",),
    )
    assert profile.id == "weight_loss"
    assert profile.priority_levels["hydration"] == "medium"
    with pytest.raises(Exception):
        profile.display_name = "x"  # type: ignore[misc]


def test_indicator_definition_documents_behavioural_proxy_defaults():
    indicator = IndicatorDefinition(
        id="protein_supporting_foods",
        display_name="Protein-supporting pattern",
        source_fields=("food_groups", "diet_preference"),
        score_direction="higher_better",
        general_quality_priority="medium",
        goal_relevance={
            "weight_loss": "high",
            "muscle_gain": "very_high",
            "energy_levels": "medium",
            "metabolic_health": "medium",
            "endurance": "high",
            "strength": "very_high",
        },
        description=(
            "Presence/diversity of reported protein-supporting food groups, "
            "NOT protein adequacy or protein intake."
        ),
        uses_diet_preference_context=True,
    )
    assert indicator.is_behavioural_proxy is True
    assert indicator.uses_diet_preference_context is True
    assert "NOT protein" in indicator.description


def test_scoring_config_keeps_blend_weights_explicit():
    scoring = ScoringConfig(
        general_quality_weight=0.7,
        goal_alignment_weight=0.3,
        zero_goal_mode="renormalize_quality_only",
        missing_indicator_policy="exclude_and_renormalize",
        priority_level_weights={
            "very_high": 1.0,
            "high": 0.75,
            "medium": 0.5,
            "low": 0.25,
            "none": 0.0,
        },
        score_bands=(
            ScoreBand(min_score=80, label="Excellent"),
            ScoreBand(min_score=0, label="At Risk"),
        ),
    )
    assert scoring.general_quality_weight == 0.7
    assert scoring.goal_alignment_weight == 0.3
    assert abs(scoring.general_quality_weight + scoring.goal_alignment_weight - 1.0) < 1e-9


def test_nutrition_result_polarity_fields_exist_without_intake_estimates():
    result = NutritionResult(
        general_quality=70.0,
        goal_alignment=60.0,
        nutrition_score=67.0,
        risk_band="Healthy",
        goals=("muscle_gain",),
        indicators=(
            IndicatorScore(
                indicator_id="hydration",
                score=75.0,
                source_fields=("water_intake_frequency",),
            ),
        ),
        targets=NutritionTargets(
            protein_g_per_kg=TargetRange(low=1.6, high=2.2, unit="g_per_kg_day"),
            notes=("TARGET only",),
        ),
        general_quality_weight=0.7,
        goal_alignment_weight=0.3,
    )
    assert result.nutrition_score == 67.0
    assert not hasattr(result, "estimated_protein_intake")
    assert result.targets.protein_g_per_kg is not None


def test_combined_profile_supports_zero_one_two_goals():
    empty = CombinedNutritionProfile(goals=(), compatibility=None, priority_levels={}, target_keys=())
    one = CombinedNutritionProfile(
        goals=("energy_levels",),
        compatibility=None,
        priority_levels={"meal_regularity": "very_high"},
        target_keys=("hydration_standard",),
    )
    two = CombinedNutritionProfile(
        goals=("weight_loss", "endurance"),
        compatibility="moderate",
        priority_levels={"hydration": "very_high"},
        target_keys=("carbohydrate_endurance_activity_dependent",),
        conflict_resolutions=("resolve_energy_conflict",),
    )
    assert len(empty.goals) == 0
    assert len(one.goals) == 1
    assert len(two.goals) == 2
