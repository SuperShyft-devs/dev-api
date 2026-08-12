"""Nutrition Intelligence Engine (Phases 1–3: models, configuration, normalizer).

Pure Python core. Configuration + normalization have no FastAPI route wiring.
"""

from __future__ import annotations

from modules.reports.nutrition_intelligence.config_loader import (
    NutritionEngineConfig,
    default_config_dir,
    load_nutrition_engine_config,
)
from modules.reports.nutrition_intelligence.models import (
    CombinationRule,
    GoalId,
    GoalProfile,
    IndicatorDefinition,
    IndicatorScore,
    NormalizedAnswers,
    NutritionResult,
    NutritionTargets,
    PriorityLevel,
    ScaleValue,
    ScoreBand,
    ScoringConfig,
    TargetRange,
)
from modules.reports.nutrition_intelligence.normalizer import normalize_questionnaire_lookup

__all__ = [
    "CombinationRule",
    "GoalId",
    "GoalProfile",
    "IndicatorDefinition",
    "IndicatorScore",
    "NormalizedAnswers",
    "NutritionEngineConfig",
    "NutritionResult",
    "NutritionTargets",
    "PriorityLevel",
    "ScaleValue",
    "ScoreBand",
    "ScoringConfig",
    "TargetRange",
    "default_config_dir",
    "load_nutrition_engine_config",
    "normalize_questionnaire_lookup",
]
