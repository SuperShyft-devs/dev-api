"""Internal data models for the Nutrition Intelligence Engine.

Phase 1 models only. No scoring logic, no I/O, no FastAPI/SQLAlchemy.

Conventions:
- Higher ``nutrition_score`` = better nutrition *behaviour* quality.
- Targets (g/kg, hydration priority, etc.) are never measured intake.
- Indicators are behavioural proxies derived from questionnaire answers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

GoalId = Literal[
    "weight_loss",
    "muscle_gain",
    "energy_levels",
    "metabolic_health",
    "endurance",
    "strength",
]

GOAL_IDS: tuple[GoalId, ...] = (
    "weight_loss",
    "muscle_gain",
    "energy_levels",
    "metabolic_health",
    "endurance",
    "strength",
)

# Questionnaire ``health_priorities`` option_value → GoalId
QUESTIONNAIRE_GOAL_CODE_TO_ID: dict[str, GoalId] = {
    "0": "weight_loss",
    "1": "muscle_gain",
    "2": "metabolic_health",
    "3": "energy_levels",
    "4": "strength",
    "5": "endurance",
}

PriorityLevel = Literal["very_high", "high", "medium", "low", "none"]

PRIORITY_LEVELS: tuple[PriorityLevel, ...] = (
    "very_high",
    "high",
    "medium",
    "low",
    "none",
)

ScoreDirection = Literal["higher_better", "lower_better"]

CompatibilityLevel = Literal["high", "moderate", "low"]

ZeroGoalMode = Literal["renormalize_quality_only", "use_neutral_alignment"]

MissingIndicatorPolicy = Literal["exclude_and_renormalize", "treat_as_neutral"]


@dataclass(frozen=True)
class TargetRange:
    """A target range (what the user should aim for), NOT measured intake."""

    low: float | None = None
    high: float | None = None
    unit: str | None = None
    mode: str | None = None


@dataclass(frozen=True)
class GoalProfile:
    """Configuration-backed profile for one nutrition/activity goal."""

    id: GoalId
    questionnaire_code: str
    display_name: str
    base_target_keys: tuple[str, ...] = ()
    priority_levels: dict[str, PriorityLevel] = field(default_factory=dict)
    food_quality_priorities: tuple[str, ...] = ()
    activity_dependencies: dict[str, object] = field(default_factory=dict)
    conflict_tags: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class IndicatorDefinition:
    """One observable behavioural proxy used for scoring.

    Scores reflect questionnaire behaviour quality, not nutrient adequacy.
    """

    id: str
    display_name: str
    source_fields: tuple[str, ...]
    score_direction: ScoreDirection
    general_quality_priority: PriorityLevel
    goal_relevance: dict[GoalId, PriorityLevel]
    description: str
    is_behavioural_proxy: bool = True
    uses_diet_preference_context: bool = False
    ordinal_scores: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ScoreBand:
    """Configurable label band for a 0–100 score (higher = better)."""

    min_score: float
    label: str


@dataclass(frozen=True)
class ScoringConfig:
    """Score blend and band configuration (all values from YAML)."""

    general_quality_weight: float
    goal_alignment_weight: float
    zero_goal_mode: ZeroGoalMode
    missing_indicator_policy: MissingIndicatorPolicy
    priority_level_weights: dict[PriorityLevel, float]
    score_bands: tuple[ScoreBand, ...]
    neutral_alignment_score: float = 50.0


@dataclass(frozen=True)
class CombinationRule:
    """Pairwise overlay for the generic goal-combination engine."""

    goal_a: GoalId
    goal_b: GoalId
    compatibility: CompatibilityLevel
    target_resolution: dict[str, str] = field(default_factory=dict)
    priority_resolution: str = "max_of_shared"
    conflict_severity: str | None = None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class NutritionTargets:
    """Target outputs only — never treated as measured intake."""

    protein_g_per_kg: TargetRange | None = None
    carbohydrate_g_per_kg: TargetRange | None = None
    fibre_priority: str | None = None
    hydration_priority: str | None = None
    energy_concept: str | None = None
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class IndicatorScore:
    """Scored behavioural proxy (0–100). Not nutrient intake."""

    indicator_id: str
    score: float
    source_fields: tuple[str, ...]
    is_behavioural_proxy: bool = True


@dataclass(frozen=True)
class NutritionResult:
    """Internal engine result before legacy output adaptation.

    ``nutrition_score`` polarity: higher = better behaviour quality.
    Does not include fabricated macro intake estimates.
    """

    general_quality: float
    goal_alignment: float | None
    nutrition_score: float
    risk_band: str
    goals: tuple[GoalId, ...]
    indicators: tuple[IndicatorScore, ...]
    targets: NutritionTargets
    general_quality_weight: float
    goal_alignment_weight: float


@dataclass(frozen=True)
class CombinedNutritionProfile:
    """Merged profile produced by the generic combination engine (later phases)."""

    goals: tuple[GoalId, ...]
    compatibility: CompatibilityLevel | None
    priority_levels: dict[str, PriorityLevel]
    target_keys: tuple[str, ...]
    conflict_resolutions: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScaleValue:
    """Canonical scale answer (value + unit). Not an intake estimate."""

    value: float | int | None
    unit: str | None = None


@dataclass(frozen=True)
class NormalizedAnswers:
    """Canonical questionnaire representation after Input Normalization.

    Contains option codes / scale values only. Does not score behaviours,
    invent goals, or fabricate intake/target values.
    """

    health_priority_codes: tuple[str, ...]
    gender: str | None = None
    height: ScaleValue | None = None
    weight: ScaleValue | None = None
    diet_preference: str | None = None
    food_groups: tuple[str, ...] | None = None
    healthy_breakfast_frequency: str | None = None
    fresh_fruit_frequency: str | None = None
    fresh_vegetable_frequency: str | None = None
    baked_goods_frequency: str | None = None
    dessert_frequency: str | None = None
    butter_dish_frequency: str | None = None
    red_meat_frequency: str | None = None
    red_meat_frequency_defaulted: bool = False
    extra_salt_frequency: str | None = None
    iodized_salt_status: str | None = None
    caffeine_frequency: str | None = None
    caffeine_type: tuple[str, ...] | None = None
    water_intake_frequency: str | None = None
    sickness_frequency: str | None = None
    exercise_frequency_week: str | None = None
    exercise_level: str | None = None
    physical_activity_frequency: str | None = None
    daily_active_duration: str | None = None
    sleeping_hours: str | None = None
    alcohol_frequency: str | None = None
    tobacco_frequency: str | None = None
    goal_preference: str | None = None
    weight_loss_goal: ScaleValue | None = None
