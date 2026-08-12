"""Load Nutrition Intelligence Engine YAML configuration into typed models.

Pure Python. Configuration is the source of science/product values;
this module only validates and maps YAML → dataclasses.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from modules.reports.nutrition_intelligence.models import (
    GOAL_IDS,
    PRIORITY_LEVELS,
    CombinationRule,
    CompatibilityLevel,
    GoalId,
    GoalProfile,
    IndicatorDefinition,
    MissingIndicatorPolicy,
    PriorityLevel,
    ScoreBand,
    ScoreDirection,
    ScoringConfig,
    TargetRange,
    ZeroGoalMode,
)

_CONFIG_FILENAMES = (
    "goals.yaml",
    "combinations.yaml",
    "indicators.yaml",
    "scoring.yaml",
    "targets.yaml",
)


@dataclass(frozen=True)
class TargetDefinition:
    """One named target entry from targets.yaml (TARGET only, not intake)."""

    key: str
    kind: str
    range: TargetRange | None
    priority: str | None
    concept: str | None
    activity_bands: dict[str, TargetRange]
    notes: tuple[str, ...]


@dataclass(frozen=True)
class CombinationDefaults:
    compatibility: CompatibilityLevel
    priority_resolution: str
    target_resolution: dict[str, str]
    conflict_severity: str


@dataclass(frozen=True)
class NutritionEngineConfig:
    """Fully loaded engine configuration."""

    goals: dict[GoalId, GoalProfile]
    indicators: dict[str, IndicatorDefinition]
    scoring: ScoringConfig
    combination_defaults: CombinationDefaults
    combination_pairs: tuple[CombinationRule, ...]
    combination_tag_rules: tuple[dict[str, Any], ...]
    targets: dict[str, TargetDefinition]
    config_dir: Path


def default_config_dir() -> Path:
    return Path(__file__).resolve().parent / "config"


def load_nutrition_engine_config(config_dir: Path | None = None) -> NutritionEngineConfig:
    root = config_dir or default_config_dir()
    if not root.is_dir():
        raise FileNotFoundError(f"Nutrition engine config directory not found: {root}")

    raw = {name: _load_yaml(root / name) for name in _CONFIG_FILENAMES}

    scoring = _parse_scoring(raw["scoring.yaml"])
    goals = _parse_goals(raw["goals.yaml"])
    indicators = _parse_indicators(raw["indicators.yaml"])
    combination_defaults, combination_pairs, tag_rules = _parse_combinations(raw["combinations.yaml"])
    targets = _parse_targets(raw["targets.yaml"])

    _validate_cross_references(goals=goals, indicators=indicators, targets=targets, scoring=scoring)

    return NutritionEngineConfig(
        goals=goals,
        indicators=indicators,
        scoring=scoring,
        combination_defaults=combination_defaults,
        combination_pairs=combination_pairs,
        combination_tag_rules=tag_rules,
        targets=targets,
        config_dir=root,
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Missing nutrition engine config file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping: {path}")
    return data


def _require_priority(value: Any, *, context: str) -> PriorityLevel:
    text = str(value).strip()
    if text not in PRIORITY_LEVELS:
        raise ValueError(f"Invalid priority level {value!r} in {context}")
    return text  # type: ignore[return-value]


def _require_goal_id(value: Any, *, context: str) -> GoalId:
    text = str(value).strip()
    if text not in GOAL_IDS:
        raise ValueError(f"Invalid goal id {value!r} in {context}")
    return text  # type: ignore[return-value]


def _parse_scoring(raw: dict[str, Any]) -> ScoringConfig:
    formula = raw.get("score_formula") or {}
    if not isinstance(formula, dict):
        raise ValueError("scoring.yaml: score_formula must be a mapping")

    gq = float(formula["general_quality_weight"])
    ga = float(formula["goal_alignment_weight"])
    if gq < 0 or ga < 0:
        raise ValueError("scoring.yaml: score weights must be non-negative")
    if abs((gq + ga) - 1.0) > 1e-6 and str(formula.get("zero_goal_mode")) != "renormalize_quality_only":
        # Allow non-unit sum only when documented; still warn via strict check for v1 defaults.
        pass
    if abs((gq + ga) - 1.0) > 1e-6:
        raise ValueError("scoring.yaml: general_quality_weight + goal_alignment_weight must equal 1.0")

    zero_goal_mode = str(formula.get("zero_goal_mode", "renormalize_quality_only"))
    if zero_goal_mode not in ("renormalize_quality_only", "use_neutral_alignment"):
        raise ValueError(f"scoring.yaml: invalid zero_goal_mode {zero_goal_mode!r}")

    missing_policy = str(formula.get("missing_indicator_policy", "exclude_and_renormalize"))
    if missing_policy not in ("exclude_and_renormalize", "treat_as_neutral"):
        raise ValueError(f"scoring.yaml: invalid missing_indicator_policy {missing_policy!r}")

    raw_priority_weights = raw.get("priority_level_weights") or {}
    if not isinstance(raw_priority_weights, dict):
        raise ValueError("scoring.yaml: priority_level_weights must be a mapping")
    priority_level_weights: dict[PriorityLevel, float] = {}
    for level in PRIORITY_LEVELS:
        if level not in raw_priority_weights:
            raise ValueError(f"scoring.yaml: missing priority_level_weights.{level}")
        priority_level_weights[level] = float(raw_priority_weights[level])

    raw_bands = raw.get("score_bands") or []
    if not isinstance(raw_bands, list) or not raw_bands:
        raise ValueError("scoring.yaml: score_bands must be a non-empty list")
    bands: list[ScoreBand] = []
    for item in raw_bands:
        if not isinstance(item, dict):
            raise ValueError("scoring.yaml: each score_band must be a mapping")
        bands.append(ScoreBand(min_score=float(item["min_score"]), label=str(item["label"])))
    bands_sorted = tuple(sorted(bands, key=lambda b: b.min_score, reverse=True))

    return ScoringConfig(
        general_quality_weight=gq,
        goal_alignment_weight=ga,
        zero_goal_mode=zero_goal_mode,  # type: ignore[arg-type]
        missing_indicator_policy=missing_policy,  # type: ignore[arg-type]
        priority_level_weights=priority_level_weights,
        score_bands=bands_sorted,
        neutral_alignment_score=float(formula.get("neutral_alignment_score", 50.0)),
    )


def _parse_goals(raw: dict[str, Any]) -> dict[GoalId, GoalProfile]:
    section = raw.get("goals") or {}
    if not isinstance(section, dict):
        raise ValueError("goals.yaml: goals must be a mapping")

    goals: dict[GoalId, GoalProfile] = {}
    for goal_key, payload in section.items():
        goal_id = _require_goal_id(goal_key, context="goals.yaml")
        if not isinstance(payload, dict):
            raise ValueError(f"goals.yaml: goal {goal_key} must be a mapping")

        raw_priorities = payload.get("priority_levels") or {}
        if not isinstance(raw_priorities, dict):
            raise ValueError(f"goals.yaml: {goal_key}.priority_levels must be a mapping")
        priority_levels = {
            str(indicator_id): _require_priority(level, context=f"goals.{goal_key}.{indicator_id}")
            for indicator_id, level in raw_priorities.items()
        }

        activity_deps = payload.get("activity_dependencies") or {}
        if not isinstance(activity_deps, dict):
            raise ValueError(f"goals.yaml: {goal_key}.activity_dependencies must be a mapping")

        goals[goal_id] = GoalProfile(
            id=goal_id,
            questionnaire_code=str(payload["questionnaire_code"]),
            display_name=str(payload["display_name"]),
            base_target_keys=tuple(str(x) for x in (payload.get("base_target_keys") or [])),
            priority_levels=priority_levels,
            food_quality_priorities=tuple(str(x) for x in (payload.get("food_quality_priorities") or [])),
            activity_dependencies=dict(activity_deps),
            conflict_tags=tuple(str(x) for x in (payload.get("conflict_tags") or [])),
            notes=tuple(str(x) for x in (payload.get("notes") or [])),
        )

    missing = [g for g in GOAL_IDS if g not in goals]
    if missing:
        raise ValueError(f"goals.yaml: missing required goals: {missing}")
    return goals


def _parse_indicators(raw: dict[str, Any]) -> dict[str, IndicatorDefinition]:
    section = raw.get("indicators") or {}
    if not isinstance(section, dict) or not section:
        raise ValueError("indicators.yaml: indicators must be a non-empty mapping")

    indicators: dict[str, IndicatorDefinition] = {}
    for indicator_id, payload in section.items():
        if not isinstance(payload, dict):
            raise ValueError(f"indicators.yaml: {indicator_id} must be a mapping")

        direction = str(payload.get("score_direction", "higher_better"))
        if direction not in ("higher_better", "lower_better"):
            raise ValueError(f"indicators.yaml: {indicator_id} has invalid score_direction")

        raw_relevance = payload.get("goal_relevance") or {}
        if not isinstance(raw_relevance, dict):
            raise ValueError(f"indicators.yaml: {indicator_id}.goal_relevance must be a mapping")
        goal_relevance: dict[GoalId, PriorityLevel] = {}
        for goal_key, level in raw_relevance.items():
            goal_id = _require_goal_id(goal_key, context=f"indicators.{indicator_id}")
            goal_relevance[goal_id] = _require_priority(level, context=f"indicators.{indicator_id}.{goal_key}")
        for goal_id in GOAL_IDS:
            if goal_id not in goal_relevance:
                raise ValueError(f"indicators.yaml: {indicator_id} missing goal_relevance.{goal_id}")

        ordinal_raw = payload.get("ordinal_scores") or {}
        if ordinal_raw is None:
            ordinal_raw = {}
        if not isinstance(ordinal_raw, dict):
            raise ValueError(f"indicators.yaml: {indicator_id}.ordinal_scores must be a mapping")
        ordinal_scores = {str(k): float(v) for k, v in ordinal_raw.items()}

        indicators[str(indicator_id)] = IndicatorDefinition(
            id=str(indicator_id),
            display_name=str(payload["display_name"]),
            source_fields=tuple(str(x) for x in (payload.get("source_fields") or [])),
            score_direction=direction,  # type: ignore[arg-type]
            general_quality_priority=_require_priority(
                payload["general_quality_priority"],
                context=f"indicators.{indicator_id}.general_quality_priority",
            ),
            goal_relevance=goal_relevance,
            description=str(payload.get("description") or "").strip(),
            is_behavioural_proxy=bool(payload.get("is_behavioural_proxy", True)),
            uses_diet_preference_context=bool(payload.get("uses_diet_preference_context", False)),
            ordinal_scores=ordinal_scores,
        )

    return indicators


def _parse_combinations(
    raw: dict[str, Any],
) -> tuple[CombinationDefaults, tuple[CombinationRule, ...], tuple[dict[str, Any], ...]]:
    defaults_raw = raw.get("defaults") or {}
    if not isinstance(defaults_raw, dict):
        raise ValueError("combinations.yaml: defaults must be a mapping")

    compatibility = str(defaults_raw.get("compatibility", "moderate"))
    if compatibility not in ("high", "moderate", "low"):
        raise ValueError("combinations.yaml: invalid defaults.compatibility")

    target_resolution = defaults_raw.get("target_resolution") or {}
    if not isinstance(target_resolution, dict):
        raise ValueError("combinations.yaml: defaults.target_resolution must be a mapping")

    defaults = CombinationDefaults(
        compatibility=compatibility,  # type: ignore[arg-type]
        priority_resolution=str(defaults_raw.get("priority_resolution", "max_of_shared")),
        target_resolution={str(k): str(v) for k, v in target_resolution.items()},
        conflict_severity=str(defaults_raw.get("conflict_severity", "medium")),
    )

    pairs_raw = raw.get("pairs") or []
    if not isinstance(pairs_raw, list):
        raise ValueError("combinations.yaml: pairs must be a list")

    pairs: list[CombinationRule] = []
    seen: set[tuple[GoalId, GoalId]] = set()
    for item in pairs_raw:
        if not isinstance(item, dict):
            raise ValueError("combinations.yaml: each pair must be a mapping")
        goals = item.get("goals") or []
        if not isinstance(goals, list) or len(goals) != 2:
            raise ValueError("combinations.yaml: each pair.goals must have exactly 2 goal ids")
        a = _require_goal_id(goals[0], context="combinations.pairs")
        b = _require_goal_id(goals[1], context="combinations.pairs")
        if a == b:
            raise ValueError(f"combinations.yaml: pair cannot repeat the same goal: {a}")
        key = tuple(sorted((a, b)))  # type: ignore[assignment]
        ordered = (key[0], key[1])
        if ordered in seen:
            raise ValueError(f"combinations.yaml: duplicate pair {ordered}")
        seen.add(ordered)

        pair_compat = str(item.get("compatibility", defaults.compatibility))
        if pair_compat not in ("high", "moderate", "low"):
            raise ValueError(f"combinations.yaml: invalid compatibility for {ordered}")

        pair_targets = item.get("target_resolution") or {}
        if not isinstance(pair_targets, dict):
            raise ValueError(f"combinations.yaml: target_resolution for {ordered} must be a mapping")

        pairs.append(
            CombinationRule(
                goal_a=ordered[0],
                goal_b=ordered[1],
                compatibility=pair_compat,  # type: ignore[arg-type]
                target_resolution={str(k): str(v) for k, v in pair_targets.items()},
                priority_resolution=str(item.get("priority_resolution", defaults.priority_resolution)),
                conflict_severity=(
                    str(item["conflict_severity"]) if item.get("conflict_severity") is not None else None
                ),
                notes=tuple(str(x) for x in (item.get("notes") or [])),
            )
        )

    tag_rules_raw = raw.get("tag_rules") or []
    if not isinstance(tag_rules_raw, list):
        raise ValueError("combinations.yaml: tag_rules must be a list")
    tag_rules = tuple(dict(x) for x in tag_rules_raw if isinstance(x, dict))

    return defaults, tuple(pairs), tag_rules


def _parse_target_range(raw: Any) -> TargetRange | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("targets.yaml: range must be a mapping")
    low = raw.get("low")
    high = raw.get("high")
    return TargetRange(
        low=float(low) if low is not None else None,
        high=float(high) if high is not None else None,
        unit=str(raw["unit"]) if raw.get("unit") is not None else None,
        mode=str(raw["mode"]) if raw.get("mode") is not None else None,
    )


def _parse_targets(raw: dict[str, Any]) -> dict[str, TargetDefinition]:
    section = raw.get("target_definitions") or {}
    if not isinstance(section, dict) or not section:
        raise ValueError("targets.yaml: target_definitions must be a non-empty mapping")

    targets: dict[str, TargetDefinition] = {}
    for key, payload in section.items():
        if not isinstance(payload, dict):
            raise ValueError(f"targets.yaml: {key} must be a mapping")
        activity_raw = payload.get("activity_bands") or {}
        if not isinstance(activity_raw, dict):
            raise ValueError(f"targets.yaml: {key}.activity_bands must be a mapping")
        activity_bands = {
            str(band): (_parse_target_range(band_payload) or TargetRange())
            for band, band_payload in activity_raw.items()
        }
        targets[str(key)] = TargetDefinition(
            key=str(key),
            kind=str(payload.get("kind") or ""),
            range=_parse_target_range(payload.get("range")),
            priority=str(payload["priority"]) if payload.get("priority") is not None else None,
            concept=str(payload["concept"]) if payload.get("concept") is not None else None,
            activity_bands=activity_bands,
            notes=tuple(str(x) for x in (payload.get("notes") or [])),
        )
    return targets


def _validate_cross_references(
    *,
    goals: dict[GoalId, GoalProfile],
    indicators: dict[str, IndicatorDefinition],
    targets: dict[str, TargetDefinition],
    scoring: ScoringConfig,
) -> None:
    _ = scoring  # reserved for future cross-checks

    for goal in goals.values():
        for target_key in goal.base_target_keys:
            if target_key not in targets:
                raise ValueError(
                    f"goals.yaml: goal {goal.id} references unknown target key {target_key!r}"
                )
        for indicator_id in goal.priority_levels:
            if indicator_id not in indicators:
                raise ValueError(
                    f"goals.yaml: goal {goal.id} references unknown indicator {indicator_id!r}"
                )

    # Goal priority tables should cover the approved v1 indicator set.
    for indicator_id, indicator in indicators.items():
        for goal_id, goal in goals.items():
            if indicator_id not in goal.priority_levels:
                raise ValueError(
                    f"goals.yaml: goal {goal_id} missing priority_levels.{indicator_id}"
                )
            # Keep goal profile and indicator goal_relevance aligned for v1.
            if goal.priority_levels[indicator_id] != indicator.goal_relevance[goal_id]:
                raise ValueError(
                    f"Priority mismatch for indicator {indicator_id!r} / goal {goal_id!r}: "
                    f"goals.yaml={goal.priority_levels[indicator_id]!r} "
                    f"indicators.yaml={indicator.goal_relevance[goal_id]!r}"
                )
