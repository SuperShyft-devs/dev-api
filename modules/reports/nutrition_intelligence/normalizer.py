"""Input Normalizer for the Nutrition Intelligence Engine.

Converts raw questionnaire lookup answers into canonical option codes and
scale values. Does not score, interpret healthiness, invent goals, or
fabricate intake/targets.

Reuses existing ReportsService resolution helpers and questionnaire_field_config
health-priority label maps rather than inventing new option mappings.
"""

from __future__ import annotations

from typing import Any

from db.seed.questionnaire_field_config import (
    HEALTH_PRIORITIES_LABEL_TO_VALUE,
    HEALTH_PRIORITIES_OPTION_VALUES,
)
from modules.reports.nutrition_intelligence.models import NormalizedAnswers, ScaleValue
from modules.reports.service import ReportsService

# Single-choice fields normalized to option_value codes when present.
_SINGLE_CHOICE_FIELDS: tuple[str, ...] = (
    "diet_preference",
    "healthy_breakfast_frequency",
    "fresh_fruit_frequency",
    "fresh_vegetable_frequency",
    "baked_goods_frequency",
    "dessert_frequency",
    "butter_dish_frequency",
    "red_meat_frequency",
    "extra_salt_frequency",
    "iodized_salt_status",
    "caffeine_frequency",
    "water_intake_frequency",
    "sickness_frequency",
    "exercise_frequency_week",
    "exercise_level",
    "physical_activity_frequency",
    "daily_active_duration",
    "sleeping_hours",
    "alcohol_frequency",
    "tobacco_frequency",
    "goal_preference",
)

_MULTI_CHOICE_FIELDS: tuple[str, ...] = (
    "food_groups",
    "caffeine_type",
)

_MAX_HEALTH_PRIORITIES = 2


def normalize_questionnaire_lookup(
    lookup: dict[str, Any],
    *,
    user_gender: str | None = None,
    option_reverse_map: dict[str, dict[str, str]] | None = None,
) -> NormalizedAnswers:
    """Normalize a ReportsService-style questionnaire lookup.

    Parameters
    ----------
    lookup:
        ``question_key -> answer`` mapping as produced by
        ``ReportsService._build_questionnaire_lookup``.
    user_gender:
        Optional profile gender fallback (same as Health Span Index).
    option_reverse_map:
        Optional ``{question_key: {label_or_code_or_fingerprint: option_value}}``
        as built by ``ReportsService._build_option_reverse_map``.
    """
    reverse_map = option_reverse_map or {}

    single_choices: dict[str, str | None] = {}
    for key in _SINGLE_CHOICE_FIELDS:
        single_choices[key] = _normalize_optional_choice(
            lookup.get(key),
            reverse_map.get(key, {}),
        )

    multi_choices: dict[str, tuple[str, ...] | None] = {}
    for key in _MULTI_CHOICE_FIELDS:
        multi_choices[key] = _normalize_optional_multi_choice(
            lookup.get(key),
            reverse_map.get(key, {}),
            present=(key in lookup),
        )

    health_priority_codes = _normalize_health_priorities(
        lookup.get("health_priorities") if "health_priorities" in lookup else None,
        reverse_map.get("health_priorities", {}),
    )

    diet_preference = single_choices["diet_preference"]
    red_meat = single_choices["red_meat_frequency"]
    red_meat_defaulted = False
    if red_meat is None and diet_preference in ReportsService._VEGETARIAN_DIET_PREFERENCE_VALUES:
        # Preserve existing ReportsService nutrition-payload fallback.
        red_meat = "5"
        red_meat_defaulted = True

    gender = ReportsService._normalize_gender(lookup.get("gender")) or ReportsService._normalize_gender(
        user_gender
    )

    height = _normalize_height(lookup.get("height") if "height" in lookup else None)
    weight = _normalize_weight(lookup.get("weight") if "weight" in lookup else None)
    weight_loss_goal = _normalize_weight_loss_goal(
        lookup.get("weight_loss_goal") if "weight_loss_goal" in lookup else None
    )

    return NormalizedAnswers(
        health_priority_codes=health_priority_codes,
        gender=gender,
        height=height,
        weight=weight,
        diet_preference=diet_preference,
        food_groups=multi_choices["food_groups"],
        healthy_breakfast_frequency=single_choices["healthy_breakfast_frequency"],
        fresh_fruit_frequency=single_choices["fresh_fruit_frequency"],
        fresh_vegetable_frequency=single_choices["fresh_vegetable_frequency"],
        baked_goods_frequency=single_choices["baked_goods_frequency"],
        dessert_frequency=single_choices["dessert_frequency"],
        butter_dish_frequency=single_choices["butter_dish_frequency"],
        red_meat_frequency=red_meat,
        red_meat_frequency_defaulted=red_meat_defaulted,
        extra_salt_frequency=single_choices["extra_salt_frequency"],
        iodized_salt_status=single_choices["iodized_salt_status"],
        caffeine_frequency=single_choices["caffeine_frequency"],
        caffeine_type=multi_choices["caffeine_type"],
        water_intake_frequency=single_choices["water_intake_frequency"],
        sickness_frequency=single_choices["sickness_frequency"],
        exercise_frequency_week=single_choices["exercise_frequency_week"],
        exercise_level=single_choices["exercise_level"],
        physical_activity_frequency=single_choices["physical_activity_frequency"],
        daily_active_duration=single_choices["daily_active_duration"],
        sleeping_hours=single_choices["sleeping_hours"],
        alcohol_frequency=single_choices["alcohol_frequency"],
        tobacco_frequency=single_choices["tobacco_frequency"],
        goal_preference=single_choices["goal_preference"],
        weight_loss_goal=weight_loss_goal,
    )


def _normalize_optional_choice(raw: Any, key_map: dict[str, str]) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, list):
        # Unexpected list for single_choice: take first resolvable item only.
        for item in raw:
            resolved = _resolve_choice_code(item, key_map)
            if resolved is not None:
                return resolved
        return None
    return _resolve_choice_code(raw, key_map)


def _normalize_optional_multi_choice(
    raw: Any,
    key_map: dict[str, str],
    *,
    present: bool,
) -> tuple[str, ...] | None:
    if not present:
        return None
    if raw is None:
        return None
    if isinstance(raw, list):
        items = raw
    else:
        items = [raw]
    resolved: list[str] = []
    seen: set[str] = set()
    for item in items:
        code = _resolve_choice_code(item, key_map)
        if code is None or code in seen:
            continue
        seen.add(code)
        resolved.append(code)
    return tuple(resolved)


def _resolve_choice_code(raw: Any, key_map: dict[str, str]) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    # Reuse ReportsService label/code/fingerprint resolution.
    resolved = ReportsService._resolve_nutrition_choice_value(raw, key_map)
    resolved_text = str(resolved).strip() if resolved is not None else ""
    return resolved_text or None


def _normalize_health_priorities(raw: Any, key_map: dict[str, str]) -> tuple[str, ...]:
    """Return 0–2 canonical health_priorities option codes. Never invent goals."""
    if raw is None:
        return ()

    if isinstance(raw, list):
        fragments = raw
    else:
        fragments = [raw]

    codes: list[str] = []
    seen: set[str] = set()
    for fragment in fragments:
        code = _resolve_health_priority_code(fragment, key_map)
        if code is None or code in seen:
            continue
        seen.add(code)
        codes.append(code)
        if len(codes) >= _MAX_HEALTH_PRIORITIES:
            break
    return tuple(codes)


def _resolve_health_priority_code(raw: Any, key_map: dict[str, str]) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.lower() == "none":
        return None

    # 1) Existing option reverse-map / ReportsService resolver.
    if key_map:
        via_map = ReportsService._resolve_nutrition_choice_value(raw, key_map)
        via_text = str(via_map).strip() if via_map is not None else ""
        if via_text in HEALTH_PRIORITIES_OPTION_VALUES:
            return via_text

    # 2) Existing questionnaire_field_config code/label maps (same source Metsights uses).
    if text in HEALTH_PRIORITIES_OPTION_VALUES:
        return text

    normalized = ReportsService._normalize_choice_label(text)
    by_label = HEALTH_PRIORITIES_LABEL_TO_VALUE.get(normalized)
    if by_label is not None:
        return by_label

    fingerprint = ReportsService._choice_fingerprint(text)
    for label, code in HEALTH_PRIORITIES_LABEL_TO_VALUE.items():
        if ReportsService._choice_fingerprint(label) == fingerprint:
            return code

    return None


def _normalize_height(raw: Any) -> ScaleValue | None:
    if raw is None:
        return None
    value, unit = ReportsService._extract_scale_answer(raw)
    normalized_unit = ReportsService._normalize_height_unit(unit)
    if value is None and normalized_unit is None and unit is None:
        # Present but unparseable → still surface empty scale rather than inventing.
        if isinstance(raw, dict):
            return ScaleValue(value=None, unit=None)
        return None
    return ScaleValue(value=value, unit=normalized_unit)


def _normalize_weight(raw: Any) -> ScaleValue | None:
    if raw is None:
        return None
    value, unit = ReportsService._extract_scale_answer(raw)
    normalized_unit = _normalize_weight_unit(unit)
    if value is None and normalized_unit is None and not isinstance(raw, dict):
        return None
    if isinstance(raw, dict):
        return ScaleValue(value=value, unit=normalized_unit)
    return None


def _normalize_weight_loss_goal(raw: Any) -> ScaleValue | None:
    # Same scale shape as weight; reuse weight-unit codes (kg/lb).
    return _normalize_weight(raw)


def _normalize_weight_unit(value: str | None) -> str | None:
    """Mirror ReportsService height-unit normalization for weight option codes."""
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"0", "kg"}:
        return "kg"
    if normalized in {"1", "lb", "lbs"}:
        return "lb"
    return value.strip() or None
