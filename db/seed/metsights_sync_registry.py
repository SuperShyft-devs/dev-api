"""Canonical Metsights sync registry: categories, question assignments, and metsights_sync configs.

Used by ``metsights_sync_operations.reset_metsights_sync`` and the admin
``POST /questionnaire/metsights-sync/reset`` endpoint.
"""

from __future__ import annotations

from typing import Any

from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    ADVANCED_BLOOD_PARAMETER_QUESTION_ORDER,
    ALL_BLOOD_PARAMETER_KEYS,
    BLOOD_PARAMETER_CATEGORY_KEY,
    BLOOD_PARAMETER_QUESTION_ORDER,
    build_blood_parameter_metsights_sync,
)
from db.seed.questionnaire_field_config import (
    CHOICE_TO_METSIGHTS_VALUE,
    DAILY_ACTIVE_DURATION_PUSH_MAP,
    HEALTH_PRIORITIES_OPTION_VALUES,
)

METSIGHTS_CATEGORY_OF = "metsights"

METSIGHTS_SYNC_CATEGORIES: tuple[tuple[str, str], ...] = (
    ("physical-measurement", "Physical Measurement"),
    ("vitals", "Vitals"),
    ("diet-lifestyle-parameters", "Diet & Lifestyle"),
    ("fitness-parameters", "Fitness Parameters"),
    (BLOOD_PARAMETER_CATEGORY_KEY, "Blood Parameters"),
    (ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY, "Advanced Blood Parameters"),
)

_PHYSICAL_MEASUREMENT_KEYS: tuple[str, ...] = (
    "height",
    "weight",
    "waist_circumference",
    "hip_circumference",
    "body_fat",
)

_VITALS_KEYS: tuple[str, ...] = (
    "systolic_blood_pressure",
    "diastolic_blood_pressure",
)

_DIET_LIFESTYLE_KEYS: tuple[str, ...] = (
    "living_region",
    "diet_preference",
    "food_groups",
    "healthy_breakfast_frequency",
    "fresh_fruit_frequency",
    "fresh_vegetable_frequency",
    "baked_goods_frequency",
    "red_meat_frequency",
    "butter_dish_frequency",
    "dessert_frequency",
    "caffeine_frequency",
    "caffeine_type",
    "iodized_salt_status",
    "extra_salt_frequency",
    "sitting_hours",
    "physical_activity_frequency",
    "sleeping_hours",
    "alcohol_frequency",
    "tobacco_frequency",
    "family_health_history",
    "family_health_history_other",
    "diagnosed_diseases",
    "diagnosed_diseases_other",
    "diagnosed_diseases_medications",
    "diagnosed_diseases_medications_other",
)

_FITNESS_ONLY_KEYS: tuple[str, ...] = (
    "exercise_frequency_week",
    "exercise_level",
    "daily_active_duration",
    "water_intake_frequency",
    "sickness_frequency",
    "health_priorities",
    "goal_preference",
    "weight_loss_goal",
)

_SCALE_KEYS: frozenset[str] = frozenset({
    "height",
    "weight",
    "waist_circumference",
    "hip_circumference",
    "body_fat",
    "systolic_blood_pressure",
    "diastolic_blood_pressure",
    "weight_loss_goal",
    *ALL_BLOOD_PARAMETER_KEYS,
})

_SKIP_IF_ONLY_KEYS: frozenset[str] = frozenset({
    "family_health_history",
    "diagnosed_diseases",
    "diagnosed_diseases_medications",
})

_CHOICE_REMAP_KEYS: frozenset[str] = frozenset({
    "alcohol_frequency",
    "tobacco_frequency",
    "caffeine_frequency",
})

_DAILY_ACTIVE_DURATION_BUCKETS: list[dict[str, Any]] = [
    {"max_minutes": 15, "option_value": "0"},
    {"max_minutes": 30, "option_value": "1"},
    {"max_minutes": 60, "option_value": "2"},
    {"max_minutes": 120, "option_value": "3"},
    {"max_minutes": None, "option_value": "4"},
]

_DAILY_ACTIVE_DURATION_BUCKET_MAP: dict[str, dict[str, Any]] = {
    bucket: {"value": value, "unit": unit}
    for bucket, (value, unit) in DAILY_ACTIVE_DURATION_PUSH_MAP.items()
}


def _build_question_category_assignments() -> dict[str, list[str]]:
    assignments: dict[str, list[str]] = {}

    def _add(key: str, category_key: str) -> None:
        assignments.setdefault(key, [])
        if category_key not in assignments[key]:
            assignments[key].append(category_key)

    for key in _PHYSICAL_MEASUREMENT_KEYS:
        _add(key, "physical-measurement")
        _add(key, "fitness-parameters")

    for key in _VITALS_KEYS:
        _add(key, "vitals")

    for key in _DIET_LIFESTYLE_KEYS:
        _add(key, "diet-lifestyle-parameters")
        _add(key, "fitness-parameters")

    for key in _FITNESS_ONLY_KEYS:
        _add(key, "fitness-parameters")

    for key in BLOOD_PARAMETER_QUESTION_ORDER:
        _add(key, BLOOD_PARAMETER_CATEGORY_KEY)

    for key in ADVANCED_BLOOD_PARAMETER_QUESTION_ORDER:
        _add(key, ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY)

    return assignments


QUESTION_CATEGORY_ASSIGNMENTS: dict[str, list[str]] = _build_question_category_assignments()

CATEGORY_QUESTION_ORDER: dict[str, list[str]] = {
    "physical-measurement": list(_PHYSICAL_MEASUREMENT_KEYS),
    "vitals": list(_VITALS_KEYS),
    "diet-lifestyle-parameters": list(_DIET_LIFESTYLE_KEYS),
    "fitness-parameters": list(_PHYSICAL_MEASUREMENT_KEYS)
    + list(_DIET_LIFESTYLE_KEYS)
    + list(_FITNESS_ONLY_KEYS),
    BLOOD_PARAMETER_CATEGORY_KEY: list(BLOOD_PARAMETER_QUESTION_ORDER),
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY: list(ADVANCED_BLOOD_PARAMETER_QUESTION_ORDER),
}

PACKAGE_METSIGHTS_CATEGORY_LINKS: dict[str, list[str]] = {
    "METSIGHTS_BASIC": [
        "physical-measurement",
        "vitals",
        "diet-lifestyle-parameters",
        BLOOD_PARAMETER_CATEGORY_KEY,
    ],
    "METSIGHTS_PRO": [
        "physical-measurement",
        "vitals",
        "diet-lifestyle-parameters",
        BLOOD_PARAMETER_CATEGORY_KEY,
        ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    ],
    "MY_FITNESS_PRINT": ["fitness-parameters"],
}

ALL_SYNCED_QUESTION_KEYS: tuple[str, ...] = tuple(QUESTION_CATEGORY_ASSIGNMENTS.keys())


def _passthrough_sync() -> dict[str, Any]:
    return {
        "pull": {"enabled": True, "strategy": "passthrough"},
        "push": {"enabled": True, "strategy": "passthrough"},
    }


def _scale_sync() -> dict[str, Any]:
    return {
        "pull": {"enabled": True, "strategy": "scale_ingest"},
        "push": {"enabled": True, "strategy": "scale_emit"},
    }


def build_metsights_sync(question_key: str) -> dict[str, Any]:
    """Return the canonical metsights_sync JSON for a question_key."""
    if question_key in ALL_BLOOD_PARAMETER_KEYS:
        return build_blood_parameter_metsights_sync(question_key)

    if question_key in _SCALE_KEYS:
        return _scale_sync()

    if question_key == "daily_active_duration":
        return {
            "pull": {
                "enabled": True,
                "strategy": "scale_to_bucket",
                "unit_codes": {"hours": "1"},
                "buckets": _DAILY_ACTIVE_DURATION_BUCKETS,
            },
            "push": {
                "enabled": True,
                "strategy": "bucket_to_scale",
                "bucket_map": _DAILY_ACTIVE_DURATION_BUCKET_MAP,
            },
        }

    if question_key in _CHOICE_REMAP_KEYS:
        choice_map = CHOICE_TO_METSIGHTS_VALUE.get(question_key) or {}
        return {
            "pull": {"enabled": True, "strategy": "passthrough"},
            "push": {"enabled": True, "strategy": "choice_remap", "choice_map": choice_map},
        }

    if question_key in _SKIP_IF_ONLY_KEYS:
        return {
            "pull": {"enabled": True, "strategy": "passthrough"},
            "push": {"enabled": True, "strategy": "skip_if_only", "skip_values": ["none"]},
        }

    if question_key == "health_priorities":
        return {
            "pull": {"enabled": True, "strategy": "list_to_single"},
            "push": {
                "enabled": True,
                "strategy": "single_to_list",
                "min_list_size": 2,
                "max_list_size": 2,
                "fill_strategy": "deterministic_next",
                "fill_from_option_values": sorted(HEALTH_PRIORITIES_OPTION_VALUES),
            },
        }

    if question_key == "caffeine_type":
        return {
            "pull": {"enabled": True, "strategy": "list_to_single"},
            "push": {"enabled": True, "strategy": "list_to_single", "pick_strategy": "first_selected"},
        }

    if question_key == "iodized_salt_status":
        return {
            "pull": {"enabled": True, "strategy": "string_boolean"},
            "push": {"enabled": True, "strategy": "boolean_string"},
        }

    return _passthrough_sync()


METSIGHTS_SYNC_BY_QUESTION_KEY: dict[str, dict[str, Any]] = {
    key: build_metsights_sync(key) for key in ALL_SYNCED_QUESTION_KEYS
}
