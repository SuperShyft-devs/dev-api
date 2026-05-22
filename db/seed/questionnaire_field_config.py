"""Questionnaire field configuration: single source of truth for type overrides,
extra options, and metsights-to-choice converters.

This module is imported by:
  - db/seed/metsights_questionnaire_data.py  (to build the correct DB seed rows)
  - modules/metsights/sync_service.py        (to convert incoming metsights values)

No backend routes and no frontend code need to be touched when making changes
to question types or option lists — edit only this file.
"""

from __future__ import annotations

from typing import Callable

# ---------------------------------------------------------------------------
# 1. Question-type overrides
#    question_key -> new question_type
#    Applied during seeding: overrides the type declared in METSIGHTS_QUESTIONS.
# ---------------------------------------------------------------------------
QUESTION_TYPE_OVERRIDES: dict[str, str] = {
    # Change 1: daily_active_duration was a raw float+unit scale field from
    # metsights; we now present it as a structured single-choice to the user.
    "daily_active_duration": "single_choice",
    # Change 6: health_priorities was multiple_choice; business requirement
    # changed to allow only one selection (single_choice).
    "health_priorities": "single_choice",
}


# ---------------------------------------------------------------------------
# 2. Extra options to append (or replace) per question key.
#    question_key -> list of (option_value, display_name)
#
#    For questions in QUESTION_TYPE_OVERRIDES whose original options are being
#    replaced (daily_active_duration), the full new option list is provided here.
#    For all other questions the list is APPENDED to the existing options.
# ---------------------------------------------------------------------------
EXTRA_OPTIONS: dict[str, list[tuple[str, str]]] = {
    # Change 1: daily_active_duration — replace old scale-unit options with
    # 5 duration-range buckets (values 0–4).
    "daily_active_duration": [
        ("0", "Less than 15 mins"),
        ("1", "Between 15-30 mins"),
        ("2", "Between 30-60 mins"),
        ("3", "Between 1-2 hours"),
        ("4", "More than 2 hours"),
    ],
    # Change 2: alcohol_frequency — append 2 infrequent-drinker options.
    # Existing values 0–3 must remain unchanged; new values are 4 and 5.
    "alcohol_frequency": [
        ("4", "1-2 times in 3 months"),
        ("5", "1-2 times in 6 months"),
    ],
    # Change 3: tobacco_frequency — append 2 occasional-smoker options.
    # Existing values 0–4 must remain unchanged; new values are 5 and 6.
    "tobacco_frequency": [
        ("5", "4-5 times a month"),
        ("6", "1-2 times a month"),
    ],
    # Change 4: caffeine_frequency — append 1 option for weekly consumers.
    # Existing values 0–2 must remain unchanged; new value is 3.
    "caffeine_frequency": [
        ("3", "2-3 times a week"),
    ],
    # Change 5: add an explicit "None" option to the three multi-select health
    # condition fields. value="none" is treated as an empty selection / clear-all
    # by the frontend and submitted as [] to the metsights API.
    "family_health_history": [
        ("none", "None"),
    ],
    "diagnosed_diseases": [
        ("none", "None"),
    ],
    "diagnosed_diseases_medications": [
        ("none", "None"),
    ],
}


# ---------------------------------------------------------------------------
# 3. Scale-to-choice converters
#    question_key -> callable(value: float, unit_code: str) -> option_value | None
#
#    Used by the metsights sync service when it receives a numeric (scale)
#    value for a field that has been reclassified as single_choice in our DB.
#    Return None to skip/discard the answer if conversion is not possible.
# ---------------------------------------------------------------------------

def _convert_daily_active_duration(value: float, unit_code: str) -> str | None:
    """Convert a metsights float+unit walking-duration into one of the 5 buckets.

    Metsights unit codes for daily_active_duration:
      "0" = minutes daily
      "1" = hours daily

    Bucketing (in minutes):
      < 15        -> "0"  (Less than 15 mins)
      15 – <30   -> "1"  (Between 15-30 mins)
      30 – <60   -> "2"  (Between 30-60 mins)
      60 – <120  -> "3"  (Between 1-2 hours)
      >= 120     -> "4"  (More than 2 hours)
    """
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None

    # Normalise to minutes
    unit = str(unit_code).strip()
    if unit == "1":          # hours daily
        minutes = val * 60.0
    else:                    # "0" = minutes daily (default)
        minutes = val

    if minutes < 15:
        return "0"
    if minutes < 30:
        return "1"
    if minutes < 60:
        return "2"
    if minutes < 120:
        return "3"
    return "4"


SCALE_TO_CHOICE_CONVERTERS: dict[str, Callable[[float, str], str | None]] = {
    "daily_active_duration": _convert_daily_active_duration,
}


# ---------------------------------------------------------------------------
# 4. Push-side conversions (Our DB answer → Metsights API format)
#
#    When pushing questionnaire answers to Metsights, our custom option values
#    must be mapped back into Metsights' own format.
#
#    CHOICE_TO_METSIGHTS_VALUE:
#      question_key -> {our_option_value -> metsights_value}
#      Used for single_choice / multiple_choice fields where we added new
#      option values that don't exist in metsights choices, so they map to
#      the closest metsights equivalent.
#
#    DAILY_ACTIVE_DURATION_PUSH_MAP:
#      Our option_value -> (metsights_float_value, metsights_unit_code)
#      Change 1: our 5 buckets map to hardcoded metsights float+unit pairs.
# ---------------------------------------------------------------------------

# Change 1: daily_active_duration
#   Our bucket   → (value sent to metsights, unit sent to metsights)
#   unit 0 = minutes daily, unit 1 = hours daily
DAILY_ACTIVE_DURATION_PUSH_MAP: dict[str, tuple[float, str]] = {
    "0": (15.0, "0"),    # Less than 15 mins   → 15 mins daily
    "1": (30.0, "0"),    # Between 15-30 mins  → 30 mins daily
    "2": (1.0,  "1"),    # Between 30-60 mins  → 1 hr daily
    "3": (2.0,  "1"),    # Between 1-2 hours   → 2 hr daily
    "4": (3.0,  "1"),    # More than 2 hours   → 3 hr daily
}

# Change 2: alcohol_frequency
#   New options "4" and "5" (infrequent drinkers) → map to "1" (I quit alcohol)
# Change 3: tobacco_frequency
#   New options "5" and "6" (occasional smokers)  → map to "1" (I quit smoking)
# Change 4: caffeine_frequency
#   New option "3" (2-3 times a week)             → map to "0" (I do not drink coffee or tea)
CHOICE_TO_METSIGHTS_VALUE: dict[str, dict[str, str]] = {
    "alcohol_frequency": {
        "4": "1",  # 1-2 times in 3 months → I quit alcohol
        "5": "1",  # 1-2 times in 6 months → I quit alcohol
    },
    "tobacco_frequency": {
        "5": "1",  # 4-5 times a month     → I quit smoking
        "6": "1",  # 1-2 times a month     → I quit smoking
    },
    "caffeine_frequency": {
        "3": "0",  # 2-3 times a week      → I do not drink coffee or tea
    },
}

# Change 5: multi-select health condition fields
#   When the user selects only "none", send nothing (skip the field entirely).
#   These sets identify which fields apply this rule.
NONE_CLEARS_MULTISELECT_FIELDS: frozenset[str] = frozenset({
    "family_health_history",
    "diagnosed_diseases",
    "diagnosed_diseases_medications",
})

# Fields stored as single_choice in our DB but Metsights expects a one-element array
# (see RECORDS_API fitness-parameters: health_priorities).
METSIGHTS_PUSH_AS_LIST: frozenset[str] = frozenset({
    "health_priorities",
})
