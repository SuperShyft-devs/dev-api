"""Evaluate healthy habit rules against questionnaire answers (report overview)."""

from __future__ import annotations

import math
from dataclasses import dataclass

from modules.questionnaire.models import QuestionnaireDefinition, QuestionnaireHealthyHabitRule

_CHOICE_TYPES = {"single_choice", "multiple_choice"}
_SCALE_TYPE = "scale"
_QUESTION_TYPE_ALIASES = {"multi_choice": "multiple_choice"}
_CONDITION_OPTION = "option_match"
_CONDITION_SCALE = "scale_range"


def _normalize_question_type(value: str | None) -> str:
    raw = (value or "").strip().lower()
    return _QUESTION_TYPE_ALIASES.get(raw, raw)


def _norm_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _rule_matches(
    *,
    rule: QuestionnaireHealthyHabitRule,
    definition: QuestionnaireDefinition,
    answer: object,
) -> bool:
    qtype = _normalize_question_type(definition.question_type)
    if qtype == "text":
        return False

    ctype = (rule.condition_type or "").strip().lower()
    if ctype == _CONDITION_OPTION:
        if qtype not in _CHOICE_TYPES:
            return False
        raw_list = rule.matched_option_values
        if not isinstance(raw_list, list) or len(raw_list) == 0:
            return False
        allowed = {_norm_text(x) for x in raw_list if x is not None and str(x).strip() != ""}
        if not allowed:
            return False
        if qtype == "single_choice":
            return _norm_text(answer) in allowed
        if qtype == "multiple_choice":
            if not isinstance(answer, list):
                return False
            selected = {_norm_text(x) for x in answer}
            return bool(selected & allowed)
        return False

    if ctype == _CONDITION_SCALE:
        if qtype != _SCALE_TYPE:
            return False
        if not isinstance(answer, dict):
            return False
        value = answer.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            return False
        unit_rule = _norm_text(rule.scale_unit)
        unit_ans = _norm_text(answer.get("unit"))
        if not unit_rule or unit_ans != unit_rule:
            return False
        if rule.scale_min is None or rule.scale_max is None:
            return False
        try:
            lo = float(rule.scale_min)
            hi = float(rule.scale_max)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(lo) or not math.isfinite(hi):
            return False
        fv = float(value)
        return lo <= fv <= hi

    return False


@dataclass(frozen=True)
class HealthyHabitComputed:
    habit_key: str | None
    habit_label: str


def compute_top_healthy_habits(
    *,
    rules: list[QuestionnaireHealthyHabitRule],
    definitions_by_id: dict[int, QuestionnaireDefinition],
    answers_by_question_id: dict[int, object],
    limit: int = 3,
) -> list[HealthyHabitComputed]:
    """Return up to `limit` habits: rules sorted by display_order, deduped by habit_key or habit_label."""
    matched: list[tuple[QuestionnaireHealthyHabitRule, tuple[int, int]]] = []
    for rule in rules:
        qid = int(rule.question_id)
        definition = definitions_by_id.get(qid)
        if definition is None:
            continue
        answer = answers_by_question_id.get(qid)
        if answer is None:
            continue
        if not _rule_matches(rule=rule, definition=definition, answer=answer):
            continue
        order_key = rule.display_order if rule.display_order is not None else 10**9
        matched.append((rule, (order_key, int(rule.rule_id))))

    matched.sort(key=lambda x: (x[1][0], x[1][1]))

    out: list[HealthyHabitComputed] = []
    seen: set[str] = set()
    for rule, _ in matched:
        key = (rule.habit_key or "").strip()
        dedupe_token = key.lower() if key else f"label:{(rule.habit_label or '').strip().lower()}"
        if dedupe_token in seen:
            continue
        seen.add(dedupe_token)
        out.append(
            HealthyHabitComputed(
                habit_key=key or None,
                habit_label=(rule.habit_label or "").strip() or "Habit",
            )
        )
        if len(out) >= limit:
            break
    return out
