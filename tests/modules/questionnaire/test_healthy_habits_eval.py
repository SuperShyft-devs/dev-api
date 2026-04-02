"""Unit tests for healthy habit rule evaluation."""

from __future__ import annotations

from decimal import Decimal

from modules.questionnaire.healthy_habits_eval import compute_top_healthy_habits
from modules.questionnaire.models import QuestionnaireDefinition, QuestionnaireHealthyHabitRule


def _def(qid: int, qtype: str) -> QuestionnaireDefinition:
    return QuestionnaireDefinition(
        question_id=qid,
        question_key=f"q{qid}",
        question_text="T",
        question_type=qtype,
        status="active",
    )


def test_single_choice_match():
    d = {1: _def(1, "single_choice")}
    rules = [
        QuestionnaireHealthyHabitRule(
            rule_id=1,
            question_id=1,
            habit_label="H1",
            condition_type="option_match",
            matched_option_values=["yes"],
            status="active",
        )
    ]
    out = compute_top_healthy_habits(
        rules=rules,
        definitions_by_id=d,
        answers_by_question_id={1: "yes"},
        limit=3,
    )
    assert len(out) == 1
    assert out[0].habit_label == "H1"


def test_multiple_choice_intersection():
    d = {2: _def(2, "multiple_choice")}
    rules = [
        QuestionnaireHealthyHabitRule(
            rule_id=2,
            question_id=2,
            habit_label="Exercise",
            condition_type="option_match",
            matched_option_values=["gym", "yoga"],
            status="active",
        )
    ]
    out = compute_top_healthy_habits(
        rules=rules,
        definitions_by_id=d,
        answers_by_question_id={2: ["running", "gym"]},
        limit=3,
    )
    assert len(out) == 1


def test_scale_range_respects_unit():
    d = {3: _def(3, "scale")}
    rules = [
        QuestionnaireHealthyHabitRule(
            rule_id=3,
            question_id=3,
            habit_label="Sleep",
            condition_type="scale_range",
            scale_min=Decimal("7"),
            scale_max=Decimal("9"),
            scale_unit="hours",
            status="active",
        )
    ]
    out = compute_top_healthy_habits(
        rules=rules,
        definitions_by_id=d,
        answers_by_question_id={3: {"value": 8, "unit": "hours"}},
        limit=3,
    )
    assert len(out) == 1
    out_bad = compute_top_healthy_habits(
        rules=rules,
        definitions_by_id=d,
        answers_by_question_id={3: {"value": 8, "unit": "minutes"}},
        limit=3,
    )
    assert len(out_bad) == 0


def test_dedupe_by_habit_key():
    d = {4: _def(4, "single_choice")}
    rules = [
        QuestionnaireHealthyHabitRule(
            rule_id=10,
            question_id=4,
            habit_key="same",
            habit_label="A",
            display_order=2,
            condition_type="option_match",
            matched_option_values=["x"],
            status="active",
        ),
        QuestionnaireHealthyHabitRule(
            rule_id=11,
            question_id=4,
            habit_key="same",
            habit_label="B",
            display_order=1,
            condition_type="option_match",
            matched_option_values=["x"],
            status="active",
        ),
    ]
    out = compute_top_healthy_habits(
        rules=rules,
        definitions_by_id=d,
        answers_by_question_id={4: "x"},
        limit=3,
    )
    assert len(out) == 1
    assert out[0].habit_label == "B"


def test_limit_three():
    d = {i: _def(i, "single_choice") for i in range(5, 9)}
    rules = [
        QuestionnaireHealthyHabitRule(
            rule_id=i,
            question_id=i,
            habit_label=f"H{i}",
            display_order=i,
            condition_type="option_match",
            matched_option_values=["ok"],
            status="active",
        )
        for i in range(5, 9)
    ]
    answers = {i: "ok" for i in range(5, 9)}
    out = compute_top_healthy_habits(
        rules=rules,
        definitions_by_id=d,
        answers_by_question_id=answers,
        limit=3,
    )
    assert len(out) == 3
