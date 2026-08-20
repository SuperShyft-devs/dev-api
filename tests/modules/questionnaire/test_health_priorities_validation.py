"""Unit tests for health_priorities answer coercion and validation."""

from __future__ import annotations

import pytest

from core.exceptions import AppError
from modules.questionnaire.service import QuestionnaireService, _coerce_answer_for_question
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository


def _health_priorities_question() -> dict:
    return {
        "question_key": "health_priorities",
        "question_type": "multiple_choice",
        "options": [
            {"option_value": "0", "display_name": "Weight loss"},
            {"option_value": "1", "display_name": "Building muscle mass"},
            {"option_value": "2", "display_name": "Improving metabolic health"},
        ],
    }


def test_coerce_health_priorities_legacy_string_to_list():
    assert _coerce_answer_for_question("health_priorities", "2") == ["2"]
    assert _coerce_answer_for_question("health_priorities", "  ") == []


def test_validate_health_priorities_accepts_up_to_two():
    svc = QuestionnaireService(
        repository=QuestionnaireRepository(),
        users_repository=UsersRepository(),
    )
    svc._validate_answer_by_type(
        question=_health_priorities_question(),
        answer=["0", "1"],
    )


def test_validate_health_priorities_rejects_more_than_two():
    svc = QuestionnaireService(
        repository=QuestionnaireRepository(),
        users_repository=UsersRepository(),
    )
    with pytest.raises(AppError) as exc:
        svc._validate_answer_by_type(
            question=_health_priorities_question(),
            answer=["0", "1", "2"],
        )
    assert exc.value.error_code == "INVALID_STATE"
    assert "at most 2" in str(exc.value.message).lower()


def test_validate_health_priorities_coerces_legacy_string_before_check():
    svc = QuestionnaireService(
        repository=QuestionnaireRepository(),
        users_repository=UsersRepository(),
    )
    svc._validate_answer_by_type(
        question=_health_priorities_question(),
        answer="1",
    )
