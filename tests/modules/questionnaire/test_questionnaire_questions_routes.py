"""Integration tests for questionnaire question-definition routes (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.questionnaire.models import QuestionnaireDefinition
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1, role: str = "admin"):
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()  # Ensure user is inserted before employee
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role=role, status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_question_requires_auth(async_client):
    response = await async_client.post(
        "/questionnaire/questions",
        json={"question_text": "Q1", "question_type": "single_choice", "options": ["a", "b"]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_question_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=9001, phone="9001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/questionnaire/questions",
        headers=_auth_header(9001),
        json={"question_text": "Q1", "question_type": "single_choice", "options": ["a", "b"]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_question_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9002, employee_id=10)

    payload = {"question_text": "How are you?", "question_type": "single_choice", "options": ["Good", "Bad"]}
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9002), json=payload)
    assert response.status_code == 201

    question_id = response.json()["data"]["question_id"]
    created = await test_db_session.get(QuestionnaireDefinition, question_id)
    assert created is not None
    assert created.question_text == "How are you?"
    assert created.question_type == "single_choice"
    assert created.options == ["Good", "Bad"]
    assert (created.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_list_questions_paginates_and_filters(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9003, employee_id=11)

    test_db_session.add_all(
        [
            QuestionnaireDefinition(question_id=100, question_text="Q1", question_type="single_choice", options=["a"], status="active"),
            QuestionnaireDefinition(question_id=101, question_text="Q2", question_type="text", options=None, status="inactive"),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/questionnaire/questions?page=1&limit=10&status=active",
        headers=_auth_header(9003),
    )
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert (row["status"] or "").lower() == "active"


@pytest.mark.asyncio
async def test_get_question_returns_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9004, employee_id=12)

    test_db_session.add(
        QuestionnaireDefinition(question_id=200, question_text="Q1", question_type="text", options=None, status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/questions/200", headers=_auth_header(9004))
    assert response.status_code == 200
    assert response.json()["data"]["question_id"] == 200


@pytest.mark.asyncio
async def test_update_question_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9005, employee_id=13)

    test_db_session.add(
        QuestionnaireDefinition(question_id=300, question_text="Old", question_type="text", options=None, status="active")
    )
    await test_db_session.commit()

    payload = {"question_text": "New text", "question_type": "single_choice", "options": ["x", "y"]}
    response = await async_client.put("/questionnaire/questions/300", headers=_auth_header(9005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(QuestionnaireDefinition, 300)
    assert updated is not None
    assert updated.question_text == "New text"
    assert updated.question_type == "single_choice"
    assert updated.options == ["x", "y"]


@pytest.mark.asyncio
async def test_patch_question_status_sets_inactive(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9006, employee_id=14)

    test_db_session.add(
        QuestionnaireDefinition(question_id=400, question_text="Q", question_type="text", options=None, status="active")
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/questionnaire/questions/400/status",
        headers=_auth_header(9006),
        json={"status": "inactive"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(QuestionnaireDefinition, 400)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"


@pytest.mark.asyncio
async def test_get_question_returns_404_when_missing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9007, employee_id=15)

    response = await async_client.get("/questionnaire/questions/999999", headers=_auth_header(9007))
    assert response.status_code == 404
    assert response.json() == {"error_code": "QUESTIONNAIRE_QUESTION_NOT_FOUND", "message": "Question does not exist"}


@pytest.mark.asyncio
async def test_list_questions_rejects_invalid_pagination(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9008, employee_id=16)

    response = await async_client.get("/questionnaire/questions?page=0&limit=10", headers=_auth_header(9008))
    assert response.status_code == 400
    assert response.json() == {"error_code": "INVALID_INPUT", "message": "Invalid request"}
