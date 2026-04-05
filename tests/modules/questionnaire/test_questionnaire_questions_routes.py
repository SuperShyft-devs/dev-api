"""Integration tests for questionnaire question-definition routes (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
)
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1, role: str = "admin"):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()  # Ensure user is inserted before employee
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role=role, status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_question_requires_auth(async_client):
    response = await async_client.post(
        "/questionnaire/questions",
        json={
            "question_key": "q1_auth",
            "question_text": "Q1",
            "question_type": "single_choice",
            "options": [
                {"option_value": "a", "display_name": "a", "tooltip_text": None},
                {"option_value": "b", "display_name": "b", "tooltip_text": None},
            ],
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_question_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=9001, age=30, phone="9001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/questionnaire/questions",
        headers=_auth_header(9001),
        json={
            "question_key": "q1_employee",
            "question_text": "Q1",
            "question_type": "single_choice",
            "options": [
                {"option_value": "a", "display_name": "a", "tooltip_text": None},
                {"option_value": "b", "display_name": "b", "tooltip_text": None},
            ],
        },
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_question_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9002, employee_id=10)

    payload = {
        "question_key": "pytest_how_are_you",
        "question_text": "How are you?",
        "question_type": "single_choice",
        "options": [
            {"option_value": "Good", "display_name": "Good", "tooltip_text": None},
            {"option_value": "Bad", "display_name": "Bad", "tooltip_text": None},
        ],
    }
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9002), json=payload)
    assert response.status_code == 201

    question_id = response.json()["data"]["question_id"]
    created = await test_db_session.get(QuestionnaireDefinition, question_id)
    assert created is not None
    assert created.question_text == "How are you?"
    assert created.question_type == "single_choice"
    from sqlalchemy import select
    opts = await test_db_session.execute(
        select(QuestionnaireOption).where(QuestionnaireOption.question_id == question_id)
    )
    assert {o.option_value for o in opts.scalars().all()} == {"Good", "Bad"}
    assert (created.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_create_question_persists_visibility_rules_and_prefill(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9012, employee_id=20)
    payload = {
        "question_key": "coffee_cups",
        "question_text": "How many cups?",
        "question_type": "number",
        "visibility_rules": {
            "match": "all",
            "conditions": [
                {
                    "type": "question_answer",
                    "operator": "equals",
                    "question_key": "consume_coffee_or_tea",
                    "value": "yes",
                }
            ],
        },
        "prefill_from": {"source": "user_preference", "preference_key": "diet_preference"},
    }
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9012), json=payload)
    assert response.status_code == 201
    question_id = response.json()["data"]["question_id"]
    created = await test_db_session.get(QuestionnaireDefinition, question_id)
    assert created is not None
    assert created.visibility_rules["match"] == "all"
    assert created.prefill_from["preference_key"] == "diet_preference"


@pytest.mark.asyncio
async def test_create_question_rejects_invalid_visibility_rules(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9013, employee_id=21)
    payload = {
        "question_key": "invalid_rule_q",
        "question_text": "Invalid rule test",
        "question_type": "text",
        "visibility_rules": {
            "match": "all",
            "conditions": [{"type": "question_answer", "operator": "equals", "value": "yes"}],
        },
    }
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9013), json=payload)
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_list_questions_paginates_and_filters(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9003, employee_id=11)

    test_db_session.add_all(
        [
            QuestionnaireDefinition(question_id=100, question_key="q100", question_text="Q1", question_type="single_choice", status="active"),
            QuestionnaireDefinition(question_id=101, question_key="q101", question_text="Q2", question_type="text", status="inactive"),
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
        QuestionnaireDefinition(question_id=200, question_key="q200", question_text="Q1", question_type="text", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/questions/200", headers=_auth_header(9004))
    assert response.status_code == 200
    assert response.json()["data"]["question_id"] == 200


@pytest.mark.asyncio
async def test_update_question_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9005, employee_id=13)

    test_db_session.add(
        QuestionnaireDefinition(question_id=300, question_key="q300", question_text="Old", question_type="text", status="active")
    )
    await test_db_session.commit()

    payload = {
        "question_key": "q300_new",
        "question_text": "New text",
        "question_type": "single_choice",
        "options": [
            {"option_value": "x", "display_name": "x", "tooltip_text": None},
            {"option_value": "y", "display_name": "y", "tooltip_text": None},
        ],
    }
    response = await async_client.put("/questionnaire/questions/300", headers=_auth_header(9005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(QuestionnaireDefinition, 300)
    assert updated is not None
    assert updated.question_text == "New text"
    assert updated.question_type == "single_choice"
    from sqlalchemy import select
    opts = await test_db_session.execute(
        select(QuestionnaireOption).where(QuestionnaireOption.question_id == 300)
    )
    assert [o.option_value for o in opts.scalars().all()] == ["x", "y"]


@pytest.mark.asyncio
async def test_create_scale_question_requires_units(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9014, employee_id=22)
    payload = {
        "question_key": "height_measurement",
        "question_text": "What is your height?",
        "question_type": "scale",
        "options": [],
    }
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9014), json=payload)
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_create_scale_question_persists_units(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9015, employee_id=23)
    payload = {
        "question_key": "weight_measurement",
        "question_text": "What is your weight?",
        "question_type": "scale",
        "options": [
            {"option_value": "kg", "display_name": "Kilograms", "tooltip_text": None},
            {"option_value": "lb", "display_name": "Pounds", "tooltip_text": None},
        ],
    }
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9015), json=payload)
    assert response.status_code == 201
    question_id = response.json()["data"]["question_id"]
    from sqlalchemy import select
    opts = await test_db_session.execute(
        select(QuestionnaireOption).where(QuestionnaireOption.question_id == question_id)
    )
    assert [o.option_value for o in opts.scalars().all()] == ["kg", "lb"]


@pytest.mark.asyncio
async def test_create_question_multi_choice_alias_normalized(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9016, employee_id=24)
    payload = {
        "question_key": "food_choices",
        "question_text": "Pick your foods",
        "question_type": "multi_choice",
        "options": [
            {"option_value": "veg", "display_name": "Veg", "tooltip_text": None},
            {"option_value": "non_veg", "display_name": "Non Veg", "tooltip_text": None},
        ],
    }
    response = await async_client.post("/questionnaire/questions", headers=_auth_header(9016), json=payload)
    assert response.status_code == 201
    question_id = response.json()["data"]["question_id"]
    created = await test_db_session.get(QuestionnaireDefinition, question_id)
    assert created is not None
    assert created.question_type == "multiple_choice"


@pytest.mark.asyncio
async def test_patch_question_status_sets_inactive(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9006, employee_id=14)

    test_db_session.add(
        QuestionnaireDefinition(question_id=400, question_key="q400", question_text="Q", question_type="text", status="active")
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


@pytest.mark.asyncio
async def test_reorder_category_questions_requires_auth(async_client):
    response = await async_client.patch(
        "/questionnaire/categories/1/questions/order",
        json={"question_ids": [1]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_reorder_category_questions_persists_display_order(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9010, employee_id=18)

    test_db_session.add(
        QuestionnaireCategory(category_id=8100, category_key="cat_8100", display_name="Category 8100", status="active")
    )
    test_db_session.add_all(
        [
            QuestionnaireDefinition(
                question_id=9101,
                question_key="q9101",
                question_text="Question one",
                question_type="text",
                status="active",
            ),
            QuestionnaireDefinition(
                question_id=9102,
                question_key="q9102",
                question_text="Question two",
                question_type="text",
                status="active",
            ),
        ]
    )
    await test_db_session.commit()
    test_db_session.add_all(
        [
            QuestionnaireCategoryQuestion(
                id=10101,
                category_id=8100,
                question_id=9101,
                display_order=1,
            ),
            QuestionnaireCategoryQuestion(
                id=10102,
                category_id=8100,
                question_id=9102,
                display_order=2,
            ),
        ]
    )
    await test_db_session.commit()

    reorder_response = await async_client.patch(
        "/questionnaire/categories/8100/questions/order",
        headers=_auth_header(9010),
        json={"question_ids": [9102, 9101]},
    )
    assert reorder_response.status_code == 200
    assert reorder_response.json()["data"]["question_ids"] == [9102, 9101]

    list_response = await async_client.get(
        "/questionnaire/categories/8100/questions",
        headers=_auth_header(9010),
    )
    assert list_response.status_code == 200
    data = list_response.json()["data"]
    assert [row["question_id"] for row in data] == [9102, 9101]


@pytest.mark.asyncio
async def test_reorder_category_questions_rejects_invalid_ids(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9011, employee_id=19)

    test_db_session.add(
        QuestionnaireCategory(category_id=8101, category_key="cat_8101", display_name="Category 8101", status="active")
    )
    test_db_session.add_all(
        [
            QuestionnaireDefinition(
                question_id=9201,
                question_key="q9201",
                question_text="Question one",
                question_type="text",
                status="active",
            ),
            QuestionnaireDefinition(
                question_id=9202,
                question_key="q9202",
                question_text="Question two",
                question_type="text",
                status="active",
            ),
        ]
    )
    await test_db_session.commit()
    test_db_session.add_all(
        [
            QuestionnaireCategoryQuestion(id=10201, category_id=8101, question_id=9201, display_order=1),
            QuestionnaireCategoryQuestion(id=10202, category_id=8101, question_id=9202, display_order=2),
        ]
    )
    await test_db_session.commit()

    duplicate_response = await async_client.patch(
        "/questionnaire/categories/8101/questions/order",
        headers=_auth_header(9011),
        json={"question_ids": [9201, 9201]},
    )
    assert duplicate_response.status_code == 400
    assert duplicate_response.json()["error_code"] == "INVALID_INPUT"

    missing_response = await async_client.patch(
        "/questionnaire/categories/8101/questions/order",
        headers=_auth_header(9011),
        json={"question_ids": [9201]},
    )
    assert missing_response.status_code == 400
    assert missing_response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_create_healthy_habit_rule_rejects_text_question(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9020, employee_id=20)
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=9301,
            question_key="q_text_habit",
            question_text="Free text",
            question_type="text",
            status="active",
        )
    )
    await test_db_session.commit()
    response = await async_client.post(
        "/questionnaire/questions/9301/healthy-habit-rules",
        headers=_auth_header(9020),
        json={
            "habit_label": "X",
            "condition_type": "option_match",
            "matched_option_values": ["a"],
            "status": "active",
        },
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_healthy_habit_rules_crud(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9021, employee_id=21)
    test_db_session.add_all(
        [
            QuestionnaireDefinition(
                question_id=9302,
                question_key="q_habit_crud",
                question_text="Pick",
                question_type="single_choice",
                status="active",
            ),
            QuestionnaireOption(question_id=9302, option_value="opt_a", display_name="A"),
        ]
    )
    await test_db_session.commit()

    create_res = await async_client.post(
        "/questionnaire/questions/9302/healthy-habit-rules",
        headers=_auth_header(9021),
        json={
            "habit_key": "good_pick",
            "habit_label": "Good pick",
            "display_order": 1,
            "condition_type": "option_match",
            "matched_option_values": ["opt_a"],
            "status": "active",
        },
    )
    assert create_res.status_code == 201
    body = create_res.json()["data"]
    rule_id = body["rule_id"]
    assert body["habit_label"] == "Good pick"

    list_res = await async_client.get("/questionnaire/questions/9302/healthy-habit-rules", headers=_auth_header(9021))
    assert list_res.status_code == 200
    assert len(list_res.json()["data"]) == 1

    update_res = await async_client.put(
        f"/questionnaire/questions/9302/healthy-habit-rules/{rule_id}",
        headers=_auth_header(9021),
        json={
            "habit_key": "good_pick",
            "habit_label": "Good pick updated",
            "display_order": 2,
            "condition_type": "option_match",
            "matched_option_values": ["opt_a"],
            "status": "inactive",
        },
    )
    assert update_res.status_code == 200
    assert update_res.json()["data"]["habit_label"] == "Good pick updated"
    assert update_res.json()["data"]["status"] == "inactive"

    delete_res = await async_client.delete(
        f"/questionnaire/questions/9302/healthy-habit-rules/{rule_id}",
        headers=_auth_header(9021),
    )
    assert delete_res.status_code == 200
    assert delete_res.json()["data"]["deleted"] is True
