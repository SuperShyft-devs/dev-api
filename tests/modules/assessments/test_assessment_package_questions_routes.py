"""Integration tests for assessment package question routes (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage, AssessmentPackageQuestion
from modules.employee.models import Employee
from modules.questionnaire.models import QuestionnaireDefinition
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()  # Ensure user is inserted before employee
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_package_questions_requires_auth(async_client):
    response = await async_client.get("/assessment-packages/1/questions")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_package_questions_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=8101, phone="8101000000", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/assessment-packages/1/questions", headers=_auth_header(8101))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_package_questions_returns_questions(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8102, employee_id=51)

    test_db_session.add(AssessmentPackage(package_id=6001, package_code="PK", display_name="PK", status="active"))
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=7001,
            question_text="Q1",
            question_type="text",
            options=None,
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=7002,
            question_text="Q2",
            question_type="single_choice",
            options=["a", "b"],
            status="active",
        )
    )
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageQuestion(id=1, package_id=6001, question_id=7001))
    test_db_session.add(AssessmentPackageQuestion(id=2, package_id=6001, question_id=7002))
    await test_db_session.commit()

    response = await async_client.get("/assessment-packages/6001/questions", headers=_auth_header(8102))
    assert response.status_code == 200

    body = response.json()["data"]
    assert isinstance(body, list)
    assert {row["question_id"] for row in body} == {7001, 7002}


@pytest.mark.asyncio
async def test_add_questions_to_package_creates_links(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8103, employee_id=52)

    test_db_session.add(AssessmentPackage(package_id=6101, package_code="PK2", display_name="PK2", status="active"))
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=7101,
            question_text="Q",
            question_type="text",
            options=None,
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/assessment-packages/6101/questions",
        headers=_auth_header(8103),
        json={"question_ids": [7101]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert data["package_id"] == 6101
    assert data["added_question_ids"] == [7101]
    assert data["skipped_question_ids"] == []

    result = await test_db_session.execute(
        AssessmentPackageQuestion.__table__.select().where(AssessmentPackageQuestion.package_id == 6101)
    )
    rows = list(result.all())
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_add_questions_to_package_skips_duplicates(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8104, employee_id=53)

    test_db_session.add(AssessmentPackage(package_id=6201, package_code="PK3", display_name="PK3", status="active"))
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=7201,
            question_text="Q",
            question_type="text",
            options=None,
            status="active",
        )
    )
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageQuestion(id=10, package_id=6201, question_id=7201))
    await test_db_session.commit()

    response = await async_client.post(
        "/assessment-packages/6201/questions",
        headers=_auth_header(8104),
        json={"question_ids": [7201, 7201]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert data["added_question_ids"] == []
    assert data["skipped_question_ids"] == [7201]


@pytest.mark.asyncio
async def test_remove_question_from_package_deletes_link(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8105, employee_id=54)

    test_db_session.add(AssessmentPackage(package_id=6301, package_code="PK4", display_name="PK4", status="active"))
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=7301,
            question_text="Q",
            question_type="text",
            options=None,
            status="active",
        )
    )
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageQuestion(id=20, package_id=6301, question_id=7301))
    await test_db_session.commit()

    response = await async_client.delete(
        "/assessment-packages/6301/questions/7301",
        headers=_auth_header(8105),
    )
    assert response.status_code == 200

    data = response.json()["data"]
    assert data == {"package_id": 6301, "removed_question_id": 7301}

    link = await test_db_session.execute(
        AssessmentPackageQuestion.__table__.select().where(
            (AssessmentPackageQuestion.package_id == 6301) & (AssessmentPackageQuestion.question_id == 7301)
        )
    )
    assert link.first() is None


@pytest.mark.asyncio
async def test_remove_question_from_package_returns_404_when_missing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8106, employee_id=55)

    test_db_session.add(AssessmentPackage(package_id=6401, package_code="PK5", display_name="PK5", status="active"))
    await test_db_session.commit()

    response = await async_client.delete(
        "/assessment-packages/6401/questions/99999",
        headers=_auth_header(8106),
    )
    assert response.status_code == 404
    assert response.json() == {
        "error_code": "ASSESSMENT_PACKAGE_QUESTION_NOT_FOUND",
        "message": "Question is not attached to this package",
    }


@pytest.mark.asyncio
async def test_add_questions_to_package_returns_404_when_question_missing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8107, employee_id=56)

    test_db_session.add(AssessmentPackage(package_id=6501, package_code="PK6", display_name="PK6", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/assessment-packages/6501/questions",
        headers=_auth_header(8107),
        json={"question_ids": [999999]},
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "QUESTIONNAIRE_QUESTION_NOT_FOUND", "message": "Question does not exist"}


@pytest.mark.asyncio
async def test_list_package_questions_returns_404_when_package_missing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8108, employee_id=57)

    response = await async_client.get("/assessment-packages/999999/questions", headers=_auth_header(8108))
    assert response.status_code == 404
    assert response.json() == {"error_code": "ASSESSMENT_PACKAGE_NOT_FOUND", "message": "Package does not exist"}
