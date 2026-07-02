"""Tests for POST /questionnaire/blood-parameters/reload."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1, role: str = "admin"):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role=role, status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_reload_blood_parameters_requires_auth(async_client):
    response = await async_client.post("/questionnaire/blood-parameters/reload")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_reload_blood_parameters_creates_haemoglobin_scale_question(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9101, employee_id=101)

    response = await async_client.post(
        "/questionnaire/blood-parameters/reload",
        headers=_auth_header(9101),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["questions_created"] == 49
    assert data["questions_deleted"] == 0

    result = await test_db_session.execute(
        select(QuestionnaireDefinition).where(QuestionnaireDefinition.question_key == "haemoglobin")
    )
    row = result.scalar_one_or_none()
    assert row is not None
    assert row.question_type == "scale"
    assert row.question_text == "Haemoglobin"
    assert row.metsights_sync is not None
    assert row.metsights_sync["push"]["strategy"] == "scale_emit"

    opts = await test_db_session.execute(
        select(QuestionnaireOption).where(QuestionnaireOption.question_id == row.question_id)
    )
    options = list(opts.scalars().all())
    assert len(options) == 1
    assert options[0].option_value == "0"
    assert options[0].display_name == "g/dL"


@pytest.mark.asyncio
async def test_reload_blood_parameters_is_idempotent(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9102, employee_id=102)

    first = await async_client.post(
        "/questionnaire/blood-parameters/reload",
        headers=_auth_header(9102),
    )
    assert first.status_code == 200
    first_id = (
        await test_db_session.execute(
            select(QuestionnaireDefinition.question_id).where(
                QuestionnaireDefinition.question_key == "haemoglobin"
            )
        )
    ).scalar_one()

    second = await async_client.post(
        "/questionnaire/blood-parameters/reload",
        headers=_auth_header(9102),
    )
    assert second.status_code == 200
    second_data = second.json()["data"]
    assert second_data["questions_deleted"] == 49
    assert second_data["questions_created"] == 49

    new_id = (
        await test_db_session.execute(
            select(QuestionnaireDefinition.question_id).where(
                QuestionnaireDefinition.question_key == "haemoglobin"
            )
        )
    ).scalar_one()
    assert new_id != first_id


@pytest.mark.asyncio
async def test_reload_blood_parameters_deletes_existing_responses(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9103, employee_id=103)

    test_db_session.add(
        QuestionnaireDefinition(
            question_id=91031,
            question_key="haemoglobin",
            question_text="Old Haemoglobin",
            question_type="scale",
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireCategory(
            category_id=91031,
            category_key="blood-parameters",
            display_name="Blood Parameters",
            category_of="metsights",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        QuestionnaireResponse(
            response_id=91031,
            assessment_instance_id=1,
            question_id=91031,
            category_id=91031,
            answer={"value": 12.5, "unit": "0"},
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/questionnaire/blood-parameters/reload",
        headers=_auth_header(9103),
    )
    assert response.status_code == 200
    assert response.json()["data"]["responses_deleted"] >= 1

    remaining = await test_db_session.execute(
        select(QuestionnaireResponse).where(QuestionnaireResponse.response_id == 91031)
    )
    assert remaining.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_reload_blood_parameters_creates_metsights_categories(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9104, employee_id=104)

    response = await async_client.post(
        "/questionnaire/blood-parameters/reload",
        headers=_auth_header(9104),
    )
    assert response.status_code == 200

    for key in ("blood-parameters", "advanced-blood-parameters"):
        result = await test_db_session.execute(
            select(QuestionnaireCategory).where(
                QuestionnaireCategory.category_key == key,
                QuestionnaireCategory.category_of == "metsights",
            )
        )
        assert result.scalar_one_or_none() is not None
