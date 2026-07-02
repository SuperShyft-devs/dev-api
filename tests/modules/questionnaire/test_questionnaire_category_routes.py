"""Integration tests for questionnaire category routes."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.questionnaire.models import QuestionnaireCategory
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
async def test_create_category_rejects_duplicate_key_across_category_of(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9201, employee_id=201)

    first = await async_client.post(
        "/questionnaire/categories",
        headers=_auth_header(9201),
        json={
            "category_key": "duplicate-key-test",
            "display_name": "Metsights Category",
            "category_of": "metsights",
        },
    )
    assert first.status_code == 201

    second = await async_client.post(
        "/questionnaire/categories",
        headers=_auth_header(9201),
        json={
            "category_key": "duplicate-key-test",
            "display_name": "Supershyft Category",
            "category_of": "supershyft",
        },
    )
    assert second.status_code == 409
    assert second.json()["error_code"] == "QUESTIONNAIRE_CATEGORY_EXISTS"


@pytest.mark.asyncio
async def test_reset_metsights_sync_creates_vitals_category(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9202, employee_id=202)

    response = await async_client.post(
        "/questionnaire/metsights-sync/reset",
        headers=_auth_header(9202),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["categories_total"] >= 1

    result = await test_db_session.execute(
        select(QuestionnaireCategory).where(
            QuestionnaireCategory.category_key == "vitals",
            QuestionnaireCategory.category_of == "metsights",
        )
    )
    vitals = result.scalar_one_or_none()
    assert vitals is not None
    assert vitals.display_name == "Vitals"
    assert (vitals.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_seed_supershyft_vitals_renamed_to_health_vitals(test_db_session):
    result = await test_db_session.execute(
        select(QuestnaireCategory).where(QuestionnaireCategory.category_id == 5)
    )
    category = result.scalar_one_or_none()
    assert category is not None
    assert category.category_key == "health_vitals"
    assert category.display_name == "Vitals"
    assert category.category_of == "supershyft"
