"""Integration tests for assessment package category routes."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage, AssessmentPackageCategory
from modules.employee.models import Employee
from modules.questionnaire.models import QuestionnaireCategory
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_package_categories_requires_auth(async_client):
    response = await async_client.get("/assessment-packages/1/categories")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_package_categories_allows_authenticated_user(async_client, test_db_session):
    test_db_session.add(User(user_id=8101, phone="8101000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=6101, package_code="PKU", display_name="User Package", status="active"))
    test_db_session.add(QuestionnaireCategory(category_id=7101, category_key="user_cat", display_name="User Cat"))
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=6101, category_id=7101))
    await test_db_session.commit()
    response = await async_client.get("/assessment-packages/6101/categories", headers=_auth_header(8101))
    assert response.status_code == 200
    assert response.json()["data"][0]["category_id"] == 7101


@pytest.mark.asyncio
async def test_add_and_list_and_remove_package_categories(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8102, employee_id=51)
    test_db_session.add(AssessmentPackage(package_id=6001, package_code="PK", display_name="PK", status="active"))
    test_db_session.add(QuestionnaireCategory(category_id=7001, category_key="wellbeing", display_name="Wellbeing"))
    await test_db_session.commit()

    add_resp = await async_client.post(
        "/assessment-packages/6001/categories",
        headers=_auth_header(8102),
        json={"category_ids": [7001]},
    )
    assert add_resp.status_code == 201
    assert add_resp.json()["data"]["added_category_ids"] == [7001]

    list_resp = await async_client.get("/assessment-packages/6001/categories", headers=_auth_header(8102))
    assert list_resp.status_code == 200
    assert list_resp.json()["data"][0]["category_id"] == 7001

    remove_resp = await async_client.delete("/assessment-packages/6001/categories/7001", headers=_auth_header(8102))
    assert remove_resp.status_code == 200
    assert remove_resp.json()["data"] == {"package_id": 6001, "removed_category_id": 7001}

    check = await test_db_session.execute(
        AssessmentPackageCategory.__table__.select().where(AssessmentPackageCategory.package_id == 6001)
    )
    assert check.first() is None
