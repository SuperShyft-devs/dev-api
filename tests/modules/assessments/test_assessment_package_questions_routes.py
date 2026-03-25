"""Integration tests for assessment package category routes."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import (
    AssessmentCategoryProgress,
    AssessmentInstance,
    AssessmentPackage,
    AssessmentPackageCategory,
)
from modules.employee.models import Employee
from modules.engagements.models import Engagement
from modules.diagnostics.models import DiagnosticPackage
from modules.questionnaire.models import QuestionnaireCategory
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_package_categories_requires_auth(async_client):
    response = await async_client.get("/assessment-packages/1/categories")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_package_categories_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=8101, age=30, phone="8101000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=6101, package_code="PKU", display_name="User Package", status="active"))
    test_db_session.add(
        QuestionnaireCategory(category_id=7101, category_key="user_cat", display_name="User Cat", status="active")
    )
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=6101, category_id=7101))
    await test_db_session.commit()
    response = await async_client.get("/assessment-packages/6101/categories", headers=_auth_header(8101))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_package_categories_allows_employee(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8105, employee_id=55)
    test_db_session.add(AssessmentPackage(package_id=6105, package_code="PKE", display_name="Employee Package", status="active"))
    test_db_session.add(
        QuestionnaireCategory(category_id=7105, category_key="emp_cat", display_name="Emp Cat", status="active")
    )
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=6105, category_id=7105))
    await test_db_session.commit()

    response = await async_client.get("/assessment-packages/6105/categories", headers=_auth_header(8105))
    assert response.status_code == 200
    assert response.json()["data"][0]["category_id"] == 7105


@pytest.mark.asyncio
async def test_list_my_package_categories_returns_incomplete_by_default(async_client, test_db_session):
    test_db_session.add(User(user_id=8103, age=30, phone="8103000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=6102, package_code="PKM", display_name="My Package", status="active"))
    test_db_session.add(
        QuestionnaireCategory(category_id=7102, category_key="my_cat", display_name="My Cat", status="active")
    )
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageCategory(package_id=6102, category_id=7102))
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package",
            diagnostic_provider="test_provider",
            no_of_tests=1,
            status="active",
            bookings_count=0,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=6200,
            engagement_name="Test Engagement",
            engagement_code="ENG6200",
            engagement_type="test",
            assessment_package_id=6102,
            diagnostic_package_id=1,
            status="active",
            participant_count=1,
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=6201,
            user_id=8103,
            package_id=6102,
            engagement_id=6200,
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/assessment-packages/me/6102/categories", headers=_auth_header(8103))
    assert response.status_code == 200
    assert response.json()["data"][0]["status"] == "incomplete"


@pytest.mark.asyncio
async def test_list_my_package_categories_returns_complete_from_progress(async_client, test_db_session):
    test_db_session.add(User(user_id=8104, age=30, phone="8104000000", status="active"))
    test_db_session.add(AssessmentPackage(package_id=6103, package_code="PKC", display_name="Complete Package", status="active"))
    test_db_session.add(
        QuestionnaireCategory(category_id=7103, category_key="complete_cat", display_name="Complete Cat", status="active")
    )
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageCategory(package_id=6103, category_id=7103))
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package",
            diagnostic_provider="test_provider",
            no_of_tests=1,
            status="active",
            bookings_count=0,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=6300,
            engagement_name="Test Engagement",
            engagement_code="ENG6300",
            engagement_type="test",
            assessment_package_id=6103,
            diagnostic_package_id=1,
            status="active",
            participant_count=1,
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=6202,
            user_id=8104,
            package_id=6103,
            engagement_id=6300,
            status="active",
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        AssessmentCategoryProgress(
            assessment_instance_id=6202,
            category_id=7103,
            status="complete",
            completed_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/assessment-packages/me/6103/categories", headers=_auth_header(8104))
    assert response.status_code == 200
    assert response.json()["data"][0]["status"] == "complete"


@pytest.mark.asyncio
async def test_add_and_list_and_remove_package_categories(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8102, employee_id=51)
    test_db_session.add(AssessmentPackage(package_id=6001, package_code="PK", display_name="PK", status="active"))
    test_db_session.add(
        QuestionnaireCategory(category_id=7001, category_key="wellbeing", display_name="Wellbeing", status="active")
    )
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


@pytest.mark.asyncio
async def test_reorder_package_categories_requires_auth(async_client):
    response = await async_client.patch(
        "/assessment-packages/1/categories/order",
        json={"category_ids": [1]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_reorder_package_categories_persists_order(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8110, employee_id=60)
    test_db_session.add(AssessmentPackage(package_id=6110, package_code="PK10", display_name="Package 10", status="active"))
    test_db_session.add_all(
        [
            QuestionnaireCategory(category_id=7110, category_key="cat_a", display_name="Cat A", status="active"),
            QuestionnaireCategory(category_id=7111, category_key="cat_b", display_name="Cat B", status="active"),
        ]
    )
    await test_db_session.commit()
    test_db_session.add_all(
        [
            AssessmentPackageCategory(id=61101, package_id=6110, category_id=7110, display_order=1),
            AssessmentPackageCategory(id=61102, package_id=6110, category_id=7111, display_order=2),
        ]
    )
    await test_db_session.commit()

    reorder_resp = await async_client.patch(
        "/assessment-packages/6110/categories/order",
        headers=_auth_header(8110),
        json={"category_ids": [7111, 7110]},
    )
    assert reorder_resp.status_code == 200
    assert reorder_resp.json()["data"]["category_ids"] == [7111, 7110]

    list_resp = await async_client.get("/assessment-packages/6110/categories", headers=_auth_header(8110))
    assert list_resp.status_code == 200
    assert [row["category_id"] for row in list_resp.json()["data"]] == [7111, 7110]


@pytest.mark.asyncio
async def test_reorder_package_categories_rejects_invalid_ids(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8111, employee_id=61)
    test_db_session.add(AssessmentPackage(package_id=6111, package_code="PK11", display_name="Package 11", status="active"))
    test_db_session.add_all(
        [
            QuestionnaireCategory(category_id=7120, category_key="cat_x", display_name="Cat X", status="active"),
            QuestionnaireCategory(category_id=7121, category_key="cat_y", display_name="Cat Y", status="active"),
        ]
    )
    await test_db_session.commit()
    test_db_session.add_all(
        [
            AssessmentPackageCategory(id=61201, package_id=6111, category_id=7120, display_order=1),
            AssessmentPackageCategory(id=61202, package_id=6111, category_id=7121, display_order=2),
        ]
    )
    await test_db_session.commit()

    duplicate_resp = await async_client.patch(
        "/assessment-packages/6111/categories/order",
        headers=_auth_header(8111),
        json={"category_ids": [7120, 7120]},
    )
    assert duplicate_resp.status_code == 400
    assert duplicate_resp.json()["error_code"] == "INVALID_INPUT"

    missing_resp = await async_client.patch(
        "/assessment-packages/6111/categories/order",
        headers=_auth_header(8111),
        json={"category_ids": [7120]},
    )
    assert missing_resp.status_code == 400
    assert missing_resp.json()["error_code"] == "INVALID_INPUT"
