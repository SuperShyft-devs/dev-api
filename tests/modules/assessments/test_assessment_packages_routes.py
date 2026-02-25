"""Integration tests for assessment package routes (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage
from modules.employee.models import Employee
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
async def test_create_package_requires_auth(async_client):
    response = await async_client.post(
        "/assessment-packages",
        json={"package_code": "P1", "display_name": "P1", "status": "active"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_package_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=8001, phone="8001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/assessment-packages",
        headers=_auth_header(8001),
        json={"package_code": "P1", "display_name": "P1", "status": "active"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_package_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8002, employee_id=31)

    response = await async_client.post(
        "/assessment-packages",
        headers=_auth_header(8002),
        json={"package_code": "BASIC", "display_name": "Basic", "status": "active"},
    )
    assert response.status_code == 201

    package_id = response.json()["data"]["package_id"]
    assert isinstance(package_id, int)

    created = await test_db_session.get(AssessmentPackage, package_id)
    assert created is not None
    assert created.package_code == "BASIC"
    assert created.display_name == "Basic"
    assert (created.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_create_package_rejects_duplicate_code(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8003, employee_id=32)

    test_db_session.add(AssessmentPackage(package_id=5001, package_code="DUP", display_name="Dup", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/assessment-packages",
        headers=_auth_header(8003),
        json={"package_code": "DUP", "display_name": "Dup2", "status": "active"},
    )
    assert response.status_code == 409
    assert response.json() == {"error_code": "ASSESSMENT_PACKAGE_ALREADY_EXISTS", "message": "Package already exists"}


@pytest.mark.asyncio
async def test_list_packages_paginates_and_filters(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8004, employee_id=33)

    test_db_session.add(AssessmentPackage(package_id=5101, package_code="A", display_name="A", status="active"))
    test_db_session.add(AssessmentPackage(package_id=5102, package_code="B", display_name="B", status="inactive"))
    await test_db_session.commit()

    response = await async_client.get(
        "/assessment-packages?page=1&limit=10&status=active",
        headers=_auth_header(8004),
    )
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert (row["status"] or "").lower() == "active"


@pytest.mark.asyncio
async def test_get_package_details_returns_details(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8005, employee_id=34)

    test_db_session.add(AssessmentPackage(package_id=5201, package_code="P2", display_name="P2", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/assessment-packages/5201", headers=_auth_header(8005))
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["package_id"] == 5201
    assert data["package_code"] == "P2"


@pytest.mark.asyncio
async def test_update_package_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8006, employee_id=35)

    test_db_session.add(AssessmentPackage(package_id=5301, package_code="OLD", display_name="Old", status="active"))
    await test_db_session.commit()

    response = await async_client.put(
        "/assessment-packages/5301",
        headers=_auth_header(8006),
        json={"package_code": "NEW", "display_name": "New"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(AssessmentPackage, 5301)
    assert updated is not None
    assert updated.package_code == "NEW"
    assert updated.display_name == "New"


@pytest.mark.asyncio
async def test_update_package_rejects_duplicate_code(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8007, employee_id=36)

    test_db_session.add(AssessmentPackage(package_id=5401, package_code="CODE1", display_name="One", status="active"))
    test_db_session.add(AssessmentPackage(package_id=5402, package_code="CODE2", display_name="Two", status="active"))
    await test_db_session.commit()

    response = await async_client.put(
        "/assessment-packages/5402",
        headers=_auth_header(8007),
        json={"package_code": "CODE1", "display_name": "Two"},
    )
    assert response.status_code == 409
    assert response.json() == {"error_code": "ASSESSMENT_PACKAGE_ALREADY_EXISTS", "message": "Package already exists"}
