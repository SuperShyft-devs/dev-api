"""Integration tests for organizations routes (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.organizations.models import Organization
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    user = User(user_id=user_id, phone=f"{user_id}000000000", status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    
    employee = Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    test_db_session.add(employee)
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_organization_requires_auth(async_client):
    response = await async_client.post("/organizations", json={"name": "Org"})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_organization_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=7101, phone="7101000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post("/organizations", headers=_auth_header(7101), json={"name": "Org"})
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_organization_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7102, employee_id=21)

    payload = {
        "name": "Acme",
        "organization_type": "corporate",
        "website_url": "https://example.com",
        "contact_email": "ops@example.com",
        "bd_employee_id": 21,
    }

    response = await async_client.post("/organizations", headers=_auth_header(7102), json=payload)
    assert response.status_code == 201

    organization_id = response.json()["data"]["organization_id"]
    assert isinstance(organization_id, int)

    created = await test_db_session.get(Organization, organization_id)
    assert created is not None
    assert created.name == "Acme"
    assert (created.status or "").lower() == "active"
    assert created.created_employee_id == 21


@pytest.mark.asyncio
async def test_list_organizations_paginates_and_filters(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7103, employee_id=22)

    test_db_session.add(
        Organization(
            organization_id=9001,
            name="O1",
            organization_type="corporate",
            status="active",
        )
    )
    test_db_session.add(
        Organization(
            organization_id=9002,
            name="O2",
            organization_type="ngo",
            status="inactive",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations?page=1&limit=10&status=active", headers=_auth_header(7103))
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert (row["status"] or "").lower() == "active"


@pytest.mark.asyncio
async def test_get_organization_details_returns_details(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7104, employee_id=23)

    test_db_session.add(
        Organization(
            organization_id=9101,
            name="DetailOrg",
            organization_type="corporate",
            website_url="https://detail.example.com",
            contact_email="c@example.com",
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/9101", headers=_auth_header(7104))
    assert response.status_code == 200
    assert response.json()["data"]["organization_id"] == 9101
    assert response.json()["data"]["name"] == "DetailOrg"


@pytest.mark.asyncio
async def test_update_organization_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7105, employee_id=24)

    test_db_session.add(Organization(organization_id=9201, name="Old", status="active"))
    await test_db_session.commit()

    payload = {
        "name": "New",
        "organization_type": "corporate",
        "logo": None,
        "website_url": None,
        "address": None,
        "pin_code": None,
        "city": None,
        "state": None,
        "country": None,
        "contact_name": None,
        "contact_email": None,
        "contact_phone": None,
        "contact_designation": None,
        "bd_employee_id": None,
    }

    response = await async_client.put("/organizations/9201", headers=_auth_header(7105), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Organization, 9201)
    assert updated is not None
    assert updated.name == "New"
    assert updated.updated_employee_id == 24


@pytest.mark.asyncio
async def test_update_organization_status_sets_inactive(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7106, employee_id=25)

    test_db_session.add(Organization(organization_id=9301, name="Org", status="active"))
    await test_db_session.commit()

    response = await async_client.patch(
        "/organizations/9301/status",
        headers=_auth_header(7106),
        json={"status": "inactive"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(Organization, 9301)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"
