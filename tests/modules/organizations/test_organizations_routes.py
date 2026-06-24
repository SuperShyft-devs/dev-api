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
    user = User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active")
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
    test_db_session.add(User(user_id=7101, age=30, phone="7101000000", status="active"))
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
        "contact_person_user_id": None,
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


@pytest.mark.asyncio
async def test_create_organization_with_departments_generates_slugs(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7107, employee_id=26)

    payload = {
        "name": "DeptOrg",
        "departments": [
            {"department": "Sales"},
            {"department": "Marketing"},
        ],
    }

    response = await async_client.post("/organizations", headers=_auth_header(7107), json=payload)
    assert response.status_code == 201

    organization_id = response.json()["data"]["organization_id"]
    created = await test_db_session.get(Organization, organization_id)
    assert created is not None
    assert created.departments == [
        {"department": "Sales", "slug": "sales"},
        {"department": "Marketing", "slug": "marketing"},
    ]

    details = await async_client.get(f"/organizations/{organization_id}", headers=_auth_header(7107))
    assert details.status_code == 200
    assert details.json()["data"]["departments"] == created.departments


@pytest.mark.asyncio
async def test_create_organization_rejects_duplicate_department_names(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7108, employee_id=27)

    payload = {
        "name": "DupDeptOrg",
        "departments": [
            {"department": "Sales"},
            {"department": "sales"},
        ],
    }

    response = await async_client.post("/organizations", headers=_auth_header(7108), json=payload)
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_update_organization_replaces_departments(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7109, employee_id=28)

    test_db_session.add(
        Organization(
            organization_id=9401,
            name="ReplaceDeptOrg",
            status="active",
            departments=[{"department": "Sales", "slug": "sales"}],
        )
    )
    await test_db_session.commit()

    payload = {
        "name": "ReplaceDeptOrg",
        "departments": [{"department": "Engineering"}],
    }

    response = await async_client.put("/organizations/9401", headers=_auth_header(7109), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Organization, 9401)
    assert updated is not None
    assert updated.departments == [{"department": "Engineering", "slug": "engineering"}]


@pytest.mark.asyncio
async def test_create_organization_with_contact_person_creates_organization_manager(
    async_client,
    test_db_session,
):
    await _seed_employee(test_db_session, user_id=7110, employee_id=29)

    contact_user = User(user_id=7111, age=35, phone="7111000000", status="active")
    test_db_session.add(contact_user)
    await test_db_session.commit()

    payload = {
        "name": "ContactPersonOrg",
        "contact_person_user_id": 7111,
    }

    response = await async_client.post("/organizations", headers=_auth_header(7110), json=payload)
    assert response.status_code == 201

    organization_id = response.json()["data"]["organization_id"]
    created = await test_db_session.get(Organization, organization_id)
    assert created is not None
    assert created.contact_person_user_id == 7111

    from sqlalchemy import select

    employee_row = (
        await test_db_session.execute(select(Employee).where(Employee.user_id == 7111))
    ).scalar_one_or_none()
    assert employee_row is not None
    assert employee_row.role == "organization_manager"
    assert (employee_row.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_organization_manager_can_get_own_organization(async_client, test_db_session):
    manager_user = User(user_id=7120, age=30, phone="7120000000", status="active")
    test_db_session.add(manager_user)
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=120, user_id=7120, role="organization_manager", status="active")
    )
    test_db_session.add(
        Organization(
            organization_id=9501,
            name="ManagedOrg",
            status="active",
            contact_person_user_id=7120,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/9501", headers=_auth_header(7120))
    assert response.status_code == 200
    assert response.json()["data"]["contact_person_user_id"] == 7120


@pytest.mark.asyncio
async def test_organization_manager_cannot_get_other_organization(async_client, test_db_session):
    manager_user = User(user_id=7121, age=30, phone="7121000000", status="active")
    other_contact_user = User(user_id=7123, age=30, phone="7123000000", status="active")
    test_db_session.add_all([manager_user, other_contact_user])
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=121, user_id=7121, role="organization_manager", status="active")
    )
    test_db_session.add(
        Organization(
            organization_id=9502,
            name="OtherOrg",
            status="active",
            contact_person_user_id=7123,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/9502", headers=_auth_header(7121))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_organization_manager_cannot_list_organizations(async_client, test_db_session):
    manager_user = User(user_id=7122, age=30, phone="7122000000", status="active")
    test_db_session.add(manager_user)
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=122, user_id=7122, role="organization_manager", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations", headers=_auth_header(7122))
    assert response.status_code == 403
