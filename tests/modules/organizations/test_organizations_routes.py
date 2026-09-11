"""Integration tests for organizations routes (employee-only)."""

from __future__ import annotations

import pytest

from modules.employee.models import Employee
from modules.organizations.models import Organization
from modules.partners.models import Partner
from modules.users.models import User
from tests.helpers.auth import (
    employee_auth_header,
    partner_auth_header,
    seed_employee,
    seed_partner,
    user_auth_header,
)


async def _seed_employee(test_db_session, *, employee_id: int, role: str = "admin"):
    await seed_employee(test_db_session, employee_id=employee_id, role=role)


@pytest.mark.asyncio
async def test_create_organization_requires_auth(async_client):
    response = await async_client.post("/organizations", json={"name": "Org"})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_organization_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=7101, age=30, phone="7101000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/organizations",
        headers=user_auth_header(7101),
        json={"name": "Org"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_organization_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=21)

    payload = {
        "name": "Acme",
        "organization_type": "corporate",
        "website_url": "https://example.com",
        "bd_employee_id": 21,
    }

    response = await async_client.post("/organizations", headers=employee_auth_header(21), json=payload)
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
    await _seed_employee(test_db_session, employee_id=22)

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

    response = await async_client.get(
        "/organizations?page=1&limit=10&status=active",
        headers=employee_auth_header(22),
    )
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert (row["status"] or "").lower() == "active"


@pytest.mark.asyncio
async def test_get_organization_details_returns_details(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=23)

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

    response = await async_client.get("/organizations/9101", headers=employee_auth_header(23))
    assert response.status_code == 200
    assert response.json()["data"]["organization_id"] == 9101
    assert response.json()["data"]["name"] == "DetailOrg"


@pytest.mark.asyncio
async def test_update_organization_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=24)

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
        "contact_person_user_ids": None,
        "bd_employee_id": None,
    }

    response = await async_client.put("/organizations/9201", headers=employee_auth_header(24), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Organization, 9201)
    assert updated is not None
    assert updated.name == "New"
    assert updated.updated_employee_id == 24


@pytest.mark.asyncio
async def test_update_organization_status_sets_inactive(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=25)

    test_db_session.add(Organization(organization_id=9301, name="Org", status="active"))
    await test_db_session.commit()

    response = await async_client.patch(
        "/organizations/9301/status",
        headers=employee_auth_header(25),
        json={"status": "inactive"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(Organization, 9301)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"


@pytest.mark.asyncio
async def test_create_organization_with_departments_generates_slugs(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=26)

    payload = {
        "name": "DeptOrg",
        "departments": [
            {"department": "Sales"},
            {"department": "Marketing"},
        ],
    }

    response = await async_client.post("/organizations", headers=employee_auth_header(26), json=payload)
    assert response.status_code == 201

    organization_id = response.json()["data"]["organization_id"]
    created = await test_db_session.get(Organization, organization_id)
    assert created is not None
    assert created.departments == [
        {"department": "Sales", "slug": "sales"},
        {"department": "Marketing", "slug": "marketing"},
    ]

    details = await async_client.get(
        f"/organizations/{organization_id}",
        headers=employee_auth_header(26),
    )
    assert details.status_code == 200
    assert details.json()["data"]["departments"] == created.departments


@pytest.mark.asyncio
async def test_create_organization_rejects_duplicate_department_names(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=27)

    payload = {
        "name": "DupDeptOrg",
        "departments": [
            {"department": "Sales"},
            {"department": "sales"},
        ],
    }

    response = await async_client.post("/organizations", headers=employee_auth_header(27), json=payload)
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_update_organization_replaces_departments(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=28)

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

    response = await async_client.put("/organizations/9401", headers=employee_auth_header(28), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Organization, 9401)
    assert updated is not None
    assert updated.departments == [{"department": "Engineering", "slug": "engineering"}]


@pytest.mark.asyncio
async def test_create_organization_with_contact_person_links_organization_manager(
    async_client,
    test_db_session,
):
    await _seed_employee(test_db_session, employee_id=29)

    contact_partner_id = 7111
    await seed_partner(
        test_db_session,
        partner_id=contact_partner_id,
        role="organization_manager",
    )

    payload = {
        "name": "ContactPersonOrg",
        "contact_person_user_ids": {"organization_managers": [contact_partner_id]},
    }

    response = await async_client.post("/organizations", headers=employee_auth_header(29), json=payload)
    assert response.status_code == 201

    organization_id = response.json()["data"]["organization_id"]
    created = await test_db_session.get(Organization, organization_id)
    assert created is not None
    assert created.contact_person_user_ids == {"organization_managers": [contact_partner_id]}

    partner_row = await test_db_session.get(Partner, contact_partner_id)
    assert partner_row is not None
    assert partner_row.role == "organization_manager"
    assert (partner_row.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_create_organization_rejects_employee_as_contact_person(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=290)
    await seed_employee(
        test_db_session,
        employee_id=291,
        role="admin",
        phone="7291000001",
    )

    response = await async_client.post(
        "/organizations",
        headers=employee_auth_header(290),
        json={
            "name": "BadContactOrg",
            "contact_person_user_ids": {"organization_managers": [291]},
        },
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_organization_manager_partner_can_get_own_organization(async_client, test_db_session):
    manager_partner_id = 120
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
    )
    test_db_session.add(
        Organization(
            organization_id=9501,
            name="ManagedOrg",
            status="active",
            contact_person_user_ids={"organization_managers": [manager_partner_id]},
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/organizations/9501",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 200
    assert response.json()["data"]["contact_person_user_ids"] == {
        "organization_managers": [manager_partner_id]
    }


@pytest.mark.asyncio
async def test_organization_manager_partner_cannot_get_other_organization(async_client, test_db_session):
    manager_partner_id = 121
    other_contact_partner_id = 123
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
    )
    await seed_partner(
        test_db_session,
        partner_id=other_contact_partner_id,
        role="organization_manager",
    )
    test_db_session.add(
        Organization(
            organization_id=9502,
            name="OtherOrg",
            status="active",
            contact_person_user_ids={"organization_managers": [other_contact_partner_id]},
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/organizations/9502",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_organization_manager_partner_cannot_list_organizations(async_client, test_db_session):
    manager_partner_id = 122
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
    )

    response = await async_client.get(
        "/organizations",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_organization_manager_partner_cannot_change_contact_persons(
    async_client, test_db_session
):
    """Org managers may update own org fields but contact JSON stays admin-only."""
    manager_partner_id = 125
    other_partner_id = 126
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
        phone="7125000001",
    )
    await seed_partner(
        test_db_session,
        partner_id=other_partner_id,
        role="organization_manager",
        phone="7126000001",
    )
    test_db_session.add(
        Organization(
            organization_id=9510,
            name="ContactLockedOrg",
            status="active",
            contact_person_user_ids={"organization_managers": [manager_partner_id]},
            city="BLR",
            country="IN",
        )
    )
    await test_db_session.commit()

    response = await async_client.put(
        "/organizations/9510",
        headers=partner_auth_header(manager_partner_id),
        json={
            "name": "ContactLockedOrg Renamed",
            "organization_type": "corporate",
            "logo": None,
            "website_url": None,
            "address": None,
            "pin_code": None,
            "city": "BLR",
            "state": None,
            "country": "IN",
            "contact_person_user_ids": {"organization_managers": [other_partner_id]},
            "bd_employee_id": None,
        },
    )
    assert response.status_code == 200, response.text

    refreshed = await async_client.get(
        "/organizations/9510",
        headers=partner_auth_header(manager_partner_id),
    )
    assert refreshed.status_code == 200
    body = refreshed.json()["data"]
    assert body["name"] == "ContactLockedOrg Renamed"
    assert body["contact_person_user_ids"] == {"organization_managers": [manager_partner_id]}
