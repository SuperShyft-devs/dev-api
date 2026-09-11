"""Integration tests for GET /organizations/we."""

from __future__ import annotations

import pytest

from modules.organizations.models import Organization
from tests.helpers.auth import (
    employee_auth_header,
    make_partner,
    partner_auth_header,
    seed_employee,
    seed_partner,
)


async def _seed_admin(test_db_session, *, employee_id: int = 61):
    await seed_employee(test_db_session, employee_id=employee_id, role="admin")


async def _seed_orgs(test_db_session):
    for partner_id in (7410, 7411):
        test_db_session.add(
            make_partner(partner_id=partner_id, role="organization_manager", status="active")
        )
    await test_db_session.flush()

    test_db_session.add(
        Organization(
            organization_id=8501,
            name="Managed Org A",
            organization_type="corporate",
            status="active",
            contact_person_user_ids={"organization_managers": [7410]},
            departments=[{"department": "Sales", "slug": "sales"}],
            address="Addr A",
            city="BLR",
            country="IN",
        )
    )
    test_db_session.add(
        Organization(
            organization_id=8502,
            name="Other Org",
            organization_type="corporate",
            status="active",
            contact_person_user_ids={"organization_managers": [7411]},
            city="MUM",
            country="IN",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_my_organizations_requires_auth(async_client):
    response = await async_client.get("/organizations/we")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_my_organizations_admin_returns_all_with_details(async_client, test_db_session):
    await _seed_admin(test_db_session)
    await _seed_orgs(test_db_session)

    response = await async_client.get(
        "/organizations/we?page=1&limit=20",
        headers=employee_auth_header(61),
    )
    assert response.status_code == 200

    body = response.json()
    orgs = body["data"]
    assert body["meta"]["total"] >= 2
    ids = {row["organization_id"] for row in orgs}
    assert 8501 in ids
    assert 8502 in ids

    managed = next(row for row in orgs if row["organization_id"] == 8501)
    assert managed["name"] == "Managed Org A"
    assert managed["contact_person_user_ids"] == {"organization_managers": [7410]}
    assert managed["address"] == "Addr A"
    assert managed["departments"] == [{"department": "Sales", "slug": "sales"}]
    assert "camp_cities" in managed
    assert "report_access" in managed
    assert managed["report_access"]["organization_manager"] is True
    assert "created_at" in managed
    assert "updated_at" in managed


@pytest.mark.asyncio
async def test_list_my_organizations_org_manager_partner_own_only(async_client, test_db_session):
    manager_partner_id = 7410
    await _seed_orgs(test_db_session)

    response = await async_client.get(
        "/organizations/we?page=1&limit=20",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 200

    body = response.json()
    orgs = body["data"]
    assert body["meta"]["total"] == 1
    assert len(orgs) == 1
    assert orgs[0]["organization_id"] == 8501
    assert orgs[0]["name"] == "Managed Org A"
    assert orgs[0]["contact_person_user_ids"] == {"organization_managers": [manager_partner_id]}
    assert "report_access" in orgs[0]
    assert orgs[0]["report_access"]["organization_manager"] is True


@pytest.mark.asyncio
async def test_list_my_organizations_org_manager_partner_not_contact_empty(async_client, test_db_session):
    manager_partner_id = 7412
    await _seed_orgs(test_db_session)
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
    )

    response = await async_client.get(
        "/organizations/we",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 200
    assert response.json()["data"] == []
    assert response.json()["meta"]["total"] == 0


@pytest.mark.asyncio
async def test_list_my_organizations_phlebo_partner_403(async_client, test_db_session):
    await seed_partner(
        test_db_session,
        partner_id=64,
        role="phlebo",
    )

    response = await async_client.get(
        "/organizations/we",
        headers=partner_auth_header(64),
    )
    assert response.status_code == 403
