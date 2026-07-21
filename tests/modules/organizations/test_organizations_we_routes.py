"""Integration tests for GET /organizations/we."""

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


async def _seed_admin(test_db_session, *, user_id: int = 7401, employee_id: int = 61):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_orgs(test_db_session):
    # contact_person_user_id is an FK to users
    for user_id in (7410, 7411):
        test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Organization(
            organization_id=8501,
            name="Managed Org A",
            organization_type="corporate",
            status="active",
            contact_person_user_id=7410,
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
            contact_person_user_id=7411,
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

    response = await async_client.get("/organizations/we?page=1&limit=20", headers=_auth_header(7401))
    assert response.status_code == 200

    body = response.json()
    orgs = body["data"]
    assert body["meta"]["total"] >= 2
    ids = {row["organization_id"] for row in orgs}
    assert 8501 in ids
    assert 8502 in ids

    managed = next(row for row in orgs if row["organization_id"] == 8501)
    assert managed["name"] == "Managed Org A"
    assert managed["contact_person_user_id"] == 7410
    assert managed["address"] == "Addr A"
    assert managed["departments"] == [{"department": "Sales", "slug": "sales"}]
    assert "created_at" in managed
    assert "updated_at" in managed


@pytest.mark.asyncio
async def test_list_my_organizations_org_manager_own_only(async_client, test_db_session):
    manager_user_id = 7410
    await _seed_orgs(test_db_session)
    test_db_session.add(
        Employee(employee_id=62, user_id=manager_user_id, role="organization_manager", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/we?page=1&limit=20", headers=_auth_header(manager_user_id))
    assert response.status_code == 200

    body = response.json()
    orgs = body["data"]
    assert body["meta"]["total"] == 1
    assert len(orgs) == 1
    assert orgs[0]["organization_id"] == 8501
    assert orgs[0]["name"] == "Managed Org A"
    assert orgs[0]["contact_person_user_id"] == manager_user_id


@pytest.mark.asyncio
async def test_list_my_organizations_org_manager_not_contact_empty(async_client, test_db_session):
    manager_user_id = 7412
    await _seed_orgs(test_db_session)
    test_db_session.add(User(user_id=manager_user_id, age=30, phone="7412000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=63, user_id=manager_user_id, role="organization_manager", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/we", headers=_auth_header(manager_user_id))
    assert response.status_code == 200
    assert response.json()["data"] == []
    assert response.json()["meta"]["total"] == 0


@pytest.mark.asyncio
async def test_list_my_organizations_onboarding_assistant_403(async_client, test_db_session):
    assistant_user_id = 7413
    test_db_session.add(User(user_id=assistant_user_id, age=30, phone="7413000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=64, user_id=assistant_user_id, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/we", headers=_auth_header(assistant_user_id))
    assert response.status_code == 403
