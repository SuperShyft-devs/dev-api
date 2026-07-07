"""Integration tests for GET /organizations/{organization_id}/camps."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_admin(test_db_session, *, user_id: int = 7301, employee_id: int = 41):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_org_with_camps(
    test_db_session,
    *,
    organization_id: int,
    other_organization_id: int,
    contact_person_user_id: int | None = None,
):
    start_a = date(2026, 6, 23)
    start_b = date(2026, 7, 1)
    camp_no_a = compute_camp_no(organization_id, start_a)
    camp_no_b = compute_camp_no(organization_id, start_b)
    other_camp_no = compute_camp_no(other_organization_id, start_a)

    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name="Target Org",
            organization_type="corporate",
            status="active",
            contact_person_user_id=contact_person_user_id,
            departments=[{"department": "Sales", "slug": "sales"}],
        )
    )
    test_db_session.add(
        Organization(
            organization_id=other_organization_id,
            name="Other Org",
            organization_type="corporate",
            status="active",
            departments=[{"department": "HR", "slug": "hr"}],
        )
    )
    await test_db_session.commit()

    for engagement_id, org_id, camp_no, code in (
        (8401, organization_id, camp_no_a, "TGT1"),
        (8402, organization_id, camp_no_b, "TGT2"),
        (8403, other_organization_id, other_camp_no, "OTH1"),
    ):
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"Engagement {engagement_id}",
                organization_id=org_id,
                camp_no=camp_no,
                engagement_code=code,
                engagement_type="bio_ai",
                assessment_package_id=None,
                diagnostic_package_id=None,
                city="BLR",
                slot_duration=20,
                start_date=start_a if camp_no != camp_no_b else start_b,
                end_date=start_a if camp_no != camp_no_b else start_b,
                status="running",
            )
        )
    await test_db_session.commit()

    return camp_no_a, camp_no_b, other_camp_no


@pytest.mark.asyncio
async def test_list_organization_camps_requires_auth(async_client):
    response = await async_client.get("/organizations/8101/camps")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_organization_camps_admin_sees_only_target_org(async_client, test_db_session):
    await _seed_admin(test_db_session)
    camp_no_a, camp_no_b, other_camp_no = await _seed_org_with_camps(
        test_db_session,
        organization_id=8101,
        other_organization_id=8102,
    )

    response = await async_client.get(
        "/organizations/8101/camps?page=1&limit=10",
        headers=_auth_header(7301),
    )
    assert response.status_code == 200

    camps = response.json()["data"]
    camp_nos = {row["camp_no"] for row in camps}
    assert camp_no_a in camp_nos
    assert camp_no_b in camp_nos
    assert other_camp_no not in camp_nos
    assert all(row["organization_id"] == 8101 for row in camps)


@pytest.mark.asyncio
async def test_list_organization_camps_org_manager_own_org(async_client, test_db_session):
    manager_user_id = 7310
    test_db_session.add(User(user_id=manager_user_id, age=30, phone="7310000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=50, user_id=manager_user_id, role="organization_manager", status="active")
    )
    await test_db_session.commit()

    camp_no_a, camp_no_b, other_camp_no = await _seed_org_with_camps(
        test_db_session,
        organization_id=8110,
        other_organization_id=8111,
        contact_person_user_id=manager_user_id,
    )

    response = await async_client.get(
        "/organizations/8110/camps?page=1&limit=10",
        headers=_auth_header(manager_user_id),
    )
    assert response.status_code == 200

    camps = response.json()["data"]
    camp_nos = {row["camp_no"] for row in camps}
    assert camp_no_a in camp_nos
    assert camp_no_b in camp_nos
    assert other_camp_no not in camp_nos


@pytest.mark.asyncio
async def test_list_organization_camps_org_manager_other_org_403(async_client, test_db_session):
    manager_user_id = 7311
    other_contact_user_id = 7312
    test_db_session.add_all(
        [
            User(user_id=manager_user_id, age=30, phone="7311000000000", status="active"),
            User(user_id=other_contact_user_id, age=30, phone="7312000000000", status="active"),
        ]
    )
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=51, user_id=manager_user_id, role="organization_manager", status="active")
    )
    await test_db_session.commit()

    await _seed_org_with_camps(
        test_db_session,
        organization_id=8120,
        other_organization_id=8121,
        contact_person_user_id=other_contact_user_id,
    )

    response = await async_client.get(
        "/organizations/8120/camps",
        headers=_auth_header(manager_user_id),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_organization_camps_onboarding_assistant_403(async_client, test_db_session):
    assistant_user_id = 7313
    test_db_session.add(User(user_id=assistant_user_id, age=30, phone="7313000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=52, user_id=assistant_user_id, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    await _seed_org_with_camps(
        test_db_session,
        organization_id=8130,
        other_organization_id=8131,
    )

    response = await async_client.get(
        "/organizations/8130/camps",
        headers=_auth_header(assistant_user_id),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_organization_camps_nonexistent_org_404(async_client, test_db_session):
    await _seed_admin(test_db_session, user_id=7314, employee_id=53)

    response = await async_client.get(
        "/organizations/999999/camps",
        headers=_auth_header(7314),
    )
    assert response.status_code == 404
