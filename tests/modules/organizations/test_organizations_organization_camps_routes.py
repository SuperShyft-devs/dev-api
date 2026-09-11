"""Integration tests for GET /organizations/{organization_id}/camps."""

from __future__ import annotations

from datetime import date

import pytest

from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.reports.models import CampReport
from tests.helpers.auth import employee_auth_header, partner_auth_header, seed_employee, seed_partner
from tests.helpers.engagement_types import engagement_type_id
from tests.helpers.org_contact import org_contact_person_ids


async def _seed_admin(test_db_session, *, employee_id: int = 41):
    await seed_employee(test_db_session, employee_id=employee_id, role="admin")


async def _seed_org_with_camps(
    test_db_session,
    *,
    organization_id: int,
    other_organization_id: int,
    contact_person_partner_id: int | None = None,
):
    start_a = date(2026, 6, 23)
    start_b = date(2026, 7, 1)
    camp_no_a = compute_camp_no(organization_id, start_a)
    camp_no_b = compute_camp_no(organization_id, start_b)
    other_camp_no = compute_camp_no(other_organization_id, start_a)
    bio_ai_type_id = await engagement_type_id(test_db_session, "bio_ai")

    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name="Target Org",
            organization_type="corporate",
            status="active",
            contact_person_user_ids=org_contact_person_ids(contact_person_partner_id),
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
                engagement_type=bio_ai_type_id,
                assessment_package_id=None,
                diagnostic_package_id=None,
                city="BLR",
                slot_duration=20,
                start_date=start_a if camp_no != camp_no_b else start_b,
                end_date=start_a if camp_no != camp_no_b else start_b,
                status="running",
            )
        )
    # Default list endpoints only return camps with initialized reports.
    for camp_no, org_id in (
        (camp_no_a, organization_id),
        (camp_no_b, organization_id),
        (other_camp_no, other_organization_id),
    ):
        test_db_session.add(
            CampReport(
                report={},
                camp_no=camp_no,
                department=None,
                city=None,
                organization_id=org_id,
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
        headers=employee_auth_header(41),
    )
    assert response.status_code == 200

    camps = response.json()["data"]
    camp_nos = {row["camp_no"] for row in camps}
    assert camp_no_a in camp_nos
    assert camp_no_b in camp_nos
    assert other_camp_no not in camp_nos
    assert all(row["organization_id"] == 8101 for row in camps)
    assert all(row["organization_name"] == "Target Org" for row in camps)
    assert all(row["organization_logo"] is None for row in camps)
    assert all("year" in row and "engagement_ids" in row and "departments" in row and "cities" in row for row in camps)
    assert all("report_access" not in row for row in camps)


@pytest.mark.asyncio
async def test_list_organization_camps_org_manager_own_org(async_client, test_db_session):
    manager_partner_id = 50
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
    )

    camp_no_a, camp_no_b, other_camp_no = await _seed_org_with_camps(
        test_db_session,
        organization_id=8110,
        other_organization_id=8111,
        contact_person_partner_id=manager_partner_id,
    )

    response = await async_client.get(
        "/organizations/8110/camps?page=1&limit=10",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 200

    camps = response.json()["data"]
    camp_nos = {row["camp_no"] for row in camps}
    assert camp_no_a in camp_nos
    assert camp_no_b in camp_nos
    assert other_camp_no not in camp_nos


@pytest.mark.asyncio
async def test_list_organization_camps_org_manager_other_org_403(async_client, test_db_session):
    manager_partner_id = 51
    other_contact_partner_id = 52
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

    await _seed_org_with_camps(
        test_db_session,
        organization_id=8120,
        other_organization_id=8121,
        contact_person_partner_id=other_contact_partner_id,
    )

    response = await async_client.get(
        "/organizations/8120/camps",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_organization_camps_onboarding_assistant_403(async_client, test_db_session):
    assistant_employee_id = 52
    await seed_employee(
        test_db_session,
        employee_id=assistant_employee_id,
        role="onboarding_assistant",
    )

    await _seed_org_with_camps(
        test_db_session,
        organization_id=8130,
        other_organization_id=8131,
    )

    response = await async_client.get(
        "/organizations/8130/camps",
        headers=employee_auth_header(assistant_employee_id),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_organization_camps_nonexistent_org_404(async_client, test_db_session):
    manager_partner_id = 55
    await seed_partner(
        test_db_session,
        partner_id=manager_partner_id,
        role="organization_manager",
    )

    response = await async_client.get(
        "/organizations/999999/camps",
        headers=partner_auth_header(manager_partner_id),
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_organization_camps_excludes_uninitialized_by_default(
    async_client, test_db_session
):
    await _seed_admin(test_db_session, employee_id=54)

    organization_id = 8140
    start_a = date(2026, 6, 23)
    start_b = date(2026, 7, 1)
    camp_no_a = compute_camp_no(organization_id, start_a)
    camp_no_b = compute_camp_no(organization_id, start_b)
    bio_ai_type_id = await engagement_type_id(test_db_session, "bio_ai")

    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name="Filter Org",
            organization_type="corporate",
            status="active",
            departments=[{"department": "Sales", "slug": "sales"}],
        )
    )
    await test_db_session.commit()

    for engagement_id, camp_no, start in (
        (8501, camp_no_a, start_a),
        (8502, camp_no_b, start_b),
    ):
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"Engagement {engagement_id}",
                organization_id=organization_id,
                camp_no=camp_no,
                engagement_code=f"F{engagement_id}",
                engagement_type=bio_ai_type_id,
                assessment_package_id=None,
                diagnostic_package_id=None,
                city="BLR",
                slot_duration=20,
                start_date=start,
                end_date=start,
                status="running",
            )
        )
    # Only camp_no_a is initialized
    test_db_session.add(
        CampReport(
            report={},
            camp_no=camp_no_a,
            department=None,
            city=None,
            organization_id=organization_id,
        )
    )
    await test_db_session.commit()

    default_response = await async_client.get(
        f"/organizations/{organization_id}/camps?page=1&limit=10",
        headers=employee_auth_header(54),
    )
    assert default_response.status_code == 200
    default_camp_nos = {row["camp_no"] for row in default_response.json()["data"]}
    assert default_camp_nos == {camp_no_a}

    all_response = await async_client.get(
        f"/organizations/{organization_id}/camps?page=1&limit=10&initialized_only=false",
        headers=employee_auth_header(54),
    )
    assert all_response.status_code == 200
    all_camp_nos = {row["camp_no"] for row in all_response.json()["data"]}
    assert all_camp_nos == {camp_no_a, camp_no_b}
