"""Integration tests for GET /organizations/camps."""

from __future__ import annotations

from datetime import date

import pytest

from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from tests.helpers.auth import employee_auth_header, seed_employee
from tests.helpers.engagement_types import engagement_type_id


async def _seed_employee(test_db_session, *, employee_id: int, role: str = "admin"):
    await seed_employee(test_db_session, employee_id=employee_id, role=role)


@pytest.mark.asyncio
async def test_list_camps_requires_auth(async_client):
    response = await async_client.get("/organizations/camps")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_camps_onboarding_assistant_403(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=32, role="onboarding_assistant")

    response = await async_client.get(
        "/organizations/camps",
        headers=employee_auth_header(32),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_camps_aggregates_engagements(async_client, test_db_session):
    await _seed_employee(test_db_session, employee_id=31)

    test_db_session.add(
        Organization(
            organization_id=8001,
            name="Camp Org",
            organization_type="corporate",
            status="active",
            departments=[
                {"department": "Sales", "slug": "sales"},
                {"department": "HR", "slug": "hr"},
            ],
        )
    )
    await test_db_session.commit()
    start = date(2026, 6, 23)
    camp_no = compute_camp_no(8001, start)
    assert camp_no == 8001230626
    bio_ai_type_id = await engagement_type_id(test_db_session, "bio_ai")

    for engagement_id, code in ((8201, "CAMP1"), (8202, "CAMP2")):
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"Engagement {engagement_id}",
                organization_id=8001,
                camp_no=camp_no,
                engagement_code=code,
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

    test_db_session.add(
        Engagement(
            engagement_id=8203,
            engagement_name="B2C Engagement",
            organization_id=None,
            camp_no=None,
            engagement_code="B2CCAMP",
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
    await test_db_session.commit()

    # Default: only camps with initialized camp reports
    default_response = await async_client.get(
        "/organizations/camps?page=1&limit=10",
        headers=employee_auth_header(31),
    )
    assert default_response.status_code == 200
    assert all(row["camp_no"] != camp_no for row in default_response.json()["data"])

    # Admin can still list uninitialized camps
    response = await async_client.get(
        "/organizations/camps?page=1&limit=10&initialized_only=false",
        headers=employee_auth_header(31),
    )
    assert response.status_code == 200

    body = response.json()
    camps = body["data"]
    matching = [row for row in camps if row["camp_no"] == camp_no]
    assert len(matching) == 1
    assert matching[0]["camp_name"] == "Camp Org 23 June 2026"
    assert matching[0]["year"] == 2026
    assert matching[0]["organization_id"] == 8001
    assert matching[0]["organization_name"] == "Camp Org"
    assert matching[0]["organization_logo"] is None
    assert matching[0]["engagement_ids"] == [8201, 8202]
    assert matching[0]["departments"] == {"count": 0, "departments": []}
    assert "engagement_count" not in matching[0]
    assert "department_count" not in matching[0]
    assert "report_count" not in matching[0]


@pytest.mark.asyncio
async def test_list_camps_initialized_only_returns_camps_with_reports(async_client, test_db_session):
    from modules.reports.models import CampReport

    await _seed_employee(test_db_session, employee_id=33)

    test_db_session.add(
        Organization(
            organization_id=8010,
            name="Init Camp Org",
            organization_type="corporate",
            status="active",
            departments=[{"department": "Sales", "slug": "sales"}],
        )
    )
    await test_db_session.commit()
    start = date(2026, 6, 23)
    camp_no = compute_camp_no(8010, start)
    other_camp_no = compute_camp_no(8010, date(2026, 7, 1))
    bio_ai_type_id = await engagement_type_id(test_db_session, "bio_ai")

    test_db_session.add(
        Engagement(
            engagement_id=8210,
            engagement_name="Initialized Camp",
            organization_id=8010,
            camp_no=camp_no,
            engagement_code="INIT1",
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
    test_db_session.add(
        Engagement(
            engagement_id=8211,
            engagement_name="Uninitialized Camp",
            organization_id=8010,
            camp_no=other_camp_no,
            engagement_code="INIT2",
            engagement_type=bio_ai_type_id,
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 1),
            status="running",
        )
    )
    test_db_session.add(
        CampReport(
            report={},
            camp_no=camp_no,
            department=None,
            city=None,
            organization_id=8010,
        )
    )
    test_db_session.add(
        CampReport(
            report={},
            camp_no=camp_no,
            department="sales",
            city=None,
            organization_id=8010,
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/organizations/camps?page=1&limit=10",
        headers=employee_auth_header(33),
    )
    assert response.status_code == 200
    camp_nos = {row["camp_no"] for row in response.json()["data"]}
    assert camp_no in camp_nos
    assert other_camp_no not in camp_nos
    matching = [row for row in response.json()["data"] if row["camp_no"] == camp_no]
    assert matching[0]["year"] == 2026
    assert matching[0]["engagement_ids"] == [8210]
    assert matching[0]["departments"] == {
        "count": 1,
        "departments": [{"name": "Sales", "slug": "sales"}],
    }
