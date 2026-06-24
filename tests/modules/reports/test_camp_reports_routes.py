"""Integration tests for camp reports routes."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement, EngagementParticipant
from modules.organizations.models import Organization
from modules.reports.models import CampReport, CampReportSection
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_camp(test_db_session, *, organization_id: int = 9101, engagement_id: int = 9101):
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name="Camp Reports Org",
            organization_type="corporate",
            status="active",
            departments=[
                {"department": "Sales", "slug": "sales"},
                {"department": "Engineering", "slug": "engineering"},
            ],
        )
    )
    await test_db_session.commit()

    start = date(2026, 6, 23)
    end = date(2026, 6, 25)
    camp_no = compute_camp_no(organization_id, start)
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp Engagement",
            organization_id=organization_id,
            camp_no=camp_no,
            engagement_code="CAMPREP1",
            engagement_type="bio_ai",
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="BLR",
            slot_duration=20,
            start_date=start,
            end_date=end,
            status="running",
            participant_count=0,
        )
    )
    await test_db_session.commit()
    return camp_no, organization_id


async def _seed_participation_section(test_db_session, *, report_sections: int = 1):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == "participation_by_age")
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Participation by Age",
        section_key="participation_by_age",
        description="Enrollment distribution across age groups",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_refresh_camp_with_participants(
    test_db_session,
    *,
    organization_id: int = 9201,
    engagement_id: int = 9201,
):
    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)

    test_db_session.add_all(
        [
            User(
                user_id=92001,
                age=22,
                phone="920010000000",
                status="active",
            ),
            User(
                user_id=92002,
                age=40,
                phone="920020000000",
                status="active",
                date_of_birth=date(1986, 1, 15),
            ),
            User(
                user_id=92003,
                age=60,
                phone="920030000000",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id + 1,
            engagement_name="Camp Engagement 2",
            organization_id=organization_id,
            camp_no=camp_no,
            engagement_code="CAMPREP2",
            engagement_type="bio_ai",
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="BLR",
            slot_duration=20,
            start_date=start,
            end_date=date(2026, 6, 25),
            status="running",
            participant_count=0,
        )
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=92001,
                engagement_id=engagement_id,
                user_id=92001,
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=92002,
                engagement_id=engagement_id,
                user_id=92002,
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=92003,
                engagement_id=engagement_id,
                user_id=92003,
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=92004,
                engagement_id=engagement_id + 1,
                user_id=92001,
                engagement_date=start,
                slot_start_time=time(12, 0),
                participant_department="sales",
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_init_camp_report_requires_auth(async_client):
    response = await async_client.post("/reports/camps/123/init")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_init_camp_report_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7401, employee_id=51)
    camp_no, _ = await _seed_camp(test_db_session)
    headers = _auth_header(7401)

    response = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert response.status_code == 201
    report_id = response.json()["data"]["report_id"]
    assert report_id is not None

    row = (
        await test_db_session.execute(
            select(CampReport).where(CampReport.report_id == report_id)
        )
    ).scalar_one()
    assert row.camp_no == camp_no
    assert row.department is None
    assert row.organization_id == 9101
    assert row.report["meta"]["camp_name"] == "Camp Reports Org 23 June 2026"
    assert row.report["meta"]["summary_available"] is False
    assert row.report["meta"]["refreshed_at"] is None
    assert row.report["meta"]["next_refresh"] is None
    assert row.report["meta"]["camp_start_date"] == "2026-06-23"
    assert row.report["meta"]["camp_end_date"] == "2026-06-25"


@pytest.mark.asyncio
async def test_init_department_camp_report(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7402, employee_id=52)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9102, engagement_id=9102)
    headers = _auth_header(7402)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert response.status_code == 201

    row = (
        await test_db_session.execute(
            select(CampReport).where(CampReport.department == "sales")
        )
    ).scalar_one()
    assert row.camp_no == camp_no
    assert row.report["meta"]["camp_name"] == "Camp Reports Org sales 23 June 2026"


@pytest.mark.asyncio
async def test_init_camp_report_conflict(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7403, employee_id=53)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9103, engagement_id=9103)
    headers = _auth_header(7403)

    first = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert first.status_code == 201

    second = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert second.status_code == 409
    assert second.json()["error_code"] == "CAMP_REPORT_EXISTS"


@pytest.mark.asyncio
async def test_init_camp_report_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7404, employee_id=54)
    headers = _auth_header(7404)

    response = await async_client.post("/reports/camps/999999999/init", headers=headers)
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_NOT_FOUND"


@pytest.mark.asyncio
async def test_init_department_invalid_slug(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7405, employee_id=55)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9105, engagement_id=9105)
    headers = _auth_header(7405)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/department/unknown/init",
        headers=headers,
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "DEPARTMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_delete_camp_reports(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7406, employee_id=56)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9106, engagement_id=9106)
    headers = _auth_header(7406)

    init_overall = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init_overall.status_code == 201
    init_dept = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init_dept.status_code == 201

    delete_overall = await async_client.delete(f"/reports/camps/{camp_no}", headers=headers)
    assert delete_overall.status_code == 200

    delete_dept = await async_client.delete(
        f"/reports/camps/{camp_no}/department/sales",
        headers=headers,
    )
    assert delete_dept.status_code == 200

    remaining = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalars().all()
    assert remaining == []


@pytest.mark.asyncio
async def test_list_camp_reports_requires_auth(async_client):
    response = await async_client.get("/reports/camps/123")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_camp_reports(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7501, employee_id=61)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9111, engagement_id=9111)
    headers = _auth_header(7501)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    init_dept = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init_dept.status_code == 201

    response = await async_client.get(f"/reports/camps/{camp_no}", headers=headers)
    assert response.status_code == 200
    rows = response.json()["data"]
    assert len(rows) == 2
    assert rows[0]["department"] is None
    assert rows[1]["department"] == "sales"


@pytest.mark.asyncio
async def test_refresh_camp_report_requires_auth(async_client):
    response = await async_client.put(
        "/reports/camps/123/refresh",
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_refresh_camp_report_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7502, employee_id=62)
    await _seed_participation_section(test_db_session, report_sections=10)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9202,
        engagement_id=9202,
    )
    headers = _auth_header(7502)

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_REPORT_NOT_FOUND"


@pytest.mark.asyncio
async def test_refresh_camp_report_invalid_section(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7503, employee_id=63)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9203,
        engagement_id=9203,
    )
    headers = _auth_header(7503)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "unknown_section"},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_SECTION"


@pytest.mark.asyncio
async def test_refresh_camp_report_participation_by_age(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7504, employee_id=64)
    await _seed_participation_section(test_db_session, report_sections=11)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9204,
        engagement_id=9204,
    )
    headers = _auth_header(7504)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["report_id"] == report_id
    section = payload["section"]
    assert section["name"] == "Participation by Age"
    assert section["description"] == "Enrollment distribution across age groups"
    assert section["total_enrolled"] == 3
    assert section["data"]["age_group"] == ["18–25", "26–35", "36–45", "46–55", "55+"]
    assert section["data"]["enrolled"] == [1, 0, 1, 0, 1]
    assert section["data"]["percent"] == [33.3, 0.0, 33.3, 0.0, 33.3]

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["meta"]["summary_available"] is True
    assert row.report["meta"]["refreshed_at"] is not None
    assert row.report["participation_by_age"]["total_enrolled"] == 3


@pytest.mark.asyncio
async def test_refresh_department_camp_report_participation_by_age(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7505, employee_id=65)
    await _seed_participation_section(test_db_session, report_sections=12)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9205,
        engagement_id=9205,
    )
    headers = _auth_header(7505)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["total_enrolled"] == 2
    assert section["data"]["enrolled"] == [1, 0, 1, 0, 0]


@pytest.mark.asyncio
async def test_refresh_camp_report_replaces_existing_section(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7506, employee_id=66)
    await _seed_participation_section(test_db_session, report_sections=13)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9206,
        engagement_id=9206,
    )
    headers = _auth_header(7506)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    first = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert first.status_code == 200

    second = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert second.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["meta"]["camp_name"] == "Camp Reports Org 23 June 2026"
    assert row.report["participation_by_age"]["total_enrolled"] == 3
