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
    assert section["data"]["total_enrolled"] == 3
    assert section["data"]["age_group"] == ["18–25", "26–35", "36–45", "46–55", "55+"]
    assert section["data"]["enrolled"] == [1, 0, 1, 0, 1]
    assert section["data"]["percent"] == [33.3, 0.0, 33.3, 0.0, 33.3]

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["meta"]["summary_available"] is True
    assert row.report["meta"]["refreshed_at"] is not None
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 3


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
    assert section["data"]["total_enrolled"] == 2
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
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 3


async def _seed_organization_manager_for_camp(
    test_db_session,
    *,
    manager_user_id: int = 7601,
    employee_id: int = 71,
    organization_id: int = 9301,
    engagement_id: int = 9301,
):
    test_db_session.add(
        User(user_id=manager_user_id, age=30, phone=f"{manager_user_id}000000000", status="active")
    )
    await test_db_session.flush()
    test_db_session.add(
        Employee(
            employee_id=employee_id,
            user_id=manager_user_id,
            role="organization_manager",
            status="active",
        )
    )
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name="Managed Camp Org",
            organization_type="corporate",
            status="active",
            contact_person_user_id=manager_user_id,
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
            engagement_name="Managed Camp Engagement",
            organization_id=organization_id,
            camp_no=camp_no,
            engagement_code="MGDCAMP1",
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


@pytest.mark.asyncio
async def test_get_camp_report_meta(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7602, employee_id=72)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9302, engagement_id=9302)
    headers = _auth_header(7602)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    response = await async_client.get(f"/reports/camps/{camp_no}/meta", headers=headers)
    assert response.status_code == 200
    meta = response.json()["data"]
    assert meta["camp_name"] == "Camp Reports Org 23 June 2026"
    assert meta["summary_available"] is False


@pytest.mark.asyncio
async def test_get_camp_report_dashboard_after_refresh(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7603, employee_id=73)
    await _seed_participation_section(test_db_session, report_sections=21)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9303,
        engagement_id=9303,
    )
    headers = _auth_header(7603)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    refresh = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert refresh.status_code == 200

    response = await async_client.get(
        f"/reports/camps/{camp_no}/dashboard",
        headers=headers,
        params={"section": "participation_by_age"},
    )
    assert response.status_code == 200
    section = response.json()["data"]
    assert section["data"]["total_enrolled"] == 3
    assert section["name"] == "Participation by Age"


@pytest.mark.asyncio
async def test_get_camp_report_dashboard_section_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7604, employee_id=74)
    await _seed_participation_section(test_db_session, report_sections=22)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9304, engagement_id=9304)
    headers = _auth_header(7604)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    response = await async_client.get(
        f"/reports/camps/{camp_no}/dashboard",
        headers=headers,
        params={"section": "participation_by_age"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "SECTION_NOT_FOUND"


@pytest.mark.asyncio
async def test_get_department_camp_report_meta(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7605, employee_id=75)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9305, engagement_id=9305)
    headers = _auth_header(7605)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.get(
        f"/reports/camps/{camp_no}/department/sales/meta",
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["data"]["camp_name"] == "Camp Reports Org sales 23 June 2026"


@pytest.mark.asyncio
async def test_organization_manager_can_access_own_camp_meta(async_client, test_db_session):
    camp_no, _ = await _seed_organization_manager_for_camp(test_db_session)
    headers = _auth_header(7601)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 403

    await _seed_employee(test_db_session, user_id=7606, employee_id=76)
    admin_headers = _auth_header(7606)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=admin_headers)
    assert init.status_code == 201

    response = await async_client.get(f"/reports/camps/{camp_no}/meta", headers=headers)
    assert response.status_code == 200
    assert "camp_name" in response.json()["data"]


@pytest.mark.asyncio
async def test_organization_manager_cannot_access_other_camp_meta(async_client, test_db_session):
    await _seed_organization_manager_for_camp(
        test_db_session,
        manager_user_id=7607,
        employee_id=77,
        organization_id=9306,
        engagement_id=9306,
    )
    await _seed_employee(test_db_session, user_id=7608, employee_id=78)
    other_camp_no, _ = await _seed_camp(test_db_session, organization_id=9307, engagement_id=9307)
    admin_headers = _auth_header(7608)
    init = await async_client.post(f"/reports/camps/{other_camp_no}/init", headers=admin_headers)
    assert init.status_code == 201

    response = await async_client.get(
        f"/reports/camps/{other_camp_no}/meta",
        headers=_auth_header(7607),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_camp_report_sections_returns_keys(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7610, employee_id=80)
    await _seed_participation_section(test_db_session, report_sections=31)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9310,
        engagement_id=9310,
    )
    headers = _auth_header(7610)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    refresh = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert refresh.status_code == 200

    response = await async_client.get(f"/reports/camps/{camp_no}/sections", headers=headers)
    assert response.status_code == 200
    keys = response.json()["data"]
    assert keys == ["meta", "participation_by_age"]


@pytest.mark.asyncio
async def test_list_department_camp_report_sections(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7611, employee_id=81)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9311, engagement_id=9311)
    headers = _auth_header(7611)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.get(
        f"/reports/camps/{camp_no}/department/sales/sections",
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["data"] == ["meta"]


@pytest.mark.asyncio
async def test_list_camp_report_sections_onboarding_assistant_403(async_client, test_db_session):
    test_db_session.add(User(user_id=7612, age=30, phone="7612000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=82, user_id=7612, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    await _seed_employee(test_db_session, user_id=7613, employee_id=83)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9312, engagement_id=9312)
    admin_headers = _auth_header(7613)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=admin_headers)
    assert init.status_code == 201

    response = await async_client.get(
        f"/reports/camps/{camp_no}/sections",
        headers=_auth_header(7612),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_organization_manager_can_list_own_camp_sections(async_client, test_db_session):
    camp_no, _ = await _seed_organization_manager_for_camp(
        test_db_session,
        manager_user_id=7614,
        employee_id=84,
        organization_id=9313,
        engagement_id=9313,
    )
    await _seed_employee(test_db_session, user_id=7615, employee_id=85)
    admin_headers = _auth_header(7615)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=admin_headers)
    assert init.status_code == 201

    response = await async_client.get(
        f"/reports/camps/{camp_no}/sections",
        headers=_auth_header(7614),
    )
    assert response.status_code == 200
    assert response.json()["data"] == ["meta"]


async def _seed_kpis_section(test_db_session, *, report_sections: int = 100):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == "kpis")
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="KPIs",
        section_key="kpis",
        description="Camp enrollment and health KPI summary",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_kpis_camp_data(
    test_db_session,
    *,
    organization_id: int = 9401,
    engagement_id: int = 9401,
):
    from modules.assessments.models import AssessmentInstance, AssessmentPackage
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)

    test_db_session.add_all(
        [
            AssessmentPackage(
                package_id=9401,
                package_code="KPIPKG1",
                display_name="Bio AI Package",
                assessment_type_code="1",
                status="active",
            ),
            AssessmentPackage(
                package_id=9402,
                package_code="KPIPKG2",
                display_name="Bio AI Package 2",
                assessment_type_code="2",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            User(
                user_id=94001,
                age=30,
                gender="Male",
                phone="940010000000",
                status="active",
            ),
            User(
                user_id=94002,
                age=40,
                gender="Female",
                phone="940020000000",
                status="active",
            ),
            User(
                user_id=94003,
                age=50,
                gender="male",
                phone="940030000000",
                status="active",
            ),
            User(
                user_id=94004,
                age=50,
                gender="Female",
                phone="940040000000",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=94001,
                engagement_id=engagement_id,
                user_id=94001,
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
                want_doctor_consultation=True,
            ),
            EngagementParticipant(
                engagement_participant_id=94002,
                engagement_id=engagement_id,
                user_id=94002,
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
                want_doctor_consultation=False,
            ),
            EngagementParticipant(
                engagement_participant_id=94003,
                engagement_id=engagement_id,
                user_id=94003,
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=94004,
                engagement_id=engagement_id,
                user_id=94004,
                engagement_date=start,
                slot_start_time=time(11, 20),
                participant_department="engineering",
                want_doctor_and_nutritionist_consultation=True,
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            AssessmentInstance(
                assessment_instance_id=94001,
                user_id=94001,
                engagement_id=engagement_id,
                package_id=9401,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=94002,
                user_id=94002,
                engagement_id=engagement_id,
                package_id=9402,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=94003,
                user_id=94003,
                engagement_id=engagement_id,
                package_id=9401,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=94004,
                user_id=94004,
                engagement_id=engagement_id,
                package_id=9401,
                status="completed",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            IndividualHealthReport(
                report_id=94001,
                user_id=94001,
                assessment_instance_id=94001,
                engagement_id=engagement_id,
                reports={"metabolic_age": 34.0},
                blood_parameters={"value": 1},
            ),
            IndividualHealthReport(
                report_id=94002,
                user_id=94002,
                assessment_instance_id=94002,
                engagement_id=engagement_id,
                reports={"metabolic_age": 41.0},
                blood_parameters={"value": 1},
            ),
            IndividualHealthReport(
                report_id=94004,
                user_id=94004,
                assessment_instance_id=94004,
                engagement_id=engagement_id,
                reports={"data": {"metabolic_age": 53.0}},
                blood_parameters={"value": 1},
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_refresh_camp_report_kpis(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7701, employee_id=91)
    await _seed_kpis_section(test_db_session, report_sections=101)
    camp_no = await _seed_kpis_camp_data(test_db_session, organization_id=9401, engagement_id=9401)
    headers = _auth_header(7701)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "kpis"},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["report_id"] == report_id
    section = payload["section"]
    assert section["name"] == "KPIs"
    assert section["description"] == "Camp enrollment and health KPI summary"
    data = section["data"]
    assert data["employees_enrolled"] == 4
    assert data["male_enrolled"] == 2
    assert data["female_enrolled"] == 2
    assert data["total_blood_test"] == 3
    assert data["blood_test_percent"] == 75
    assert data["doctor_consultation"] == 2
    assert data["high_risk_group"] == 2

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["kpis"]["data"]["employees_enrolled"] == 4


@pytest.mark.asyncio
async def test_refresh_department_camp_report_kpis(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7702, employee_id=92)
    await _seed_kpis_section(test_db_session, report_sections=102)
    camp_no = await _seed_kpis_camp_data(test_db_session, organization_id=9402, engagement_id=9402)
    headers = _auth_header(7702)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "kpis"},
    )
    assert response.status_code == 200
    data = response.json()["data"]["section"]["data"]
    assert data["employees_enrolled"] == 2
    assert data["male_enrolled"] == 1
    assert data["female_enrolled"] == 1
    assert data["total_blood_test"] == 2
    assert data["blood_test_percent"] == 100
    assert data["doctor_consultation"] == 1
    assert data["high_risk_group"] == 1


@pytest.mark.asyncio
async def test_refresh_camp_report_kpis_replaces_without_touching_other_sections(
    async_client,
    test_db_session,
):
    await _seed_employee(test_db_session, user_id=7703, employee_id=93)
    await _seed_participation_section(test_db_session, report_sections=103)
    await _seed_kpis_section(test_db_session, report_sections=104)
    camp_no = await _seed_kpis_camp_data(test_db_session, organization_id=9403, engagement_id=9403)
    headers = _auth_header(7703)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    participation = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert participation.status_code == 200

    kpis = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "kpis"},
    )
    assert kpis.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "participation_by_age" in row.report
    assert "kpis" in row.report
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 4
    assert row.report["kpis"]["data"]["employees_enrolled"] == 4

    kpis_again = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "kpis"},
    )
    assert kpis_again.status_code == 200
    assert kpis_again.json()["data"]["section"]["data"]["employees_enrolled"] == 4
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 4


async def _seed_overall_risk_score_section(test_db_session, *, report_sections: int = 200):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == "overall_risk_score")
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.section = "Overall Risk Score"
        existing.description = "Metabolic score distribution across risk bands"
        await test_db_session.commit()
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Overall Risk Score",
        section_key="overall_risk_score",
        description="Metabolic score distribution across risk bands",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_overall_risk_score_camp_data(
    test_db_session,
    *,
    organization_id: int = 9501,
    engagement_id: int = 9501,
):
    from modules.assessments.models import AssessmentInstance, AssessmentPackage
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)

    test_db_session.add_all(
        [
            AssessmentPackage(
                package_id=9501,
                package_code="ORSPKG1",
                display_name="Bio AI Package",
                assessment_type_code="1",
                status="active",
            ),
            AssessmentPackage(
                package_id=9502,
                package_code="ORSPKG2",
                display_name="Bio AI Package 2",
                assessment_type_code="2",
                status="active",
            ),
            AssessmentPackage(
                package_id=9503,
                package_code="ORSPKG7",
                display_name="FitPrint Package",
                assessment_type_code="7",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            User(user_id=95001, age=30, phone="950010000000", status="active"),
            User(user_id=95002, age=35, phone="950020000000", status="active"),
            User(user_id=95003, age=40, phone="950030000000", status="active"),
            User(user_id=95004, age=45, phone="950040000000", status="active"),
            User(user_id=95005, age=50, phone="950050000000", status="active"),
            User(user_id=95006, age=28, phone="950060000000", status="active"),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=95001,
                engagement_id=engagement_id,
                user_id=95001,
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=95002,
                engagement_id=engagement_id,
                user_id=95002,
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=95003,
                engagement_id=engagement_id,
                user_id=95003,
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=95004,
                engagement_id=engagement_id,
                user_id=95004,
                engagement_date=start,
                slot_start_time=time(11, 20),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=95005,
                engagement_id=engagement_id,
                user_id=95005,
                engagement_date=start,
                slot_start_time=time(12, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=95006,
                engagement_id=engagement_id,
                user_id=95006,
                engagement_date=start,
                slot_start_time=time(12, 20),
                participant_department="sales",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            AssessmentInstance(
                assessment_instance_id=95001,
                user_id=95001,
                engagement_id=engagement_id,
                package_id=9501,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=95002,
                user_id=95002,
                engagement_id=engagement_id,
                package_id=9502,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=95003,
                user_id=95003,
                engagement_id=engagement_id,
                package_id=9501,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=95004,
                user_id=95004,
                engagement_id=engagement_id,
                package_id=9502,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=95005,
                user_id=95005,
                engagement_id=engagement_id,
                package_id=9501,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=95006,
                user_id=95006,
                engagement_id=engagement_id,
                package_id=9503,
                status="completed",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            IndividualHealthReport(
                report_id=95001,
                user_id=95001,
                assessment_instance_id=95001,
                engagement_id=engagement_id,
                reports={"metabolic_score": 20.0},
            ),
            IndividualHealthReport(
                report_id=95002,
                user_id=95002,
                assessment_instance_id=95002,
                engagement_id=engagement_id,
                reports={"data": {"metabolic_score": 35.0}},
            ),
            IndividualHealthReport(
                report_id=95003,
                user_id=95003,
                assessment_instance_id=95003,
                engagement_id=engagement_id,
                reports={"metabolic_score": 50.0},
            ),
            IndividualHealthReport(
                report_id=95004,
                user_id=95004,
                assessment_instance_id=95004,
                engagement_id=engagement_id,
                reports={"metabolic_score": 65.0},
            ),
            IndividualHealthReport(
                report_id=95005,
                user_id=95005,
                assessment_instance_id=95005,
                engagement_id=engagement_id,
                reports={"metabolic_age": 45.0},
            ),
            IndividualHealthReport(
                report_id=95006,
                user_id=95006,
                assessment_instance_id=95006,
                engagement_id=engagement_id,
                reports={"metabolic_score": 10.0},
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_refresh_camp_report_overall_risk_score(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7801, employee_id=201)
    await _seed_overall_risk_score_section(test_db_session, report_sections=201)
    camp_no = await _seed_overall_risk_score_camp_data(
        test_db_session, organization_id=9501, engagement_id=9501
    )
    headers = _auth_header(7801)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "overall_risk_score"},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["report_id"] == report_id
    section = payload["section"]
    assert section["name"] == "Overall Risk Score"
    assert section["description"] == "Metabolic score distribution across risk bands"
    data = section["data"]
    assert data["group"] == ["optimal", "low_risk", "increased_risk", "high_risk"]
    assert data["count"] == [1, 1, 1, 1]
    assert data["percent"] == [25.0, 25.0, 25.0, 25.0]
    assert data["total_employees"] == 4
    assert data["avg_metabolic_score"] == 42.5

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["overall_risk_score"]["data"]["total_employees"] == 4


@pytest.mark.asyncio
async def test_refresh_department_camp_report_overall_risk_score(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7802, employee_id=202)
    await _seed_overall_risk_score_section(test_db_session, report_sections=202)
    camp_no = await _seed_overall_risk_score_camp_data(
        test_db_session, organization_id=9502, engagement_id=9502
    )
    headers = _auth_header(7802)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "overall_risk_score"},
    )
    assert response.status_code == 200
    data = response.json()["data"]["section"]["data"]
    assert data["count"] == [1, 1, 0, 0]
    assert data["total_employees"] == 2
    assert data["avg_metabolic_score"] == 27.5


@pytest.mark.asyncio
async def test_refresh_overall_risk_score_updates_existing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7803, employee_id=203)
    await _seed_participation_section(test_db_session, report_sections=203)
    await _seed_overall_risk_score_section(test_db_session, report_sections=204)
    camp_no = await _seed_overall_risk_score_camp_data(
        test_db_session, organization_id=9503, engagement_id=9503
    )
    headers = _auth_header(7803)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    participation = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert participation.status_code == 200

    overall = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "overall_risk_score"},
    )
    assert overall.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "participation_by_age" in row.report
    assert "overall_risk_score" in row.report
    assert row.report["overall_risk_score"]["data"]["total_employees"] == 4

    overall_again = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "overall_risk_score"},
    )
    assert overall_again.status_code == 200
    assert overall_again.json()["data"]["section"]["data"]["total_employees"] == 4
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 6


async def _seed_camp_participants_with_profile_fields(
    test_db_session,
    *,
    organization_id: int = 9601,
    engagement_id: int = 9601,
):
    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)

    test_db_session.add(
        User(
            user_id=96001,
            first_name="Jane",
            last_name="Doe",
            gender="female",
            age=30,
            phone="960010000000",
            status="active",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=96001,
            engagement_id=engagement_id,
            user_id=96001,
            engagement_date=start,
            slot_start_time=time(10, 0),
            participant_department="engineering",
            participant_blood_group="O+",
        )
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_list_camp_participants_returns_all_enrollments(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7901, employee_id=210)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9602,
        engagement_id=9602,
    )
    headers = _auth_header(7901)

    response = await async_client.get(f"/reports/camps/{camp_no}/participants", headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 4
    assert len(body["data"]) == 4

    user_ids = [row["user_id"] for row in body["data"]]
    assert user_ids.count(92001) == 2


@pytest.mark.asyncio
async def test_list_camp_participants_pagination(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7902, employee_id=211)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9603,
        engagement_id=9603,
    )
    headers = _auth_header(7902)

    page1 = await async_client.get(
        f"/reports/camps/{camp_no}/participants?page=1&limit=2",
        headers=headers,
    )
    assert page1.status_code == 200
    assert page1.json()["meta"] == {"page": 1, "limit": 2, "total": 4}
    assert len(page1.json()["data"]) == 2

    page2 = await async_client.get(
        f"/reports/camps/{camp_no}/participants?page=2&limit=2",
        headers=headers,
    )
    assert page2.status_code == 200
    assert len(page2.json()["data"]) == 2


@pytest.mark.asyncio
async def test_list_camp_participants_includes_profile_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7903, employee_id=212)
    camp_no = await _seed_camp_participants_with_profile_fields(test_db_session)
    headers = _auth_header(7903)

    response = await async_client.get(f"/reports/camps/{camp_no}/participants", headers=headers)
    assert response.status_code == 200
    row = response.json()["data"][0]
    assert row["first_name"] == "Jane"
    assert row["last_name"] == "Doe"
    assert row["phone"] == "960010000000"
    assert row["gender"] == "female"
    assert row["participant_blood_group"] == "O+"
    assert row["participant_department"] == "engineering"


@pytest.mark.asyncio
async def test_list_camp_participants_organization_manager_own_camp(async_client, test_db_session):
    camp_no, _ = await _seed_organization_manager_for_camp(test_db_session)
    start = date(2026, 6, 23)
    test_db_session.add(
        User(
            user_id=96010,
            first_name="Org",
            last_name="Participant",
            gender="male",
            age=28,
            phone="960100000000",
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=96010,
            engagement_id=9301,
            user_id=96010,
            engagement_date=start,
            slot_start_time=time(9, 0),
            participant_department="sales",
            participant_blood_group="A+",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        f"/reports/camps/{camp_no}/participants",
        headers=_auth_header(7601),
    )
    assert response.status_code == 200
    assert response.json()["meta"]["total"] == 1
    assert response.json()["data"][0]["first_name"] == "Org"


@pytest.mark.asyncio
async def test_list_camp_participants_organization_manager_other_camp_403(async_client, test_db_session):
    await _seed_organization_manager_for_camp(
        test_db_session,
        manager_user_id=7904,
        employee_id=213,
        organization_id=9604,
        engagement_id=9604,
    )
    await _seed_employee(test_db_session, user_id=7905, employee_id=214)
    other_camp_no, _ = await _seed_camp(test_db_session, organization_id=9605, engagement_id=9605)

    response = await async_client.get(
        f"/reports/camps/{other_camp_no}/participants",
        headers=_auth_header(7904),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_camp_participants_onboarding_assistant(async_client, test_db_session):
    test_db_session.add(User(user_id=7906, age=30, phone="7906000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(
        Employee(employee_id=215, user_id=7906, role="onboarding_assistant", status="active")
    )
    await test_db_session.commit()

    camp_no = await _seed_camp_participants_with_profile_fields(
        test_db_session,
        organization_id=9606,
        engagement_id=9606,
    )

    response = await async_client.get(
        f"/reports/camps/{camp_no}/participants",
        headers=_auth_header(7906),
    )
    assert response.status_code == 200
    assert response.json()["meta"]["total"] == 1


@pytest.mark.asyncio
async def test_list_camp_participants_camp_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7907, employee_id=216)

    response = await async_client.get(
        "/reports/camps/99999999999/participants",
        headers=_auth_header(7907),
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_NOT_FOUND"
