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
from modules.experts.models import ConsultationBooking
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
async def test_init_all_camp_reports_creates_overall_departments_cities_and_combos(
    async_client, test_db_session
):
    await _seed_employee(test_db_session, user_id=7480, employee_id=80)
    camp_no, organization_id = await _seed_camp(
        test_db_session, organization_id=9180, engagement_id=9180
    )
    test_db_session.add(
        Engagement(
            engagement_id=9181,
            engagement_name="Camp Engagement HYD",
            organization_id=organization_id,
            camp_no=camp_no,
            engagement_code="CAMPREP2",
            engagement_type="bio_ai",
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="HYD",
            slot_duration=20,
            start_date=date(2026, 6, 23),
            end_date=date(2026, 6, 25),
            status="running",
        )
    )
    await test_db_session.commit()
    headers = _auth_header(7480)

    response = await async_client.post(f"/reports/camps/{camp_no}/init-all", headers=headers)
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["created"] == {
        "overall": 1,
        "departments": 2,
        "cities": 2,
        "city_departments": 4,
    }
    assert payload["skipped"] == {
        "overall": 0,
        "departments": 0,
        "cities": 0,
        "city_departments": 0,
    }
    assert sorted(payload["departments"]) == ["engineering", "sales"]
    assert sorted(city.lower() for city in payload["cities"]) == ["blr", "hyd"]

    rows = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalars().all()
    assert len(rows) == 9
    overall = [r for r in rows if r.department is None and r.city is None]
    departments = [r for r in rows if r.department is not None and r.city is None]
    cities = [r for r in rows if r.department is None and r.city is not None]
    combos = [r for r in rows if r.department is not None and r.city is not None]
    assert len(overall) == 1
    assert {r.department for r in departments} == {"sales", "engineering"}
    assert {r.city for r in cities} == {"BLR", "HYD"}
    assert {(r.city, r.department) for r in combos} == {
        ("BLR", "sales"),
        ("BLR", "engineering"),
        ("HYD", "sales"),
        ("HYD", "engineering"),
    }

    again = await async_client.post(f"/reports/camps/{camp_no}/init-all", headers=headers)
    assert again.status_code == 200
    skipped = again.json()["data"]["skipped"]
    assert skipped == {
        "overall": 1,
        "departments": 2,
        "cities": 2,
        "city_departments": 4,
    }
    assert again.json()["data"]["created"] == {
        "overall": 0,
        "departments": 0,
        "cities": 0,
        "city_departments": 0,
    }


@pytest.mark.asyncio
async def test_init_all_camp_reports_requires_auth(async_client):
    response = await async_client.post("/reports/camps/123/init-all")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_init_all_camp_reports_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7481, employee_id=81)
    headers = _auth_header(7481)

    response = await async_client.post("/reports/camps/999999998/init-all", headers=headers)
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
    init_city = await async_client.post(f"/reports/camps/{camp_no}/BLR/init", headers=headers)
    assert init_city.status_code == 201
    init_city_dept = await async_client.post(
        f"/reports/camps/{camp_no}/BLR/department/sales/init",
        headers=headers,
    )
    assert init_city_dept.status_code == 201

    seeded = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalars().all()
    assert len(seeded) == 4

    # DELETE /reports/camps/{camp_no} removes overall, department, city, and city×department
    delete_all = await async_client.delete(f"/reports/camps/{camp_no}", headers=headers)
    assert delete_all.status_code == 200

    remaining = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalars().all()
    assert remaining == []

    # Idempotent second delete should 404 once nothing remains
    delete_again = await async_client.delete(f"/reports/camps/{camp_no}", headers=headers)
    assert delete_again.status_code == 404


@pytest.mark.asyncio
async def test_delete_department_camp_report(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7407, employee_id=57)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9107, engagement_id=9107)
    headers = _auth_header(7407)

    init_overall = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init_overall.status_code == 201
    init_dept = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init_dept.status_code == 201

    delete_dept = await async_client.delete(
        f"/reports/camps/{camp_no}/department/sales",
        headers=headers,
    )
    assert delete_dept.status_code == 200

    remaining = (
        await test_db_session.execute(select(CampReport).where(CampReport.camp_no == camp_no))
    ).scalars().all()
    assert len(remaining) == 1
    assert remaining[0].department is None


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
async def test_refresh_city_camp_report_persists_participation_by_age(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7620, employee_id=920)
    await _seed_participation_section(test_db_session, report_sections=9610)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9610,
        engagement_id=9610,
    )
    headers = _auth_header(7620)

    init_overall = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init_overall.status_code == 201
    overall_id = init_overall.json()["data"]["report_id"]

    init_city = await async_client.post(f"/reports/camps/{camp_no}/BLR/init", headers=headers)
    assert init_city.status_code == 201
    city_id = init_city.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/BLR/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["data"]["total_enrolled"] == 3
    assert section["data"]["enrolled"] == [1, 0, 1, 0, 1]

    test_db_session.expire_all()
    city_row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == city_id))
    ).scalar_one()
    overall_row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == overall_id))
    ).scalar_one()
    assert city_row.report["participation_by_age"]["data"]["total_enrolled"] == 3
    assert city_row.report["meta"]["summary_available"] is True
    assert "participation_by_age" not in (overall_row.report or {})

    dashboard = await async_client.get(
        f"/reports/camps/{camp_no}/BLR/dashboard",
        headers=headers,
        params={"section": "participation_by_age"},
    )
    assert dashboard.status_code == 200
    assert dashboard.json()["data"]["data"]["total_enrolled"] == 3


@pytest.mark.asyncio
async def test_refresh_city_department_camp_report_persists_participation_by_age(
    async_client,
    test_db_session,
):
    await _seed_employee(test_db_session, user_id=7621, employee_id=921)
    await _seed_participation_section(test_db_session, report_sections=9611)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9611,
        engagement_id=9611,
    )
    headers = _auth_header(7621)

    init_dept = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init_dept.status_code == 201
    department_id = init_dept.json()["data"]["report_id"]

    init_combo = await async_client.post(
        f"/reports/camps/{camp_no}/BLR/department/sales/init",
        headers=headers,
    )
    assert init_combo.status_code == 201
    combo_id = init_combo.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/BLR/department/sales/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["data"]["total_enrolled"] == 2
    assert section["data"]["enrolled"] == [1, 0, 1, 0, 0]

    test_db_session.expire_all()
    combo_row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == combo_id))
    ).scalar_one()
    department_row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == department_id))
    ).scalar_one()
    assert combo_row.report["participation_by_age"]["data"]["total_enrolled"] == 2
    assert combo_row.report["meta"]["summary_available"] is True
    assert "participation_by_age" not in (department_row.report or {})


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
            contact_person_user_ids={"organization_managers": [manager_user_id]},
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
async def test_update_camp_report_dashboard_section_payload(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7610, employee_id=80)
    await _seed_participation_section(test_db_session, report_sections=23)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9310,
        engagement_id=9310,
    )
    headers = _auth_header(7610)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    refresh = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert refresh.status_code == 200

    edited = {
        "data": {"total_enrolled": 99, "age_group": ["18-30"], "enrolled": [99], "percent": [100]},
        "name": "Participation by Age",
        "description": "manually edited",
    }
    update = await async_client.put(
        f"/reports/camps/{camp_no}/dashboard",
        headers=headers,
        json={"section": "participation_by_age", "payload": edited},
    )
    assert update.status_code == 200
    assert update.json()["data"]["report_id"] == report_id
    assert update.json()["data"]["section"]["data"]["total_enrolled"] == 99
    assert update.json()["data"]["section"]["description"] == "manually edited"

    dashboard = await async_client.get(
        f"/reports/camps/{camp_no}/dashboard",
        headers=headers,
        params={"section": "participation_by_age"},
    )
    assert dashboard.status_code == 200
    assert dashboard.json()["data"]["data"]["total_enrolled"] == 99

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 99
    assert row.report["participation_by_age"]["description"] == "manually edited"


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
    from modules.experts.models import ExpertTypeModel
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)

    existing_types = (
        await test_db_session.execute(select(ExpertTypeModel.type_key))
    ).scalars().all()
    existing_type_keys = {str(k) for k in existing_types}
    to_add = []
    if "doctor" not in existing_type_keys:
        to_add.append(ExpertTypeModel(type_key="doctor", type="Doctor"))
    if "nutritionist" not in existing_type_keys:
        to_add.append(ExpertTypeModel(type_key="nutritionist", type="Nutritionist"))
    if to_add:
        test_db_session.add_all(to_add)
        await test_db_session.flush()

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
                booking_id="BK-94001",
            ),
            EngagementParticipant(
                engagement_participant_id=94002,
                engagement_id=engagement_id,
                user_id=94002,
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
                booking_id="BK-94002",
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
                booking_id="BK-94004",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            ConsultationBooking(
                consultation_id=940001,
                engagement_participant_id=94001,
                expert_type="doctor",
                want=True,
            ),
            ConsultationBooking(
                consultation_id=940002,
                engagement_participant_id=94002,
                expert_type="doctor",
                want=False,
            ),
            ConsultationBooking(
                consultation_id=940003,
                engagement_participant_id=94004,
                expert_type="doctor",
                want=True,
            ),
            ConsultationBooking(
                consultation_id=940004,
                engagement_participant_id=94004,
                expert_type="nutritionist",
                want=True,
            ),
        ]
    )
    await test_db_session.flush()

    participants = (
        await test_db_session.execute(
            select(EngagementParticipant).where(
                EngagementParticipant.engagement_participant_id.in_([94001, 94002, 94003, 94004])
            )
        )
    ).scalars().all()
    booking_ids_by_participant = {
        94001: [940001],
        94002: [940002],
        94004: [940003, 940004],
    }
    for participant in participants:
        ids = booking_ids_by_participant.get(participant.engagement_participant_id)
        if ids:
            participant.consultation_booking_ids = ids
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
    assert data["nutritionist_consultation"] == 1
    assert data["doctor_and_nutritionist_consultation"] == 1
    assert data["consultations"]["doctor"] == 2
    assert data["consultations"]["nutritionist"] == 1
    assert data["consultations"]["doctor_nutritionist"] == 1
    assert data["bio_ai_report_generated"] == 3
    assert data["high_risk_group"] == 2
    assert data["caution_risk_group"] == 1
    assert data["good_risk_group"] == 0
    assert data["questionnaire_completed"] == 0
    assert (
        data["high_risk_group"]
        + data["caution_risk_group"]
        + data["good_risk_group"]
        == data["bio_ai_report_generated"]
    )

    assert "report_bts" in payload
    assert payload["report_bts"]["status"] == "ok"
    assert payload["report_bts"]["expected"]["employees_enrolled"] == 4
    assert "risk_groups" in payload["report_bts"]["details"]
    assert len(payload["report_bts"]["details"]["risk_groups"]["people"]) == 3

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["kpis"]["data"]["employees_enrolled"] == 4
    assert row.report_bts is not None
    assert row.report_bts["kpis"]["status"] == "ok"


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
    assert data["nutritionist_consultation"] == 0
    assert data["doctor_and_nutritionist_consultation"] == 0
    assert data["bio_ai_report_generated"] == 2
    assert data["high_risk_group"] == 1
    assert data["caution_risk_group"] == 1
    assert data["good_risk_group"] == 0


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
    assert data["elevated_metabolic_score"] == 50.0
    assert "total_employees" not in data
    assert "total_enrolled" not in data

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["overall_risk_score"]["data"]["elevated_metabolic_score"] == 50.0
    assert "total_employees" not in row.report["overall_risk_score"]["data"]
    assert row.report_bts["overall_risk_score"]["status"] == "ok"
    assert "elevated_math" in row.report_bts["overall_risk_score"]["details"]
    assert "bands" in row.report_bts["overall_risk_score"]["details"]


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
    assert data["elevated_metabolic_score"] == 0.0
    assert "total_employees" not in data


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
    assert row.report["overall_risk_score"]["data"]["elevated_metabolic_score"] == 50.0
    assert "total_employees" not in row.report["overall_risk_score"]["data"]

    overall_again = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "overall_risk_score"},
    )
    assert overall_again.status_code == 200
    assert overall_again.json()["data"]["section"]["data"]["elevated_metabolic_score"] == 50.0
    assert "total_employees" not in overall_again.json()["data"]["section"]["data"]
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 6


async def _seed_oxidative_stress_section(test_db_session, *, report_sections: int = 210):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(
                CampReportSection.section_key == "distribution_by_oxidative_stress"
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.section = "Oxidative Stress Distribution"
        existing.description = "Distribution of oxidative stress risk bands across assessed employees"
        await test_db_session.commit()
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Oxidative Stress Distribution",
        section_key="distribution_by_oxidative_stress",
        description="Distribution of oxidative stress risk bands across assessed employees",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_oxidative_stress_camp_data(
    test_db_session,
    *,
    organization_id: int = 9601,
    engagement_id: int = 9601,
):
    from modules.assessments.models import AssessmentInstance, AssessmentPackage
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)

    test_db_session.add_all(
        [
            AssessmentPackage(
                package_id=9601,
                package_code="OXSPKG1",
                display_name="Bio AI Package",
                assessment_type_code="1",
                status="active",
            ),
            AssessmentPackage(
                package_id=9602,
                package_code="OXSPKG2",
                display_name="Bio AI Package 2",
                assessment_type_code="2",
                status="active",
            ),
            AssessmentPackage(
                package_id=9603,
                package_code="OXSPKG7",
                display_name="FitPrint Package",
                assessment_type_code="7",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            User(user_id=96001, age=30, phone="960010000000", status="active"),
            User(user_id=96002, age=35, phone="960020000000", status="active"),
            User(user_id=96003, age=40, phone="960030000000", status="active"),
            User(user_id=96004, age=45, phone="960040000000", status="active"),
            User(user_id=96005, age=50, phone="960050000000", status="active"),
            User(user_id=96006, age=28, phone="960060000000", status="active"),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=96001,
                engagement_id=engagement_id,
                user_id=96001,
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=96002,
                engagement_id=engagement_id,
                user_id=96002,
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=96003,
                engagement_id=engagement_id,
                user_id=96003,
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=96004,
                engagement_id=engagement_id,
                user_id=96004,
                engagement_date=start,
                slot_start_time=time(11, 20),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=96005,
                engagement_id=engagement_id,
                user_id=96005,
                engagement_date=start,
                slot_start_time=time(12, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=96006,
                engagement_id=engagement_id,
                user_id=96006,
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
                assessment_instance_id=96001,
                user_id=96001,
                engagement_id=engagement_id,
                package_id=9601,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=96002,
                user_id=96002,
                engagement_id=engagement_id,
                package_id=9602,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=96003,
                user_id=96003,
                engagement_id=engagement_id,
                package_id=9601,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=96004,
                user_id=96004,
                engagement_id=engagement_id,
                package_id=9602,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=96005,
                user_id=96005,
                engagement_id=engagement_id,
                package_id=9601,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=96006,
                user_id=96006,
                engagement_id=engagement_id,
                package_id=9603,
                status="completed",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            IndividualHealthReport(
                report_id=96001,
                user_id=96001,
                assessment_instance_id=96001,
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 20},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=96002,
                user_id=96002,
                assessment_instance_id=96002,
                engagement_id=engagement_id,
                reports={
                    "data": {
                        "diseases": [
                            {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 35},
                        ]
                    }
                },
            ),
            IndividualHealthReport(
                report_id=96003,
                user_id=96003,
                assessment_instance_id=96003,
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 50},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=96004,
                user_id=96004,
                assessment_instance_id=96004,
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 65},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=96005,
                user_id=96005,
                assessment_instance_id=96005,
                engagement_id=engagement_id,
                reports={"metabolic_age": 45.0},
            ),
            IndividualHealthReport(
                report_id=96006,
                user_id=96006,
                assessment_instance_id=96006,
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "oxidative_stress", "name": "Oxidative stress", "risk_score_scaled": 10},
                    ]
                },
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_refresh_camp_report_distribution_by_oxidative_stress(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7901, employee_id=211)
    await _seed_oxidative_stress_section(test_db_session, report_sections=211)
    camp_no = await _seed_oxidative_stress_camp_data(
        test_db_session, organization_id=9601, engagement_id=9601
    )
    headers = _auth_header(7901)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_oxidative_stress"},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["report_id"] == report_id
    section = payload["section"]
    assert section["name"] == "Oxidative Stress Distribution"
    assert section["description"] == "Distribution of oxidative stress risk bands across assessed employees"
    data = section["data"]
    assert data["group"] == ["low", "moderate", "high", "very_high"]
    assert data["count"] == [1, 1, 1, 1]
    assert data["percent"] == [25.0, 25.0, 25.0, 25.0]
    assert data["total_employees"] == 4
    assert data["elevated_oxidative_stress_percent"] == 50.0

    bts = payload.get("report_bts")
    assert bts is not None
    assert bts["status"] == "ok"
    assert bts["status"] != "not_implemented"
    assert "bands" in bts["details"]
    assert "elevated_math" in bts["details"]
    assert bts["details"]["elevated_math"]["result_percent"] == 50.0

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["distribution_by_oxidative_stress"]["data"]["total_employees"] == 4
    assert row.report_bts is not None
    assert row.report_bts["distribution_by_oxidative_stress"]["status"] == "ok"
    assert "bands" in row.report_bts["distribution_by_oxidative_stress"]["details"]


@pytest.mark.asyncio
async def test_refresh_department_camp_report_distribution_by_oxidative_stress(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7902, employee_id=212)
    await _seed_oxidative_stress_section(test_db_session, report_sections=212)
    camp_no = await _seed_oxidative_stress_camp_data(
        test_db_session, organization_id=9602, engagement_id=9602
    )
    headers = _auth_header(7902)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "distribution_by_oxidative_stress"},
    )
    assert response.status_code == 200
    data = response.json()["data"]["section"]["data"]
    assert data["count"] == [1, 1, 0, 0]
    assert data["total_employees"] == 2
    assert data["elevated_oxidative_stress_percent"] == 0.0


@pytest.mark.asyncio
async def test_refresh_oxidative_stress_updates_existing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7903, employee_id=213)
    await _seed_participation_section(test_db_session, report_sections=213)
    await _seed_oxidative_stress_section(test_db_session, report_sections=214)
    camp_no = await _seed_oxidative_stress_camp_data(
        test_db_session, organization_id=9603, engagement_id=9603
    )
    headers = _auth_header(7903)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    participation = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert participation.status_code == 200

    oxidative = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_oxidative_stress"},
    )
    assert oxidative.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "participation_by_age" in row.report
    assert "distribution_by_oxidative_stress" in row.report
    assert row.report["distribution_by_oxidative_stress"]["data"]["total_employees"] == 4

    oxidative_again = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_oxidative_stress"},
    )
    assert oxidative_again.status_code == 200
    assert oxidative_again.json()["data"]["section"]["data"]["total_employees"] == 4
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
            email="jane.doe@example.com",
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
            participants_employee_id="EMP-96001",
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
    assert row["phone"] == "********0000"
    assert row["email"] == "****e.doe@example.com"
    assert row["age"] == 30
    assert row["gender"] == "female"
    assert row["participant_blood_group"] == "O+"
    assert row["participant_department"] == "engineering"
    assert row["participants_employee_id"] == "EMP-96001"
    assert row["questionnaires"] == {}
    assert row["reports"] == {
        "blood_report_generated": False,
        "blood_report_sent": False,
        "bio_ai_report_generated": False,
        "bio_ai_report_sent": False,
    }
    assert row["consultations"] is False


async def _seed_camp_participants_enriched(test_db_session, *, organization_id: int = 9701, engagement_id: int = 9701):
    from datetime import datetime, timezone

    from sqlalchemy import text

    from modules.assessments.models import (
        AssessmentCategoryProgress,
        AssessmentInstance,
        AssessmentPackage,
        AssessmentPackageCategory,
    )
    from modules.engagements.models import AutoNotificationEvent, Engagement, EngagementNotification
    from modules.notifications.models import Notification
    from modules.questionnaire.models import QuestionnaireCategory
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(
        test_db_session,
        organization_id=organization_id,
        engagement_id=engagement_id,
    )
    start = date(2026, 6, 23)
    pkg_id = organization_id
    cat_complete_id = organization_id + 1
    cat_incomplete_id = organization_id + 2
    user_id = organization_id + 10
    participant_id = organization_id + 20
    assessment_id = organization_id + 30
    blood_service = f"camp-blood-{organization_id}"
    bio_service = f"camp-bio-{organization_id}"

    engagement = (
        await test_db_session.execute(select(Engagement).where(Engagement.engagement_id == engagement_id))
    ).scalar_one()
    engagement.assessment_package_id = pkg_id

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code=f"PKG-{organization_id}",
            display_name=f"Package {organization_id}",
            assessment_type_code="2",
            status="active",
        )
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            QuestionnaireCategory(
                category_id=cat_complete_id,
                category_key="physical-measurement",
                display_name="Physical Measurement",
                category_of="metsights",
                status="active",
            ),
            QuestionnaireCategory(
                category_id=cat_incomplete_id,
                category_key="vitals",
                display_name="Vitals",
                category_of="metsights",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            AssessmentPackageCategory(
                id=organization_id + 100,
                package_id=pkg_id,
                category_id=cat_complete_id,
                display_order=1,
            ),
            AssessmentPackageCategory(
                id=organization_id + 101,
                package_id=pkg_id,
                category_id=cat_incomplete_id,
                display_order=2,
            ),
        ]
    )

    test_db_session.add(
        User(
            user_id=user_id,
            first_name="Kiran",
            last_name="Amrute",
            gender="male",
            age=32,
            phone="9867458059",
            email="pratheek.fitnastic@gmail.com",
            status="active",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=participant_id,
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=start,
            slot_start_time=time(10, 0),
            participant_department="hr_admin",
            participant_blood_group="AB+",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=pkg_id,
            engagement_id=engagement_id,
            status="active",
            metsights_record_id=f"REC-{assessment_id}",
            assigned_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            AssessmentCategoryProgress(
                id=organization_id + 200,
                assessment_instance_id=assessment_id,
                category_id=cat_complete_id,
                status="complete",
                is_submitted=True,
            ),
            AssessmentCategoryProgress(
                id=organization_id + 201,
                assessment_instance_id=assessment_id,
                category_id=cat_incomplete_id,
                status="incomplete",
                is_submitted=False,
            ),
        ]
    )

    test_db_session.add(
        IndividualHealthReport(
            report_id=organization_id + 300,
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_id,
            diagnostic_report_url="https://example.com/blood.pdf",
            report_url="https://example.com/bio-ai.pdf",
        )
    )

    test_db_session.add(
        ConsultationBooking(
            consultation_id=organization_id + 400,
            engagement_participant_id=participant_id,
            expert_type="doctor",
            want=True,
        )
    )

    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:blood_sk, :blood_sk, 'email', 'test-webhook', true, true, false, false), "
            "(:bio_sk, :bio_sk, 'email', 'test-webhook', true, false, true, false) "
            "ON CONFLICT (service_key) DO NOTHING"
        ),
        {"blood_sk": blood_service, "bio_sk": bio_service},
    )

    blood_event = (
        await test_db_session.execute(
            select(AutoNotificationEvent).where(AutoNotificationEvent.event_code == "blood_report_ready")
        )
    ).scalar_one_or_none()
    if blood_event is None:
        blood_event = AutoNotificationEvent(
            id=organization_id + 500,
            engagement_type_ids=[1],
            event_code="blood_report_ready",
            display_name="Blood Report Ready",
        )
        test_db_session.add(blood_event)
        await test_db_session.flush()

    bio_event = (
        await test_db_session.execute(
            select(AutoNotificationEvent).where(AutoNotificationEvent.event_code == "bioai_report_ready")
        )
    ).scalar_one_or_none()
    if bio_event is None:
        bio_event = AutoNotificationEvent(
            id=organization_id + 501,
            engagement_type_ids=[1],
            event_code="bioai_report_ready",
            display_name="Bio AI Report Ready",
        )
        test_db_session.add(bio_event)
        await test_db_session.flush()

    test_db_session.add_all(
        [
            EngagementNotification(
                id=organization_id + 600,
                engagement_id=engagement_id,
                notification_event_id=blood_event.id,
                # Production stores JSON objects after migration 0118.
                notification_services=[{"service_key": blood_service, "external_link": None}],
            ),
            EngagementNotification(
                id=organization_id + 601,
                engagement_id=engagement_id,
                notification_event_id=bio_event.id,
                notification_services=[{"service_key": bio_service, "external_link": None}],
            ),
        ]
    )

    test_db_session.add_all(
        [
            Notification(
                service_key=blood_service,
                status="sent",
                channel="email",
                user={"user_ids": [user_id]},
                engagement_id=engagement_id,
                message="blood report sent",
                dispatched_at=datetime.now(timezone.utc),
            ),
            Notification(
                service_key=bio_service,
                status="sent",
                channel="email",
                user={"user_ids": [user_id]},
                engagement_id=engagement_id,
                message="bio ai report sent",
                dispatched_at=datetime.now(timezone.utc),
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no, user_id


@pytest.mark.asyncio
async def test_list_camp_participants_enriched_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7906, employee_id=215)
    camp_no, user_id = await _seed_camp_participants_enriched(test_db_session)
    headers = _auth_header(7906)

    response = await async_client.get(f"/reports/camps/{camp_no}/participants", headers=headers)
    assert response.status_code == 200
    row = next(item for item in response.json()["data"] if item["user_id"] == user_id)

    assert row["first_name"] == "Kiran"
    assert row["phone"] == "******8059"
    assert row["email"] == "*************stic@gmail.com"
    assert row["age"] == 32
    assert row["participant_department"] == "hr_admin"
    assert row["questionnaires"] == {
        "physical-measurement": True,
        "vitals": False,
    }
    assert row["reports"] == {
        "blood_report_generated": True,
        "blood_report_sent": True,
        "bio_ai_report_generated": True,
        "bio_ai_report_sent": True,
    }
    assert row["consultations"] is True


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


async def _seed_multi_city_camp_participants(
    test_db_session,
    *,
    organization_id: int = 9701,
    engagement_id: int = 9701,
):
    """Camp with BLR + MUM engagements and mixed department enrollments."""
    camp_no, _ = await _seed_camp(
        test_db_session,
        organization_id=organization_id,
        engagement_id=engagement_id,
    )
    start = date(2026, 6, 23)

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id + 1,
            engagement_name="Camp Engagement MUM",
            organization_id=organization_id,
            camp_no=camp_no,
            engagement_code="CAMPMUM1",
            engagement_type="bio_ai",
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="MUM",
            slot_duration=20,
            start_date=start,
            end_date=date(2026, 6, 25),
            status="running",
        )
    )
    test_db_session.add_all(
        [
            User(user_id=97001, age=25, phone="970010000000", status="active"),
            User(user_id=97002, age=30, phone="970020000000", status="active"),
            User(user_id=97003, age=35, phone="970030000000", status="active"),
            User(user_id=97004, age=40, phone="970040000000", status="active"),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=97001,
                engagement_id=engagement_id,
                user_id=97001,
                engagement_date=start,
                slot_start_time=time(9, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=97002,
                engagement_id=engagement_id,
                user_id=97002,
                engagement_date=start,
                slot_start_time=time(9, 20),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=97003,
                engagement_id=engagement_id + 1,
                user_id=97003,
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=97004,
                engagement_id=engagement_id + 1,
                user_id=97004,
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="engineering",
            ),
            # Duplicate enrollment in BLR sales (same user twice)
            EngagementParticipant(
                engagement_participant_id=97005,
                engagement_id=engagement_id,
                user_id=97001,
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="sales",
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_list_department_camp_participants_filters_by_slug(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7910, employee_id=220)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9702,
        engagement_id=9702,
    )
    headers = _auth_header(7910)

    response = await async_client.get(
        f"/reports/camps/{camp_no}/department/sales/participants",
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 3
    assert len(body["data"]) == 3
    assert all(row["participant_department"] == "sales" for row in body["data"])
    assert [row["user_id"] for row in body["data"]].count(92001) == 2


@pytest.mark.asyncio
async def test_list_department_camp_participants_unknown_slug_404(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7911, employee_id=221)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9703,
        engagement_id=9703,
    )

    response = await async_client.get(
        f"/reports/camps/{camp_no}/department/marketing/participants",
        headers=_auth_header(7911),
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "DEPARTMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_list_city_camp_participants_filters_by_city(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7912, employee_id=222)
    camp_no = await _seed_multi_city_camp_participants(
        test_db_session,
        organization_id=9704,
        engagement_id=9704,
    )
    headers = _auth_header(7912)

    response = await async_client.get(
        f"/reports/camps/{camp_no}/MUM/participants",
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 2
    assert {row["user_id"] for row in body["data"]} == {97003, 97004}


@pytest.mark.asyncio
async def test_list_city_camp_participants_unknown_city_404(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7913, employee_id=223)
    camp_no = await _seed_multi_city_camp_participants(
        test_db_session,
        organization_id=9705,
        engagement_id=9705,
    )

    response = await async_client.get(
        f"/reports/camps/{camp_no}/DEL/participants",
        headers=_auth_header(7913),
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CITY_NOT_FOUND"


@pytest.mark.asyncio
async def test_list_city_department_camp_participants(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7914, employee_id=224)
    camp_no = await _seed_multi_city_camp_participants(
        test_db_session,
        organization_id=9706,
        engagement_id=9706,
    )
    headers = _auth_header(7914)

    response = await async_client.get(
        f"/reports/camps/{camp_no}/BLR/department/sales/participants",
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 2
    assert all(row["participant_department"] == "sales" for row in body["data"])
    assert [row["user_id"] for row in body["data"]].count(97001) == 2


@pytest.mark.asyncio
async def test_list_city_department_camp_participants_zero_matches(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7915, employee_id=225)
    camp_no, _ = await _seed_camp(
        test_db_session,
        organization_id=9708,
        engagement_id=9708,
    )
    start = date(2026, 6, 23)
    test_db_session.add(
        User(user_id=97010, age=28, phone="970100000000", status="active")
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=97010,
            engagement_id=9708,
            user_id=97010,
            engagement_date=start,
            slot_start_time=time(9, 0),
            participant_department="sales",
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        f"/reports/camps/{camp_no}/BLR/department/engineering/participants",
        headers=_auth_header(7915),
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 0
    assert body["data"] == []


async def _seed_physical_activity_section(test_db_session, *, report_sections: int = 300):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(
                CampReportSection.section_key == "distribution_by_physical_activity_frequency"
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.section = "Physical Activity Distribution"
        existing.description = "Distribution of daily physical activity frequency by gender"
        await test_db_session.commit()
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Physical Activity Distribution",
        section_key="distribution_by_physical_activity_frequency",
        description="Distribution of daily physical activity frequency by gender",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_physical_activity_camp_data(
    test_db_session,
    *,
    organization_id: int = 9701,
    engagement_id: int = 9701,
):
    from modules.assessments.models import AssessmentInstance, AssessmentPackage
    from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireDefinition, QuestionnaireResponse

    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)
    pkg_id = engagement_id
    q_id = engagement_id
    cat_id = engagement_id
    user_base = engagement_id * 10

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code=f"PAPKG{engagement_id}",
            display_name="Bio AI Package",
            assessment_type_code="1",
            status="active",
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        QuestionnaireCategory(
            category_id=cat_id,
            category_key=f"pa_cat_{engagement_id}",
            display_name="Lifestyle",
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=q_id,
            question_key="physical_activity_frequency",
            question_text="How much time do you spend engaging in physical activity or exercise daily?",
            question_type="single_choice",
            status="active",
        )
    )
    await test_db_session.flush()

    user_ids = [user_base + offset for offset in range(1, 7)]
    test_db_session.add_all(
        [
            User(user_id=user_ids[0], age=30, gender="male", phone=f"{user_ids[0]}000000000", status="active"),
            User(user_id=user_ids[1], age=35, gender="Male", phone=f"{user_ids[1]}000000000", status="active"),
            User(user_id=user_ids[2], age=40, gender="female", phone=f"{user_ids[2]}000000000", status="active"),
            User(user_id=user_ids[3], age=45, gender="F", phone=f"{user_ids[3]}000000000", status="active"),
            User(user_id=user_ids[4], age=50, gender="m", phone=f"{user_ids[4]}000000000", status="active"),
            User(user_id=user_ids[5], age=28, gender="female", phone=f"{user_ids[5]}000000000", status="active"),
        ]
    )
    await test_db_session.flush()

    participant_ids = [user_base + offset for offset in range(1, 7)]
    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=participant_ids[0],
                engagement_id=engagement_id,
                user_id=user_ids[0],
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[1],
                engagement_id=engagement_id,
                user_id=user_ids[1],
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[2],
                engagement_id=engagement_id,
                user_id=user_ids[2],
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[3],
                engagement_id=engagement_id,
                user_id=user_ids[3],
                engagement_date=start,
                slot_start_time=time(11, 20),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[4],
                engagement_id=engagement_id,
                user_id=user_ids[4],
                engagement_date=start,
                slot_start_time=time(12, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[5],
                engagement_id=engagement_id,
                user_id=user_ids[5],
                engagement_date=start,
                slot_start_time=time(12, 20),
                participant_department="sales",
            ),
        ]
    )
    await test_db_session.flush()

    instance_specs = [
        (user_base + 1, user_ids[0], "1"),
        (user_base + 2, user_ids[1], "2"),
        (user_base + 3, user_ids[2], "3"),
        (user_base + 4, user_ids[3], "5"),
        (user_base + 5, user_ids[4], "3"),
        (user_base + 6, user_ids[5], "1"),
    ]
    test_db_session.add_all(
        [
            AssessmentInstance(
                assessment_instance_id=instance_id,
                user_id=user_id,
                engagement_id=engagement_id,
                package_id=pkg_id,
                status="completed",
            )
            for instance_id, user_id, _answer in instance_specs
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            QuestionnaireResponse(
                response_id=engagement_id * 100 + instance_id,
                assessment_instance_id=instance_id,
                question_id=q_id,
                category_id=cat_id,
                answer=answer,
            )
            for instance_id, _user_id, answer in instance_specs
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_refresh_camp_report_physical_activity_distribution(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8001, employee_id=301)
    await _seed_physical_activity_section(test_db_session, report_sections=301)
    camp_no = await _seed_physical_activity_camp_data(
        test_db_session, organization_id=9701, engagement_id=9701
    )
    headers = _auth_header(8001)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_physical_activity_frequency"},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["report_id"] == report_id
    section = payload["section"]
    assert section["name"] == "Physical Activity Distribution"
    assert section["description"] == "Distribution of daily physical activity frequency by gender"
    male = section["data"]["male"]
    female = section["data"]["female"]
    assert male["group"] == [
        "less_than_30mins",
        "30_60_mins",
        "more_than_60_mins",
        "rarely_or_never",
    ]
    assert male["count"] == [1, 1, 1, 0]
    assert male["percent"] == [33.3, 33.3, 33.3, 0.0]
    assert female["count"] == [1, 0, 1, 1]
    assert female["percent"] == [33.3, 0.0, 33.3, 33.3]
    bts = payload.get("report_bts") or {}
    assert bts.get("status") in {"ok", "mismatch"}
    assert "details" in bts

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["distribution_by_physical_activity_frequency"]["data"]["male"]["count"] == [1, 1, 1, 0]


@pytest.mark.asyncio
async def test_refresh_department_camp_report_physical_activity_distribution(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8002, employee_id=302)
    await _seed_physical_activity_section(test_db_session, report_sections=302)
    camp_no = await _seed_physical_activity_camp_data(
        test_db_session, organization_id=9702, engagement_id=9702
    )
    headers = _auth_header(8002)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "distribution_by_physical_activity_frequency"},
    )
    assert response.status_code == 200
    male = response.json()["data"]["section"]["data"]["male"]
    female = response.json()["data"]["section"]["data"]["female"]
    assert male["count"] == [1, 1, 0, 0]
    assert male["percent"] == [50.0, 50.0, 0.0, 0.0]
    assert female["count"] == [1, 0, 0, 0]
    assert female["percent"] == [100.0, 0.0, 0.0, 0.0]


@pytest.mark.asyncio
async def test_refresh_physical_activity_distribution_updates_existing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8003, employee_id=303)
    await _seed_participation_section(test_db_session, report_sections=303)
    await _seed_physical_activity_section(test_db_session, report_sections=304)
    camp_no = await _seed_physical_activity_camp_data(
        test_db_session, organization_id=9703, engagement_id=9703
    )
    headers = _auth_header(8003)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    participation = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert participation.status_code == 200

    physical = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_physical_activity_frequency"},
    )
    assert physical.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "participation_by_age" in row.report
    assert "distribution_by_physical_activity_frequency" in row.report
    assert row.report["distribution_by_physical_activity_frequency"]["data"]["male"]["count"] == [1, 1, 1, 0]

    physical_again = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_physical_activity_frequency"},
    )
    assert physical_again.status_code == 200
    assert (
        physical_again.json()["data"]["section"]["data"]["female"]["count"] == [1, 0, 1, 1]
    )
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 6


async def _seed_gender_metabolic_syndrome_section(test_db_session, *, report_sections: int = 400):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(
                CampReportSection.section_key == "distribution_by_gender_by_metabolic_syndrome"
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.section = "Disease Risk by Gender"
        existing.description = "Distribution of metabolic disease risk bands by gender"
        await test_db_session.commit()
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Disease Risk by Gender",
        section_key="distribution_by_gender_by_metabolic_syndrome",
        description="Distribution of metabolic disease risk bands by gender",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_gender_metabolic_syndrome_camp_data(
    test_db_session,
    *,
    organization_id: int = 9801,
    engagement_id: int = 9801,
):
    from modules.assessments.models import AssessmentInstance, AssessmentPackage
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(test_db_session, organization_id=organization_id, engagement_id=engagement_id)
    start = date(2026, 6, 23)
    pkg_basic = engagement_id * 10 + 1
    pkg_pro = engagement_id * 10 + 2
    pkg_fitprint = engagement_id * 10 + 3
    user_base = engagement_id * 100
    participant_base = engagement_id * 100
    instance_base = engagement_id * 100
    report_base = engagement_id * 100

    test_db_session.add_all(
        [
            AssessmentPackage(
                package_id=pkg_basic,
                package_code=f"GMSPKG{engagement_id}1",
                display_name="Bio AI Package",
                assessment_type_code="1",
                status="active",
            ),
            AssessmentPackage(
                package_id=pkg_pro,
                package_code=f"GMSPKG{engagement_id}2",
                display_name="Bio AI Package 2",
                assessment_type_code="2",
                status="active",
            ),
            AssessmentPackage(
                package_id=pkg_fitprint,
                package_code=f"GMSPKG{engagement_id}7",
                display_name="FitPrint Package",
                assessment_type_code="7",
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    user_ids = [user_base + offset for offset in range(1, 7)]
    test_db_session.add_all(
        [
            User(user_id=user_ids[0], age=30, gender="male", phone=f"{user_ids[0]}000000000", status="active"),
            User(user_id=user_ids[1], age=35, gender="male", phone=f"{user_ids[1]}000000000", status="active"),
            User(user_id=user_ids[2], age=40, gender="female", phone=f"{user_ids[2]}000000000", status="active"),
            User(user_id=user_ids[3], age=45, gender="female", phone=f"{user_ids[3]}000000000", status="active"),
            User(user_id=user_ids[4], age=50, gender="female", phone=f"{user_ids[4]}000000000", status="active"),
            User(user_id=user_ids[5], age=28, gender="male", phone=f"{user_ids[5]}000000000", status="active"),
        ]
    )
    await test_db_session.flush()

    participant_ids = [participant_base + offset for offset in range(1, 7)]
    test_db_session.add_all(
        [
            EngagementParticipant(
                engagement_participant_id=participant_ids[0],
                engagement_id=engagement_id,
                user_id=user_ids[0],
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[1],
                engagement_id=engagement_id,
                user_id=user_ids[1],
                engagement_date=start,
                slot_start_time=time(10, 20),
                participant_department="sales",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[2],
                engagement_id=engagement_id,
                user_id=user_ids[2],
                engagement_date=start,
                slot_start_time=time(11, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[3],
                engagement_id=engagement_id,
                user_id=user_ids[3],
                engagement_date=start,
                slot_start_time=time(11, 20),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[4],
                engagement_id=engagement_id,
                user_id=user_ids[4],
                engagement_date=start,
                slot_start_time=time(12, 0),
                participant_department="engineering",
            ),
            EngagementParticipant(
                engagement_participant_id=participant_ids[5],
                engagement_id=engagement_id,
                user_id=user_ids[5],
                engagement_date=start,
                slot_start_time=time(12, 20),
                participant_department="sales",
            ),
        ]
    )
    await test_db_session.flush()

    instance_ids = [instance_base + offset for offset in range(1, 7)]
    test_db_session.add_all(
        [
            AssessmentInstance(
                assessment_instance_id=instance_ids[0],
                user_id=user_ids[0],
                engagement_id=engagement_id,
                package_id=pkg_basic,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=instance_ids[1],
                user_id=user_ids[1],
                engagement_id=engagement_id,
                package_id=pkg_pro,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=instance_ids[2],
                user_id=user_ids[2],
                engagement_id=engagement_id,
                package_id=pkg_basic,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=instance_ids[3],
                user_id=user_ids[3],
                engagement_id=engagement_id,
                package_id=pkg_pro,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=instance_ids[4],
                user_id=user_ids[4],
                engagement_id=engagement_id,
                package_id=pkg_basic,
                status="completed",
            ),
            AssessmentInstance(
                assessment_instance_id=instance_ids[5],
                user_id=user_ids[5],
                engagement_id=engagement_id,
                package_id=pkg_fitprint,
                status="completed",
            ),
        ]
    )
    await test_db_session.flush()

    report_ids = [report_base + offset for offset in range(1, 7)]
    test_db_session.add_all(
        [
            IndividualHealthReport(
                report_id=report_ids[0],
                user_id=user_ids[0],
                assessment_instance_id=instance_ids[0],
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "hypertension", "risk_score_scaled": 20},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=report_ids[1],
                user_id=user_ids[1],
                assessment_instance_id=instance_ids[1],
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "hypertension", "risk_score_scaled": 50},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=report_ids[2],
                user_id=user_ids[2],
                assessment_instance_id=instance_ids[2],
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "hypertension", "risk_score_scaled": 35},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=report_ids[3],
                user_id=user_ids[3],
                assessment_instance_id=instance_ids[3],
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "diabetes", "risk_score_scaled": 10},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=report_ids[4],
                user_id=user_ids[4],
                assessment_instance_id=instance_ids[4],
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "oxidative_stress", "risk_score_scaled": 70},
                    ]
                },
            ),
            IndividualHealthReport(
                report_id=report_ids[5],
                user_id=user_ids[5],
                assessment_instance_id=instance_ids[5],
                engagement_id=engagement_id,
                reports={
                    "diseases": [
                        {"code": "hypertension", "risk_score_scaled": 60},
                    ]
                },
            ),
        ]
    )
    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_refresh_camp_report_distribution_by_gender_by_metabolic_syndrome(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8101, employee_id=401)
    await _seed_gender_metabolic_syndrome_section(test_db_session, report_sections=401)
    camp_no = await _seed_gender_metabolic_syndrome_camp_data(
        test_db_session, organization_id=9801, engagement_id=9801
    )
    headers = _auth_header(8101)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_gender_by_metabolic_syndrome"},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["report_id"] == report_id
    section = payload["section"]
    assert section["name"] == "Disease Risk by Gender"
    assert section["description"] == "Distribution of metabolic disease risk bands by gender"

    diseases = section["data"]["diseases"]
    codes = [d["code"] for d in diseases]
    assert codes == ["type_2_diabetes", "hypertension"]

    hypertension = next(d for d in diseases if d["code"] == "hypertension")
    assert hypertension["male"]["group"] == ["healthy", "increased", "high", "very_high"]
    assert hypertension["male"]["count"] == [1, 0, 1, 0]
    assert hypertension["male"]["percent"] == [50.0, 0.0, 50.0, 0.0]
    assert hypertension["male"]["elevated_percent"] == 50.0
    assert hypertension["female"]["count"] == [0, 1, 0, 0]
    assert hypertension["female"]["elevated_percent"] == 0.0

    diabetes = next(d for d in diseases if d["code"] == "type_2_diabetes")
    assert diabetes["female"]["count"] == [1, 0, 0, 0]

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["distribution_by_gender_by_metabolic_syndrome"]["data"]["diseases"][1]["code"] == "hypertension"

    bts = payload.get("report_bts")
    assert bts is not None
    assert bts["status"] == "ok"
    assert "diseases" in bts["details"]
    assert row.report_bts["distribution_by_gender_by_metabolic_syndrome"]["status"] == "ok"


@pytest.mark.asyncio
async def test_refresh_department_camp_report_distribution_by_gender_by_metabolic_syndrome(
    async_client, test_db_session
):
    await _seed_employee(test_db_session, user_id=8102, employee_id=402)
    await _seed_gender_metabolic_syndrome_section(test_db_session, report_sections=402)
    camp_no = await _seed_gender_metabolic_syndrome_camp_data(
        test_db_session, organization_id=9802, engagement_id=9802
    )
    headers = _auth_header(8102)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "distribution_by_gender_by_metabolic_syndrome"},
    )
    assert response.status_code == 200
    diseases = response.json()["data"]["section"]["data"]["diseases"]
    assert len(diseases) == 1
    assert diseases[0]["code"] == "hypertension"
    assert diseases[0]["male"]["count"] == [1, 0, 1, 0]
    assert diseases[0]["female"]["count"] == [0, 0, 0, 0]


@pytest.mark.asyncio
async def test_refresh_gender_metabolic_syndrome_updates_existing(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8103, employee_id=403)
    await _seed_participation_section(test_db_session, report_sections=403)
    await _seed_gender_metabolic_syndrome_section(test_db_session, report_sections=404)
    camp_no = await _seed_gender_metabolic_syndrome_camp_data(
        test_db_session, organization_id=9803, engagement_id=9803
    )
    headers = _auth_header(8103)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    participation = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert participation.status_code == 200

    gender_section = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_gender_by_metabolic_syndrome"},
    )
    assert gender_section.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "participation_by_age" in row.report
    assert "distribution_by_gender_by_metabolic_syndrome" in row.report
    assert len(row.report["distribution_by_gender_by_metabolic_syndrome"]["data"]["diseases"]) == 2

    gender_again = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "distribution_by_gender_by_metabolic_syndrome"},
    )
    assert gender_again.status_code == 200
    assert (
        gender_again.json()["data"]["section"]["data"]["diseases"][1]["male"]["count"] == [1, 0, 1, 0]
    )
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 6


async def _seed_positive_wins_section(test_db_session, *, report_sections: int = 100):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == "positive_wins")
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Positive Wins",
        section_key="positive_wins",
        description="Top healthy habits and profiles across the camp",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_positive_wins_camp_with_assessments(test_db_session, *, organization_id: int = 9301):
    from datetime import datetime, timezone

    from sqlalchemy import text

    from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
    from modules.engagements.models import Engagement
    from modules.questionnaire.models import (
        QuestionnaireCategory,
        QuestionnaireCategoryQuestion,
        QuestionnaireDefinition,
        QuestionnaireHealthyHabitRule,
        QuestionnaireOption,
        QuestionnaireResponse,
    )
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(
        test_db_session,
        organization_id=organization_id,
        engagement_id=organization_id,
    )
    start = date(2026, 6, 23)
    pkg_id = organization_id % 1000
    cat_id = organization_id + 100
    q_alcohol_id = organization_id + 200
    q_walk_id = organization_id + 201

    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'REF1', 'Diag Package 1', 'test_provider', 'active') "
            "ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    engagement = (
        await test_db_session.execute(
            select(Engagement).where(Engagement.engagement_id == organization_id)
        )
    ).scalar_one()
    engagement.assessment_package_id = pkg_id
    engagement.diagnostic_package_id = 1

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code=f"PKG{organization_id}",
            display_name=f"Package {organization_id}",
            assessment_type_code="2",
            status="active",
        )
    )
    await test_db_session.flush()

    users = [
        (organization_id + 1, "male", "sales"),
        (organization_id + 2, "female", "sales"),
        (organization_id + 3, "male", "engineering"),
    ]
    for user_id, gender, _dept in users:
        test_db_session.add(
            User(
                user_id=user_id,
                age=30,
                phone=f"{user_id}000000000",
                gender=gender,
                status="active",
            )
        )
    await test_db_session.flush()

    for idx, (user_id, _gender, dept) in enumerate(users):
        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=organization_id * 10 + idx + 1,
                engagement_id=organization_id,
                user_id=user_id,
                engagement_date=start,
                slot_start_time=time(10, idx * 20),
                participant_department=dept,
            )
        )
    await test_db_session.flush()

    test_db_session.add(
        QuestionnaireCategory(
            category_id=cat_id,
            category_key=f"hab_cat_{organization_id}",
            display_name="Habits",
            status="active",
        )
    )
    test_db_session.add_all(
        [
            QuestionnaireDefinition(
                question_id=q_alcohol_id,
                question_key="alcohol_consumption",
                question_text="Weekly alcohol?",
                question_type="single_choice",
                status="active",
            ),
            QuestionnaireDefinition(
                question_id=q_walk_id,
                question_key="daily_walk",
                question_text="Daily walk?",
                question_type="single_choice",
                status="active",
            ),
        ]
    )
    test_db_session.add_all(
        [
            QuestionnaireOption(question_id=q_alcohol_id, option_value="no_alcohol", display_name="None"),
            QuestionnaireOption(question_id=q_walk_id, option_value="yes_walk", display_name="Yes"),
        ]
    )
    await test_db_session.flush()

    test_db_session.add_all(
        [
            QuestionnaireCategoryQuestion(
                id=organization_id + 300,
                category_id=cat_id,
                question_id=q_alcohol_id,
                display_order=1,
            ),
            QuestionnaireCategoryQuestion(
                id=organization_id + 301,
                category_id=cat_id,
                question_id=q_walk_id,
                display_order=2,
            ),
            AssessmentPackageCategory(
                id=organization_id + 302,
                package_id=pkg_id,
                category_id=cat_id,
                display_order=1,
            ),
            QuestionnaireHealthyHabitRule(
                question_id=q_alcohol_id,
                habit_key="no_alcohol",
                habit_label="No Alcohol",
                display_order=1,
                condition_type="option_match",
                matched_option_values=["no_alcohol"],
                status="active",
            ),
            QuestionnaireHealthyHabitRule(
                question_id=q_walk_id,
                habit_key="daily_walk",
                habit_label="Daily Walk",
                display_order=2,
                condition_type="option_match",
                matched_option_values=["yes_walk"],
                status="active",
            ),
        ]
    )
    await test_db_session.flush()

    for idx, (user_id, _gender, _dept) in enumerate(users):
        assessment_id = organization_id + 400 + idx
        test_db_session.add(
            AssessmentInstance(
                assessment_instance_id=assessment_id,
                user_id=user_id,
                package_id=pkg_id,
                engagement_id=organization_id,
                status="completed",
                metsights_record_id=f"REC{assessment_id}",
                assigned_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
        )
        if idx < 2:
            test_db_session.add(
                QuestionnaireResponse(
                    response_id=organization_id + 500 + idx,
                    assessment_instance_id=assessment_id,
                    question_id=q_alcohol_id,
                    category_id=cat_id,
                    answer="no_alcohol",
                )
            )
        else:
            test_db_session.add(
                QuestionnaireResponse(
                    response_id=organization_id + 500 + idx,
                    assessment_instance_id=assessment_id,
                    question_id=q_walk_id,
                    category_id=cat_id,
                    answer="yes_walk",
                )
            )
        if idx < 2:
            diseases = [
                {
                    "code": "low_a",
                    "name": "Low A",
                    "risk_status": "Healthy",
                    "risk_score_scaled": 12,
                    "healthy_percentile": 50,
                },
            ]
            if idx == 0:
                diseases.append(
                    {
                        "code": "low_b",
                        "name": "Low B",
                        "risk_status": "Healthy",
                        "risk_score_scaled": 15,
                        "healthy_percentile": 50,
                    }
                )
        else:
            diseases = [
                {
                    "code": "low_c",
                    "name": "Low C",
                    "risk_status": "Healthy",
                    "risk_score_scaled": 10,
                    "healthy_percentile": 50,
                },
            ]
        test_db_session.add(
            IndividualHealthReport(
                report_id=organization_id + 600 + idx,
                user_id=user_id,
                engagement_id=organization_id,
                assessment_instance_id=assessment_id,
                reports={"metabolic_age": 30.0, "diseases": diseases},
                blood_parameters={
                    "b1": 5.0,
                    "b2": 5.0,
                    "b3": 5.0,
                    "a1": 5.0,
                    "a2": 5.0,
                    "g1": 5.0,
                },
            )
        )

    await test_db_session.commit()
    return camp_no


def _diag_multi_group_factory():
    from modules.diagnostics.schemas import (
        HealthParameterResponse,
        PackageTestsResponse,
        ParameterType,
        TestGroupResponse as DiagnosticTestGroupResponse,
    )

    def _hp(tid: int, tname: str, pkey: str) -> HealthParameterResponse:
        return HealthParameterResponse(
            test_id=tid,
            parameter_type=ParameterType.TEST,
            test_name=tname,
            parameter_key=pkey,
            unit="u",
            meaning=None,
            low_risk_lower_range_male=1.0,
            low_risk_higher_range_male=10.0,
            low_risk_lower_range_female=1.0,
            low_risk_higher_range_female=10.0,
            causes_when_high=None,
            causes_when_low=None,
            effects_when_high=None,
            effects_when_low=None,
            what_to_do_when_low=None,
            what_to_do_when_high=None,
            is_available=True,
            display_order=tid,
        )

    class _DiagMultiGroup:
        async def get_package_tests(self, db, *, package_id: int) -> PackageTestsResponse:
            return PackageTestsResponse(
                diagnostic_package_id=1,
                groups=[
                    DiagnosticTestGroupResponse(
                        group_id=1,
                        group_name="Beta",
                        group_key="beta",
                        test_count=3,
                        display_order=2,
                        tests=[_hp(1, "b1", "b1"), _hp(2, "b2", "b2"), _hp(3, "b3", "b3")],
                    ),
                    DiagnosticTestGroupResponse(
                        group_id=2,
                        group_name="Alpha",
                        group_key="alpha",
                        test_count=2,
                        display_order=1,
                        tests=[_hp(4, "a1", "a1"), _hp(5, "a2", "a2")],
                    ),
                    DiagnosticTestGroupResponse(
                        group_id=3,
                        group_name="Gamma",
                        group_key="gamma",
                        test_count=1,
                        display_order=3,
                        tests=[_hp(6, "g1", "g1")],
                    ),
                ],
            )

        async def get_health_parameter_by_parameter_key(self, db, *, parameter_key: str):
            return None

    return _DiagMultiGroup()


def _reports_service_for_positive_wins(*, diagnostics_service):
    from modules.assessments.repository import AssessmentsRepository
    from modules.audit.repository import AuditRepository
    from modules.audit.service import AuditService
    from modules.questionnaire.healthy_habits_service import HealthyHabitsService
    from modules.questionnaire.repository import QuestionnaireRepository
    from modules.reports.repository import ReportsRepository
    from modules.reports.service import ReportsService
    from tests.modules.reports.test_reports_routes import _FakeMetsightsService

    return ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(payload={}, should_fail=True),
        diagnostics_service=diagnostics_service,
        audit_service=AuditService(AuditRepository()),
        healthy_habits_service=HealthyHabitsService(QuestionnaireRepository()),
    )


@pytest.mark.asyncio
async def test_refresh_camp_report_positive_wins(async_client, fastapi_app, test_db_session):
    from modules.reports.dependencies import get_reports_service

    await _seed_employee(test_db_session, user_id=7601, employee_id=701)
    await _seed_positive_wins_section(test_db_session, report_sections=101)
    camp_no = await _seed_positive_wins_camp_with_assessments(test_db_session, organization_id=9301)
    headers = _auth_header(7601)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_positive_wins(
        diagnostics_service=_diag_multi_group_factory(),
    )

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "positive_wins"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    bts = response.json()["data"].get("report_bts")
    assert section["name"] == "Positive Wins"
    assert section["description"] == "Top healthy habits and profiles across the camp"
    assert [item["code"] for item in section["data"]["low_risk"]] == ["low_a", "low_b", "low_c"]
    assert section["data"]["healthy_habits"][0] == {
        "habit_key": "no_alcohol",
        "habit_label": "No Alcohol",
    }
    assert section["data"]["healthy_habits"][1] == {
        "habit_key": "daily_walk",
        "habit_label": "Daily Walk",
    }
    assert section["data"]["healthy_profiles"] == ["Alpha", "Beta", "Gamma"]

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["positive_wins"]["name"] == "Positive Wins"
    assert row.report["positive_wins"]["data"]["healthy_habits"][0]["habit_label"] == "No Alcohol"
    assert bts is not None
    assert bts["status"] == "ok"
    assert bts["details"]["method"]["section_kind"] == "positive_wins"
    assert row.report_bts["positive_wins"]["status"] == "ok"

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_refresh_department_camp_report_positive_wins(async_client, fastapi_app, test_db_session):
    from modules.reports.dependencies import get_reports_service

    class _DiagSalesOnly:
        async def get_package_tests(self, db, *, package_id: int):
            from modules.diagnostics.schemas import (
                HealthParameterResponse,
                PackageTestsResponse,
                ParameterType,
                TestGroupResponse as DiagnosticTestGroupResponse,
            )

            return PackageTestsResponse(
                diagnostic_package_id=1,
                groups=[
                    DiagnosticTestGroupResponse(
                        group_id=1,
                        group_name="Beta",
                        group_key="beta",
                        test_count=3,
                        display_order=1,
                        tests=[
                            HealthParameterResponse(
                                test_id=1,
                                parameter_type=ParameterType.TEST,
                                test_name="b1",
                                parameter_key="b1",
                                unit="u",
                                meaning=None,
                                low_risk_lower_range_male=1.0,
                                low_risk_higher_range_male=10.0,
                                low_risk_lower_range_female=1.0,
                                low_risk_higher_range_female=10.0,
                                causes_when_high=None,
                                causes_when_low=None,
                                effects_when_high=None,
                                effects_when_low=None,
                                what_to_do_when_low=None,
                                what_to_do_when_high=None,
                                is_available=True,
                                display_order=1,
                            ),
                        ],
                    ),
                ],
            )

        async def get_health_parameter_by_parameter_key(self, db, *, parameter_key: str):
            return None

    await _seed_employee(test_db_session, user_id=7602, employee_id=702)
    await _seed_positive_wins_section(test_db_session, report_sections=102)
    camp_no = await _seed_positive_wins_camp_with_assessments(test_db_session, organization_id=9302)
    headers = _auth_header(7602)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_positive_wins(
        diagnostics_service=_DiagSalesOnly(),
    )

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "positive_wins"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert len(section["data"]["healthy_habits"]) == 1
    assert section["data"]["healthy_habits"][0]["habit_label"] == "No Alcohol"
    assert section["data"]["healthy_profiles"] == ["Beta"]

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_refresh_camp_report_positive_wins_empty_camp(async_client, fastapi_app, test_db_session):
    from modules.reports.dependencies import get_reports_service
    from tests.modules.reports.test_reports_routes import _FakeDiagnosticsService

    await _seed_employee(test_db_session, user_id=7603, employee_id=703)
    await _seed_positive_wins_section(test_db_session, report_sections=103)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9303, engagement_id=9303)
    headers = _auth_header(7603)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_positive_wins(
        diagnostics_service=_FakeDiagnosticsService(),
    )

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "positive_wins"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["data"]["healthy_habits"] == []
    assert section["data"]["healthy_profiles"] == []
    assert section["data"]["low_risk"] == []

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_refresh_camp_report_positive_wins_preserves_other_sections(
    async_client, fastapi_app, test_db_session
):
    from modules.reports.dependencies import get_reports_service
    from tests.modules.reports.test_reports_routes import _FakeDiagnosticsService

    await _seed_employee(test_db_session, user_id=7604, employee_id=704)
    await _seed_participation_section(test_db_session, report_sections=104)
    await _seed_positive_wins_section(test_db_session, report_sections=105)
    camp_no = await _seed_positive_wins_camp_with_assessments(test_db_session, organization_id=9304)
    headers = _auth_header(7604)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_positive_wins(
        diagnostics_service=_FakeDiagnosticsService(),
    )

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    participation = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert participation.status_code == 200

    positive = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "positive_wins"},
    )
    assert positive.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "participation_by_age" in row.report
    assert "positive_wins" in row.report
    assert row.report["participation_by_age"]["data"]["total_enrolled"] == 3

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


async def _seed_company_average_scores_section(test_db_session, *, report_sections: int = 200):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == "company_average_scores")
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Company Average Scores",
        section_key="company_average_scores",
        description="Average nutrition, fitness, and lifestyle scores across participants",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_company_average_scores_camp(test_db_session, *, organization_id: int = 9401):
    from datetime import datetime, timezone

    from modules.assessments.models import AssessmentInstance, AssessmentPackage
    from modules.engagements.models import Engagement
    from modules.reports.models import IndividualHealthReport

    camp_no, _ = await _seed_camp(
        test_db_session,
        organization_id=organization_id,
        engagement_id=organization_id,
    )
    start = date(2026, 6, 23)
    pkg_id = organization_id + 700

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code=f"FITPRINT{organization_id}",
            display_name=f"FitPrint Package {organization_id}",
            assessment_type_code="7",
            status="active",
        )
    )
    await test_db_session.flush()

    users = [
        (organization_id + 1, "male", "sales"),
        (organization_id + 2, "female", "sales"),
        (organization_id + 3, "male", "engineering"),
    ]
    for user_id, gender, _dept in users:
        test_db_session.add(
            User(
                user_id=user_id,
                age=30,
                phone=f"{user_id}000000000",
                gender=gender,
                status="active",
            )
        )
    await test_db_session.flush()

    for idx, (user_id, _gender, dept) in enumerate(users):
        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=organization_id * 10 + idx + 1,
                engagement_id=organization_id,
                user_id=user_id,
                engagement_date=start,
                slot_start_time=time(10, idx * 20),
                participant_department=dept,
            )
        )
    await test_db_session.flush()

    fitprint_reports = [
        {"fitness_specification": {"score": 60.0}, "activity_specification": {"score": 50.0}},
        {"fitness_specification": {"score": 70.0}, "activity_specification": {"score": 60.0}},
        {"fitness_specification": {"score": 65.0}, "activity_specification": {"score": 55.0}},
    ]

    for idx, (user_id, _gender, _dept) in enumerate(users):
        ai_id = organization_id * 100 + idx + 1
        test_db_session.add(
            AssessmentInstance(
                assessment_instance_id=ai_id,
                user_id=user_id,
                package_id=pkg_id,
                engagement_id=organization_id,
                status="completed",
                metsights_record_id=f"REC{ai_id}",
                assigned_at=datetime(2026, 6, 23, tzinfo=timezone.utc),
                completed_at=datetime(2026, 6, 24, tzinfo=timezone.utc),
            )
        )
        await test_db_session.flush()
        test_db_session.add(
            IndividualHealthReport(
                user_id=user_id,
                engagement_id=organization_id,
                assessment_instance_id=ai_id,
                reports=fitprint_reports[idx],
                blood_parameters=None,
            )
        )
    await test_db_session.commit()
    return camp_no


def _reports_service_for_company_average_scores(*, nutrition_score: float = 64.0):
    from modules.assessments.repository import AssessmentsRepository
    from modules.audit.repository import AuditRepository
    from modules.audit.service import AuditService
    from modules.questionnaire.healthy_habits_service import HealthyHabitsService
    from modules.questionnaire.repository import QuestionnaireRepository
    from modules.reports.repository import ReportsRepository
    from modules.reports.service import ReportsService
    from tests.modules.reports.test_reports_routes import _FakeMetsightsService

    svc = ReportsService(
        repository=ReportsRepository(),
        assessments_repository=AssessmentsRepository(),
        metsights_service=_FakeMetsightsService(payload={}, should_fail=True),
        diagnostics_service=None,
        audit_service=AuditService(AuditRepository()),
        healthy_habits_service=HealthyHabitsService(QuestionnaireRepository()),
        questionnaire_repository=QuestionnaireRepository(),
    )

    async def _mock_call_nutrition_api(db, payload, **kwargs):
        return {"nutrition_score": nutrition_score}

    svc._call_nutrition_api = _mock_call_nutrition_api
    return svc


@pytest.mark.asyncio
async def test_refresh_camp_report_company_average_scores(async_client, fastapi_app, test_db_session):
    from modules.reports.dependencies import get_reports_service

    await _seed_employee(test_db_session, user_id=7801, employee_id=801)
    await _seed_company_average_scores_section(test_db_session, report_sections=200)
    camp_no = await _seed_company_average_scores_camp(test_db_session, organization_id=9401)
    headers = _auth_header(7801)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_company_average_scores(
        nutrition_score=64.0,
    )

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "company_average_scores"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["name"] == "Company Average Scores"
    assert section["description"] == "Average nutrition, fitness, and lifestyle scores across participants"
    assert section["data"]["nutrition"]["score"] == 64
    assert section["data"]["fitness"]["score"] == 55
    assert section["data"]["lifestyle"]["score"] == 65

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "company_average_scores" in row.report
    assert row.report["company_average_scores"]["name"] == "Company Average Scores"
    assert row.report["company_average_scores"]["data"]["nutrition"]["score"] == 64

    bts = row.report_bts["company_average_scores"]
    assert bts["status"] == "ok"
    assert bts["details"]["method"]["section_kind"] == "company_average_scores"
    assert len(bts["details"]["participants"]) == 3
    assert bts["details"]["aggregation"]["nutrition"]["rounded_score"] == 64
    assert bts["fields"]["nutrition.score"]["match"] is True

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_refresh_department_camp_report_company_average_scores(async_client, fastapi_app, test_db_session):
    from modules.reports.dependencies import get_reports_service

    await _seed_employee(test_db_session, user_id=7802, employee_id=802)
    await _seed_company_average_scores_section(test_db_session, report_sections=201)
    camp_no = await _seed_company_average_scores_camp(test_db_session, organization_id=9402)
    headers = _auth_header(7802)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_company_average_scores(
        nutrition_score=70.0,
    )

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "company_average_scores"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["data"]["nutrition"]["score"] == 70
    assert section["data"]["lifestyle"]["score"] == 65
    assert section["data"]["fitness"]["score"] == 55

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_refresh_camp_report_company_average_scores_empty_camp(async_client, fastapi_app, test_db_session):
    from modules.reports.dependencies import get_reports_service

    await _seed_employee(test_db_session, user_id=7803, employee_id=803)
    await _seed_company_average_scores_section(test_db_session, report_sections=202)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=9403, engagement_id=9403)
    headers = _auth_header(7803)

    fastapi_app.dependency_overrides[get_reports_service] = lambda: _reports_service_for_company_average_scores()

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "company_average_scores"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["data"]["nutrition"]["score"] == 0
    assert section["data"]["fitness"]["score"] == 0
    assert section["data"]["lifestyle"]["score"] == 0

    fastapi_app.dependency_overrides.pop(get_reports_service, None)


@pytest.mark.asyncio
async def test_estimate_camp_report_requires_auth(async_client):
    response = await async_client.post(
        "/reports/camps/123/estimate",
        json={"operations": [{"section": "kpis", "action": "refresh"}]},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_estimate_camp_report_fast_section_under_timeout(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7901, employee_id=901)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9501,
        engagement_id=9501,
    )
    headers = _auth_header(7901)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={"operations": [{"section": "kpis", "action": "refresh", "department": None}]},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["timeout_seconds"] == 120
    assert payload["all_allowed"] is True
    assert len(payload["operations"]) == 1
    op = payload["operations"][0]
    assert op["section"] == "kpis"
    assert op["action"] == "refresh"
    assert op["department"] is None
    assert op["participant_count"] == 4
    assert op["unit_count"] == 0  # no Metsights checks needed (no record ids without booking)
    assert op["estimated_seconds"] == 3  # ceil(3 + 0.35*0)
    assert op["allowed"] is True
    assert payload["total_estimated_seconds"] == 3


@pytest.mark.asyncio
async def test_estimate_validate_matches_refresh_cost(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7910, employee_id=910)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9510,
        engagement_id=9510,
    )
    headers = _auth_header(7910)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={
            "operations": [
                {"section": "kpis", "action": "refresh"},
                {"section": "kpis", "action": "validate"},
            ]
        },
    )
    assert response.status_code == 200
    ops = response.json()["data"]["operations"]
    assert ops[0]["estimated_seconds"] == ops[1]["estimated_seconds"]
    assert ops[0]["unit_count"] == ops[1]["unit_count"]


@pytest.mark.asyncio
async def test_validate_routes_removed(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7911, employee_id=911)
    headers = _auth_header(7911)
    response = await async_client.get(
        "/reports/camps/123/validate/company-average-scores",
        headers=headers,
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_refresh_participation_by_age_writes_real_bts(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7912, employee_id=912)
    await _seed_participation_section(test_db_session, report_sections=95120)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9512,
        engagement_id=9512,
    )
    headers = _auth_header(7912)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "participation_by_age"},
    )
    assert response.status_code == 200
    bts = response.json()["data"]["report_bts"]
    assert bts["status"] == "ok"
    assert "age_groups" in bts["details"]
    assert "method" in bts["details"]
    assert bts["details"]["method"]["distinct_people"] == bts["expected"]["total_enrolled"]
    assert set(bts["details"]["age_groups"].keys()) == {
        "18–25",
        "26–35",
        "36–45",
        "46–55",
        "55+",
    }

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report_bts["participation_by_age"]["status"] == "ok"
    assert "age_groups" in row.report_bts["participation_by_age"]["details"]


@pytest.mark.asyncio
async def test_refresh_overall_risk_score_writes_bts(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7913, employee_id=913)
    await _seed_overall_risk_score_section(test_db_session, report_sections=95130)
    camp_no = await _seed_overall_risk_score_camp_data(
        test_db_session,
        organization_id=9513,
        engagement_id=9513,
    )
    headers = _auth_header(7913)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "overall_risk_score"},
    )
    assert response.status_code == 200
    bts = response.json()["data"]["report_bts"]
    assert bts["status"] == "ok"
    assert bts["expected"]["elevated_metabolic_score"] == 50.0
    assert "elevated_math" in bts["details"]
    assert "bands" in bts["details"]
    assert set(bts["details"]["bands"].keys()) == {
        "optimal",
        "low_risk",
        "increased_risk",
        "high_risk",
    }
    assert bts["details"]["elevated_math"]["result_percent"] == 50.0
    assert len(bts["details"]["elevated_math"]["steps"]) >= 5

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report_bts["overall_risk_score"]["status"] == "ok"
    assert "excluded" in row.report_bts["overall_risk_score"]["details"]


@pytest.mark.asyncio
async def test_estimate_camp_report_department_scope(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7902, employee_id=902)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9502,
        engagement_id=9502,
    )
    headers = _auth_header(7902)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={"operations": [{"section": "kpis", "action": "refresh", "department": "sales"}]},
    )
    assert response.status_code == 200
    op = response.json()["data"]["operations"][0]
    assert op["department"] == "sales"
    assert op["city"] is None
    assert op["participant_count"] == 3
    assert op["unit_count"] == 0  # kpi_metsights: no record ids needing fetch-collections


@pytest.mark.asyncio
async def test_estimate_camp_report_city_scope(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7908, employee_id=908)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9508,
        engagement_id=9508,
    )
    headers = _auth_header(7908)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={
            "operations": [
                {
                    "section": "participation_by_age",
                    "action": "refresh",
                    "city": "BLR",
                }
            ]
        },
    )
    assert response.status_code == 200
    op = response.json()["data"]["operations"][0]
    assert op["city"] == "BLR"
    assert op["department"] is None
    assert op["participant_count"] == 4


@pytest.mark.asyncio
async def test_estimate_camp_report_blocks_when_over_timeout(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7903, employee_id=903)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9503,
        engagement_id=9503,
    )
    headers = _auth_header(7903)
    monkeypatch.setattr(settings, "CAMP_REPORT_CLIENT_TIMEOUT_SECONDS", 2)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={"operations": [{"section": "kpis", "action": "refresh"}]},
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["timeout_seconds"] == 2
    op = payload["operations"][0]
    assert op["estimated_seconds"] == 3
    assert op["allowed"] is False
    assert payload["all_allowed"] is False


@pytest.mark.asyncio
async def test_estimate_camp_report_bulk_all_allowed_false(
    async_client, test_db_session, monkeypatch
):
    await _seed_employee(test_db_session, user_id=7904, employee_id=904)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9504,
        engagement_id=9504,
    )
    headers = _auth_header(7904)
    # company_average_scores with 0 fitprint contexts => ceil(5)=5; timeout 4 blocks it
    monkeypatch.setattr(settings, "CAMP_REPORT_CLIENT_TIMEOUT_SECONDS", 4)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={
            "operations": [
                {"section": "kpis", "action": "refresh"},
                {"section": "company_average_scores", "action": "validate"},
            ]
        },
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert len(payload["operations"]) == 2
    assert payload["operations"][0]["section"] == "kpis"
    assert payload["operations"][0]["allowed"] is True
    assert payload["operations"][1]["section"] == "company_average_scores"
    assert payload["operations"][1]["action"] == "validate"
    assert payload["operations"][1]["estimated_seconds"] == 5
    assert payload["operations"][1]["allowed"] is False
    assert payload["all_allowed"] is False
    assert payload["total_estimated_seconds"] == (
        payload["operations"][0]["estimated_seconds"] + payload["operations"][1]["estimated_seconds"]
    )


@pytest.mark.asyncio
async def test_estimate_positive_wins_uses_health_assessment_count(
    async_client, test_db_session, monkeypatch
):
    """positive_wins scales with health assessments, not FitPrint (which may be 0)."""
    await _seed_employee(test_db_session, user_id=7906, employee_id=906)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9506,
        engagement_id=9506,
    )
    headers = _auth_header(7906)

    from modules.reports.camp_reports_repository import CampReportsRepository

    async def fake_health_count(self, db, *, camp_no, department=None, city=None):
        return 200

    monkeypatch.setattr(
        CampReportsRepository,
        "count_health_assessment_contexts",
        fake_health_count,
    )

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={"operations": [{"section": "positive_wins", "action": "refresh"}]},
    )
    assert response.status_code == 200
    op = response.json()["data"]["operations"][0]
    assert op["section"] == "positive_wins"
    assert op["unit_count"] == 200
    # ceil(5 + 0.08*200) = ceil(21) = 21 — would be 5s if FitPrint(0) were used
    assert op["estimated_seconds"] == 21
    assert op["allowed"] is True


@pytest.mark.asyncio
async def test_estimate_camp_report_invalid_action(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7905, employee_id=905)
    camp_no = await _seed_refresh_camp_with_participants(
        test_db_session,
        organization_id=9505,
        engagement_id=9505,
    )
    headers = _auth_header(7905)

    response = await async_client.post(
        f"/reports/camps/{camp_no}/estimate",
        headers=headers,
        json={"operations": [{"section": "kpis", "action": "delete"}]},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"

