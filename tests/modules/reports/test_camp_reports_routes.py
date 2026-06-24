"""Integration tests for camp reports routes."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.reports.models import CampReport
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
