"""Tests for camp report section enrichment APIs."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import select

from modules.employee.models import Employee
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement, EngagementType
from modules.organizations.models import Organization
from modules.reports.models import CampReport
from modules.users.models import User
from tests.modules.reports.test_camp_reports_routes import _auth_header

async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _bio_ai_type_id(test_db_session) -> int | None:
    return (
        await test_db_session.execute(
            select(EngagementType.id).where(EngagementType.code == "bio_ai").limit(1)
        )
    ).scalar_one_or_none()


async def _seed_camp(test_db_session, *, organization_id: int, engagement_id: int):
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
            engagement_code=f"ENR{engagement_id}",
            engagement_type=await _bio_ai_type_id(test_db_session),
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

_SLEEP_SECTION = {
    "data": {
        "labels": ["<6 hrs", "6–7 hrs", "7–8 hrs", "8+ hrs"],
        "both": [10, 20, 30, 14],
        "male": [8, 16, 24, 12],
        "female": [2, 4, 6, 2],
    },
    "name": "Sleep",
    "description": None,
}

_SAMPLE_REPORT = Path(
    "modules/reports/camp_report_intelligence/sample_camp_report.json"
)


def _fake_intelligence(report: dict, section: str):
    name = (report.get("meta") or {}).get("camp_name") or "unknown"
    return (
        "distribution_by_sleeping_hours",
        {
            "tone": "concern",
            "observation": name,
            "explanation": f"scoped:{section}",
            "recommendation": "Act.",
        },
    )


async def _attach_sleep_section(test_db_session, *, report_id: int) -> None:
    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    report = dict(row.report or {})
    report["distribution_by_sleeping_hours"] = dict(_SLEEP_SECTION)
    row.report = report
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_enrich_requires_auth(async_client):
    response = await async_client.put(
        "/reports/camps/123/enrich",
        json={"section": "sleep"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_enrich_invalid_section(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8401, employee_id=401)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8401, engagement_id=8401)
    headers = _auth_header(8401)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/enrich",
        headers=headers,
        json={"section": "does_not_exist"},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_SECTION"


@pytest.mark.asyncio
async def test_enrich_camp_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8402, employee_id=402)
    headers = _auth_header(8402)
    response = await async_client.put(
        "/reports/camps/999999999/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_NOT_FOUND"


@pytest.mark.asyncio
async def test_enrich_camp_report_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8403, employee_id=403)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8403, engagement_id=8403)
    headers = _auth_header(8403)
    response = await async_client.put(
        f"/reports/camps/{camp_no}/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_REPORT_NOT_FOUND"


@pytest.mark.asyncio
async def test_enrich_department_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8404, employee_id=404)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8404, engagement_id=8404)
    headers = _auth_header(8404)
    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/unknown/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "DEPARTMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_enrich_city_not_found(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8405, employee_id=405)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8405, engagement_id=8405)
    headers = _auth_header(8405)
    await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    response = await async_client.put(
        f"/reports/camps/{camp_no}/DEL/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CITY_NOT_FOUND"


@pytest.mark.asyncio
async def test_enrich_invalid_city_department_combination(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8406, employee_id=406)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8406, engagement_id=8406)
    headers = _auth_header(8406)
    await async_client.post(f"/reports/camps/{camp_no}/BLR/init", headers=headers)
    response = await async_client.put(
        f"/reports/camps/{camp_no}/BLR/department/sales/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "CAMP_REPORT_NOT_FOUND"


@pytest.mark.asyncio
async def test_enrich_four_scopes_use_correct_report(
    async_client,
    test_db_session,
    monkeypatch,
):
    monkeypatch.setattr(
        "modules.reports.camp_reports_service.generate_camp_section_intelligence",
        _fake_intelligence,
    )
    await _seed_employee(test_db_session, user_id=8407, employee_id=407)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8407, engagement_id=8407)
    headers = _auth_header(8407)

    camp_init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    dept_init = await async_client.post(
        f"/reports/camps/{camp_no}/department/engineering/init",
        headers=headers,
    )
    city_init = await async_client.post(f"/reports/camps/{camp_no}/BLR/init", headers=headers)
    city_dept_init = await async_client.post(
        f"/reports/camps/{camp_no}/BLR/department/engineering/init",
        headers=headers,
    )
    assert {camp_init.status_code, dept_init.status_code, city_init.status_code, city_dept_init.status_code} == {201}

    await _attach_sleep_section(test_db_session, report_id=camp_init.json()["data"]["report_id"])
    await _attach_sleep_section(test_db_session, report_id=dept_init.json()["data"]["report_id"])
    await _attach_sleep_section(test_db_session, report_id=city_init.json()["data"]["report_id"])
    await _attach_sleep_section(test_db_session, report_id=city_dept_init.json()["data"]["report_id"])

    camp = await async_client.put(
        f"/reports/camps/{camp_no}/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    dept = await async_client.put(
        f"/reports/camps/{camp_no}/department/engineering/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    city = await async_client.put(
        f"/reports/camps/{camp_no}/BLR/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    city_dept = await async_client.put(
        f"/reports/camps/{camp_no}/BLR/department/engineering/enrich",
        headers=headers,
        json={"section": "sleep"},
    )

    assert camp.status_code == 200
    assert dept.status_code == 200
    assert city.status_code == 200
    assert city_dept.status_code == 200

    camp_body = camp.json()["data"]
    dept_body = dept.json()["data"]
    city_body = city.json()["data"]
    city_dept_body = city_dept.json()["data"]

    assert camp_body["scope"] == {"type": "camp", "city": None, "department": None}
    assert dept_body["scope"] == {"type": "department", "city": None, "department": "engineering"}
    assert city_body["scope"] == {"type": "city", "city": "BLR", "department": None}
    assert city_dept_body["scope"] == {
        "type": "city_department",
        "city": "BLR",
        "department": "engineering",
    }

    assert camp_body["section"] == "sleep"
    assert "engineering" in dept_body["data"]["observation"]
    assert camp_body["data"]["observation"] != dept_body["data"]["observation"]
    assert city_body["data"]["observation"] != camp_body["data"]["observation"]
    assert "engineering" in city_dept_body["data"]["observation"]
    assert "BLR" in city_body["data"]["observation"] or "blr" in city_body["data"]["observation"].lower()


@pytest.mark.asyncio
async def test_enrich_sample_camp_report_sleep_section(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=8408, employee_id=408)
    camp_no, _ = await _seed_camp(test_db_session, organization_id=8408, engagement_id=8408)
    headers = _auth_header(8408)
    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    sample = json.loads(_SAMPLE_REPORT.read_text())
    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    row.report = sample
    await test_db_session.commit()

    response = await async_client.put(
        f"/reports/camps/{camp_no}/enrich",
        headers=headers,
        json={"section": "sleep"},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["section"] == "sleep"
    assert body["scope"]["type"] == "camp"
    intel = body["data"]
    assert "both" in intel
    assert {"tone", "observation", "explanation", "recommendation"} <= set(intel["both"].keys())
    stored = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert "intelligence" in stored.report["distribution_by_sleeping_hours"]
    assert "intelligence" not in (stored.report.get("overall_risk_score") or {})
