"""Integration tests for multi-city camp ranking section refresh."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.employee.models import Employee
from modules.engagements.camp_no import compute_camp_no
from modules.engagements.models import Engagement, EngagementParticipant
from modules.organizations.models import Industry, Organization
from modules.reports.models import CampReport, CampReportSection, IndividualHealthReport
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_ranking_section(test_db_session, *, report_sections: int = 9601):
    existing = (
        await test_db_session.execute(
            select(CampReportSection).where(CampReportSection.section_key == "ranking")
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    row = CampReportSection(
        report_sections=report_sections,
        section="Ranking",
        section_key="ranking",
        description="Camp health rank by city based on average metabolic risk score.",
    )
    test_db_session.add(row)
    await test_db_session.commit()
    return row


async def _seed_industries(test_db_session):
    existing = (
        await test_db_session.execute(select(Industry).where(Industry.industry_key == "manufacturing"))
    ).scalar_one_or_none()
    if existing is None:
        test_db_session.add_all(
            [
                Industry(industry_key="manufacturing", industry="Manufacturing"),
                Industry(industry_key="information_technology", industry="Information Technology"),
            ]
        )
        await test_db_session.commit()


async def _ensure_bio_ai_package(test_db_session, *, package_id: int = 9601):
    existing = (
        await test_db_session.execute(
            select(AssessmentPackage).where(AssessmentPackage.package_id == package_id)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    pkg = AssessmentPackage(
        package_id=package_id,
        package_code="RANKPKG1",
        display_name="Bio AI Package",
        assessment_type_code="1",
        status="active",
    )
    test_db_session.add(pkg)
    await test_db_session.flush()
    return pkg


async def _seed_org_camp_with_city_scores(
    test_db_session,
    *,
    organization_id: int,
    org_name: str,
    industry_key: str,
    cities: list[tuple[str, float]],
    start: date,
    id_base: int,
    package_id: int = 9601,
) -> int:
    """Create one org/camp with one engagement+user+score per city. Returns camp_no."""
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name=org_name,
            organization_type="corporate",
            status="active",
            industry_key=industry_key,
            departments=[{"department": "Sales", "slug": "sales"}],
        )
    )
    await test_db_session.flush()

    camp_no = compute_camp_no(organization_id, start)
    assert camp_no is not None

    for offset, (city, score) in enumerate(cities):
        engagement_id = id_base + offset
        user_id = id_base + offset
        participant_id = id_base + offset
        assessment_id = id_base + offset
        report_id = id_base + offset

        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"{org_name} {city}",
                organization_id=organization_id,
                camp_no=camp_no,
                engagement_code=f"R{engagement_id}",
                engagement_type="bio_ai",
                assessment_package_id=package_id,
                diagnostic_package_id=None,
                city=city,
                slot_duration=20,
                start_date=start,
                end_date=start,
                status="running",
            )
        )
        await test_db_session.flush()

        test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}111111111", status="active"))
        await test_db_session.flush()

        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=participant_id,
                engagement_id=engagement_id,
                user_id=user_id,
                engagement_date=start,
                slot_start_time=time(10, 0),
                participant_department="sales",
            )
        )
        test_db_session.add(
            AssessmentInstance(
                assessment_instance_id=assessment_id,
                user_id=user_id,
                engagement_id=engagement_id,
                package_id=package_id,
                status="completed",
            )
        )
        await test_db_session.flush()

        test_db_session.add(
            IndividualHealthReport(
                report_id=report_id,
                user_id=user_id,
                assessment_instance_id=assessment_id,
                engagement_id=engagement_id,
                reports={"metabolic_score": score},
            )
        )

    await test_db_session.commit()
    return camp_no


@pytest.mark.asyncio
async def test_refresh_ranking_multi_city_peer_and_industry_ranks(async_client, test_db_session):
    """Subject camp in Bangalore+Pune ranks among city peers; industry filter applies."""
    await _seed_employee(test_db_session, user_id=9600, employee_id=9600)
    ranking_section = await _seed_ranking_section(test_db_session)
    await _seed_industries(test_db_session)
    await _ensure_bio_ai_package(test_db_session)

    start = date(2026, 6, 23)

    # Subject: Bangalore avg 40, Pune avg 10 (city-scoped — not blended to 25)
    subject_camp_no = await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9610,
        org_name="Subject Corp",
        industry_key="manufacturing",
        cities=[("Bangalore", 40.0), ("Pune", 10.0)],
        start=start,
        id_base=96100,
    )

    # Bangalore manufacturing peer with better (lower) score → city+industry rank 1
    await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9611,
        org_name="Peer Better BLR",
        industry_key="manufacturing",
        cities=[("Bangalore", 30.0)],
        start=start,
        id_base=96110,
    )

    # Bangalore IT peer with worse score — in city ranking but not industry
    await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9612,
        org_name="Peer IT BLR",
        industry_key="information_technology",
        cities=[("Bangalore", 50.0)],
        start=start,
        id_base=96120,
    )

    # Bangalore manufacturing peer at 32 — proves city-scoped avg (40) not whole-camp (25)
    await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9613,
        org_name="Peer Mid BLR",
        industry_key="manufacturing",
        cities=[("Bangalore", 32.0)],
        start=start,
        id_base=96130,
    )

    # Pune manufacturing peer with better score
    await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9614,
        org_name="Peer Better Pune",
        industry_key="manufacturing",
        cities=[("Pune", 5.0)],
        start=start,
        id_base=96140,
    )

    headers = _auth_header(9600)

    init = await async_client.post(f"/reports/camps/{subject_camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    response = await async_client.put(
        f"/reports/camps/{subject_camp_no}/refresh",
        headers=headers,
        json={"section": "ranking"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["name"] == ranking_section.section
    assert section["description"] == ranking_section.description
    data = section["data"]

    # Bangalore peers by avg: 30, 32, 40(subject), 50 → rank 3
    # Manufacturing among those: 30, 32, 40 → industry_rank 3
    assert data["Bangalore"]["rank"] == 3
    assert data["Bangalore"]["total_camps"] == 4
    assert data["Bangalore"]["industry_rank"] == 3
    assert data["Bangalore"]["total_industry_camps"] == 3

    # If scores were wrongly blended (25), Bangalore rank would be 1 (25 < 30 < 32 < 50)
    assert data["Bangalore"]["rank"] != 1

    # Pune peers: 5, 10(subject) → rank 2; same industry → industry_rank 2
    assert data["Pune"]["rank"] == 2
    assert data["Pune"]["total_camps"] == 2
    assert data["Pune"]["industry_rank"] == 2
    assert data["Pune"]["total_industry_camps"] == 2

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    stored = row.report["ranking"]
    assert stored["name"] == ranking_section.section
    assert stored["description"] == ranking_section.description
    assert stored["data"]["Bangalore"]["rank"] == 3
    assert stored["data"]["Bangalore"]["total_camps"] == 4
    assert stored["data"]["Pune"]["rank"] == 2
    assert stored["data"]["Pune"]["total_camps"] == 2


@pytest.mark.asyncio
async def test_refresh_ranking_updates_existing_section(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9620, employee_id=9620)
    ranking_section = await _seed_ranking_section(test_db_session, report_sections=9620)
    await _seed_industries(test_db_session)
    await _ensure_bio_ai_package(test_db_session)

    start = date(2026, 7, 1)
    camp_no = await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9621,
        org_name="Update Ranking Org",
        industry_key="manufacturing",
        cities=[("Mumbai", 22.0)],
        start=start,
        id_base=96210,
    )
    headers = _auth_header(9620)

    init = await async_client.post(f"/reports/camps/{camp_no}/init", headers=headers)
    assert init.status_code == 201
    report_id = init.json()["data"]["report_id"]

    # Pre-seed old flat ranking shape
    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    report = dict(row.report or {})
    report["ranking"] = {
        "data": {"city": "Mumbai", "rank_city": 9},
        "name": "Old",
        "description": "old",
    }
    report["kpis"] = {"data": {"keep": True}, "name": "KPIs", "description": "keep me"}
    row.report = report
    await test_db_session.commit()

    response = await async_client.put(
        f"/reports/camps/{camp_no}/refresh",
        headers=headers,
        json={"section": "ranking"},
    )
    assert response.status_code == 200

    row = (
        await test_db_session.execute(select(CampReport).where(CampReport.report_id == report_id))
    ).scalar_one()
    assert row.report["kpis"]["data"]["keep"] is True
    ranking = row.report["ranking"]
    assert ranking["name"] == ranking_section.section
    assert ranking["description"] == ranking_section.description
    assert "Mumbai" in ranking["data"]
    assert ranking["data"]["Mumbai"]["rank"] == 1
    assert ranking["data"]["Mumbai"]["total_camps"] == 1
    assert ranking["data"]["Mumbai"]["industry_rank"] == 1
    assert ranking["data"]["Mumbai"]["total_industry_camps"] == 1
    assert "rank_city" not in ranking["data"]


@pytest.mark.asyncio
async def test_refresh_department_ranking_stores_same_multi_city_shape(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9630, employee_id=9630)
    await _seed_ranking_section(test_db_session, report_sections=9630)
    await _seed_industries(test_db_session)
    await _ensure_bio_ai_package(test_db_session)

    start = date(2026, 8, 10)
    camp_no = await _seed_org_camp_with_city_scores(
        test_db_session,
        organization_id=9631,
        org_name="Dept Ranking Org",
        industry_key="manufacturing",
        cities=[("Chennai", 18.0)],
        start=start,
        id_base=96310,
    )
    headers = _auth_header(9630)

    init = await async_client.post(
        f"/reports/camps/{camp_no}/department/sales/init",
        headers=headers,
    )
    assert init.status_code == 201

    response = await async_client.put(
        f"/reports/camps/{camp_no}/department/sales/refresh",
        headers=headers,
        json={"section": "ranking"},
    )
    assert response.status_code == 200
    section = response.json()["data"]["section"]
    assert section["name"] == "Ranking"
    assert section["data"]["Chennai"]["rank"] == 1
    assert section["data"]["Chennai"]["total_camps"] == 1
    assert section["data"]["Chennai"]["industry_rank"] == 1
    assert section["data"]["Chennai"]["total_industry_camps"] == 1
