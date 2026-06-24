"""Integration tests for GET /organizations/camps."""

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


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_list_camps_requires_auth(async_client):
    response = await async_client.get("/organizations/camps")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_camps_aggregates_engagements(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7201, employee_id=31)

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

    for engagement_id, code in ((8201, "CAMP1"), (8202, "CAMP2")):
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name=f"Engagement {engagement_id}",
                organization_id=8001,
                camp_no=camp_no,
                engagement_code=code,
                engagement_type="bio_ai",
                assessment_package_id=None,
                diagnostic_package_id=None,
                city="BLR",
                slot_duration=20,
                start_date=start,
                end_date=start,
                status="running",
                participant_count=0,
            )
        )

    test_db_session.add(
        Engagement(
            engagement_id=8203,
            engagement_name="B2C Engagement",
            organization_id=None,
            camp_no=None,
            engagement_code="B2CCAMP",
            engagement_type="bio_ai",
            assessment_package_id=None,
            diagnostic_package_id=None,
            city="BLR",
            slot_duration=20,
            start_date=start,
            end_date=start,
            status="running",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/camps?page=1&limit=10", headers=_auth_header(7201))
    assert response.status_code == 200

    body = response.json()
    camps = body["data"]
    matching = [row for row in camps if row["camp_no"] == camp_no]
    assert len(matching) == 1
    assert matching[0]["engagement_count"] == 2
    assert matching[0]["camp_name"] == "Camp Org 23 June 2026"
    assert matching[0]["organization_id"] == 8001
    assert matching[0]["department_count"] == 2
    assert matching[0]["report_count"] == 0
