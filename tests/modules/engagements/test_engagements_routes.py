"""Integration tests for engagements routes."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    test_db_session.add(User(user_id=user_id, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active"))
    await test_db_session.commit()


async def _seed_organization(test_db_session, *, organization_id: int, name: str = "Test Org"):
    from modules.organizations.models import Organization
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name=name,
            organization_type="corporate",
            status="active",
        )
    )
    await test_db_session.commit()


async def _seed_assessment_package(test_db_session, *, package_id: int, package_code: str = "PKG1"):
    from modules.assessments.models import AssessmentPackage
    test_db_session.add(
        AssessmentPackage(
            package_id=package_id,
            package_code=package_code,
            display_name=f"Test Package {package_id}",
            status="active",
        )
    )
    await test_db_session.commit()


async def _seed_diagnostic_package(test_db_session, *, diagnostic_package_id: int):
    from modules.diagnostics.models import DiagnosticPackage
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=diagnostic_package_id,
            package_name=f"Test Diagnostic Package {diagnostic_package_id}",
            diagnostic_provider="test_provider",
            status="active",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_engagement_requires_auth(async_client):
    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "healthcamp",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-01",
    }

    response = await async_client.post("/engagements", json=payload)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_engagement_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=7001, phone="7001000000", status="active"))
    await test_db_session.commit()

    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "healthcamp",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-01",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7001), json=payload)
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_engagement_creates_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7002, employee_id=10)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    payload = {
        "engagement_name": "Camp",
        "organization_id": 1,
        "engagement_type": "healthcamp",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
    }

    response = await async_client.post("/engagements", headers=_auth_header(7002), json=payload)
    assert response.status_code == 201

    engagement_id = response.json()["data"]["engagement_id"]
    assert isinstance(engagement_id, int)


@pytest.mark.asyncio
async def test_list_engagements_paginates_and_filters(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7003, employee_id=11)
    await _seed_organization(test_db_session, organization_id=1, name="Org 1")
    await _seed_organization(test_db_session, organization_id=2, name="Org 2")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    # Create engagements
    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8101,
            engagement_name="E1",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE1",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=8102,
            engagement_name="E2",
            metsights_engagement_id=None,
            organization_id=2,
            engagement_code="CODE2",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="DEL",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 3),
            status="inactive",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements?page=1&limit=10&org_id=1&status=active&city=BLR&date=2026-02-01",
        headers=_auth_header(7003),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    assert len(body["data"]) == 1
    assert body["data"][0]["engagement_id"] == 8101
    assert body["data"][0]["assessment_package_id"] == 1


@pytest.mark.asyncio
async def test_get_engagement_details_returns_row(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7004, employee_id=12)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8201,
            engagement_name="E1",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE3",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/8201", headers=_auth_header(7004))
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["engagement_id"] == 8201
    assert body["assessment_package_id"] == 1


@pytest.mark.asyncio
async def test_update_engagement_updates_fields(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7005, employee_id=13)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8301,
            engagement_name="Old",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE4",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "New",
        "organization_id": 1,
        "engagement_type": "healthcamp",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "Pune",
        "slot_duration": 30,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
        "metsights_engagement_id": "MS1",
    }

    response = await async_client.put("/engagements/8301", headers=_auth_header(7005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(Engagement, 8301)
    assert updated is not None
    assert updated.engagement_name == "New"
    assert updated.city == "Pune"
    assert updated.slot_duration == 30
    assert updated.metsights_engagement_id == "MS1"


@pytest.mark.asyncio
async def test_get_occupied_slots_by_engagement_code_is_public(async_client):
    # No auth header required
    response = await async_client.get("/engagements/code/ENG1/occupied-slots")
    assert response.status_code in (200, 404)


@pytest.mark.asyncio
async def test_get_occupied_slots_by_engagement_code_returns_grouped_slots(async_client, test_db_session):
    from modules.engagements.models import Engagement, EngagementTimeSlot
    from modules.users.models import User
    from datetime import time

    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    # Create users for time slots
    test_db_session.add(User(user_id=1001, phone="1001000000", status="active"))
    test_db_session.add(User(user_id=1002, phone="1002000000", status="active"))
    test_db_session.add(User(user_id=1003, phone="1003000000", status="active"))
    await test_db_session.flush()

    test_db_session.add(
        Engagement(
            engagement_id=9101,
            engagement_name="E",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="ENG9101",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 2),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    test_db_session.add_all(
        [
            EngagementTimeSlot(
                time_slot_id=1,
                engagement_id=9101,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(10, 0),
            ),
            EngagementTimeSlot(
                time_slot_id=2,
                engagement_id=9101,
                user_id=1002,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(10, 20),
            ),
            EngagementTimeSlot(
                time_slot_id=3,
                engagement_id=9101,
                user_id=1003,
                engagement_date=date(2026, 2, 2),
                slot_start_time=time(11, 0),
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/code/ENG9101/occupied-slots",
    )

    assert response.status_code == 200
    occupied = response.json()["data"]["occupied_slots"]
    assert occupied == {
        "2026-02-01": ["10:00:00", "10:20:00"],
        "2026-02-02": ["11:00:00"],
    }


@pytest.mark.asyncio
async def test_get_public_occupied_slots_is_public(async_client, test_db_session):
    # No auth header required
    response = await async_client.get("/engagements/public/occupied-slots")
    assert response.status_code == 200
    assert response.json()["data"]["occupied_slots"] == {}


@pytest.mark.asyncio
async def test_get_public_occupied_slots_returns_only_active_b2c(async_client, test_db_session):
    from modules.engagements.models import Engagement, EngagementTimeSlot
    from modules.users.models import User
    from datetime import time

    await _seed_organization(test_db_session, organization_id=99, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    # Create user for time slots
    test_db_session.add(User(user_id=1001, phone="1001000000", status="active"))
    await test_db_session.flush()

    # Active B2C (organization_id is None)
    test_db_session.add(
        Engagement(
            engagement_id=9201,
            engagement_name="B2C1",
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code="B2C1",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
        )
    )

    # Active B2B (must be excluded)
    test_db_session.add(
        Engagement(
            engagement_id=9202,
            engagement_name="B2B1",
            metsights_engagement_id=None,
            organization_id=99,
            engagement_code="B2B1",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
        )
    )

    # Inactive B2C (must be excluded)
    test_db_session.add(
        Engagement(
            engagement_id=9203,
            engagement_name="B2C2",
            metsights_engagement_id=None,
            organization_id=None,
            engagement_code="B2C2",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="inactive",
            participant_count=0,
        )
    )

    await test_db_session.commit()

    test_db_session.add_all(
        [
            EngagementTimeSlot(
                time_slot_id=10,
                engagement_id=9201,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(9, 0),
            ),
            EngagementTimeSlot(
                time_slot_id=11,
                engagement_id=9202,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(9, 20),
            ),
            EngagementTimeSlot(
                time_slot_id=12,
                engagement_id=9203,
                user_id=1001,
                engagement_date=date(2026, 2, 1),
                slot_start_time=time(9, 40),
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/public/occupied-slots",
    )

    assert response.status_code == 200
    occupied = response.json()["data"]["occupied_slots"]
    assert occupied == {"2026-02-01": ["09:00:00"]}


@pytest.mark.asyncio
async def test_patch_engagement_status_changes_status(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=7006, employee_id=14)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)

    from modules.engagements.models import Engagement

    test_db_session.add(
        Engagement(
            engagement_id=8401,
            engagement_name="E",
            metsights_engagement_id=None,
            organization_id=1,
            engagement_code="CODE5",
            engagement_type="healthcamp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 2, 1),
            end_date=date(2026, 2, 1),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.patch(
        "/engagements/8401/status",
        headers=_auth_header(7006),
        json={"status": "inactive"},
    )
    assert response.status_code == 200

    updated = await test_db_session.get(Engagement, 8401)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"
