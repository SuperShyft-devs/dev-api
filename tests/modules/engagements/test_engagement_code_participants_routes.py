"""Integration tests for engagement code participants endpoint (employee-only)."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.diagnostics.models import DiagnosticPackage
from modules.users.models import User
from modules.engagements.models import Engagement, EngagementTimeSlot
from modules.assessments.models import AssessmentPackage


def _auth_header(user_id: int) -> dict[str, str]:
    """Create authentication header for testing."""
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    """Seed a test employee."""
    # Seed required diagnostic package for engagements (b2b requires non-null FK).
    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package",
            diagnostic_provider="test_provider",
            status="active",
            bookings_count=0,
        )
    )
    user = User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    
    employee = Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    test_db_session.add(employee)
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_get_engagement_code_participants_requires_auth(async_client):
    """Test that the endpoint requires authentication."""
    response = await async_client.get("/engagements/code/ENG001/participants")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_engagement_code_participants_requires_employee(async_client, test_db_session):
    """Test that the endpoint requires employee role."""
    test_db_session.add(User(user_id=7001, age=30, phone="7001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/engagements/code/ENG001/participants", headers=_auth_header(7001))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_engagement_code_participants_returns_404_for_nonexistent_engagement(async_client, test_db_session):
    """Test that the endpoint returns 404 for non-existent engagement."""
    await _seed_employee(test_db_session, user_id=7002, employee_id=201)

    response = await async_client.get("/engagements/code/NONEXISTENT/participants", headers=_auth_header(7002))
    assert response.status_code == 404
    assert response.json()["error_code"] == "ENGAGEMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_get_engagement_code_participants_returns_empty_list_when_no_participants(async_client, test_db_session):
    """Test that the endpoint returns empty list when engagement has no participants."""
    await _seed_employee(test_db_session, user_id=7003, employee_id=202)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=101,
            package_code="PKG101",
            display_name="Test Package 101",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create engagement with no participants
    test_db_session.add(
        Engagement(
            engagement_id=3001,
            engagement_code="ENG3001",
            engagement_type="b2b",
            assessment_package_id=101,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/code/ENG3001/participants", headers=_auth_header(7003))
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["meta"]["total"] == 0
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 20


@pytest.mark.asyncio
async def test_get_engagement_code_participants_returns_participants_from_engagement(
    async_client, test_db_session
):
    """Test that the endpoint returns participants from a specific engagement."""
    await _seed_employee(test_db_session, user_id=7004, employee_id=203)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=102,
            package_code="PKG102",
            display_name="Test Package 102",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create engagement
    test_db_session.add(
        Engagement(
            engagement_id=3002,
            engagement_code="ENG3002",
            engagement_type="b2b",
            assessment_package_id=102,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=2,
        )
    )
    await test_db_session.flush()

    # Create participants
    test_db_session.add(
        User(
            user_id=8001,
            age=30,
            first_name="Alice",
            last_name="Smith",
            phone="8001111111",
            email="alice@example.com",
            city="CityA",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=8002,
            age=30,
            first_name="Bob",
            last_name="Jones",
            phone="8002222222",
            email="bob@example.com",
            city="CityB",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll participants in engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=4001,
            engagement_id=3002,
            user_id=8001,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=4002,
            engagement_id=3002,
            user_id=8002,
            slot_start_time=time(9, 30),
            engagement_date=date(2026, 3, 10),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/code/ENG3002/participants", headers=_auth_header(7004))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 2
    assert len(body["data"]) == 2

    # Check participant details
    user_ids = [p["user_id"] for p in body["data"]]
    assert 8001 in user_ids
    assert 8002 in user_ids

    alice = next(p for p in body["data"] if p["user_id"] == 8001)
    assert alice["first_name"] == "Alice"
    assert alice["last_name"] == "Smith"
    assert alice["phone"] == "8001111111"
    assert alice["email"] == "alice@example.com"
    assert alice["city"] == "CityA"
    assert alice["status"] == "active"


@pytest.mark.asyncio
async def test_get_engagement_code_participants_returns_distinct_users(async_client, test_db_session):
    """Test that the endpoint returns distinct users even if enrolled in multiple slots."""
    await _seed_employee(test_db_session, user_id=7005, employee_id=204)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=103,
            package_code="PKG103",
            display_name="Test Package 103",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create engagement
    test_db_session.add(
        Engagement(
            engagement_id=3003,
            engagement_code="ENG3003",
            engagement_type="b2b",
            assessment_package_id=103,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    await test_db_session.flush()

    # Create participant
    test_db_session.add(
        User(
            user_id=8003,
            age=30,
            first_name="Charlie",
            last_name="Brown",
            phone="8003333333",
            email="charlie@example.com",
            city="CityC",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll same user in multiple slots (should appear only once)
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=4003,
            engagement_id=3003,
            user_id=8003,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=4004,
            engagement_id=3003,
            user_id=8003,
            slot_start_time=time(10, 0),
            engagement_date=date(2026, 3, 11),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/code/ENG3003/participants", headers=_auth_header(7005))
    assert response.status_code == 200
    body = response.json()
    
    # Should return only 1 distinct user even though enrolled in 2 slots
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["user_id"] == 8003
    assert body["data"][0]["first_name"] == "Charlie"


@pytest.mark.asyncio
async def test_get_engagement_code_participants_validates_pagination_params(async_client, test_db_session):
    """Test that the endpoint validates pagination parameters."""
    await _seed_employee(test_db_session, user_id=7006, employee_id=205)

    # Create assessment package and engagement
    test_db_session.add(
        AssessmentPackage(
            package_id=104,
            package_code="PKG104",
            display_name="Test Package 104",
            status="active",
        )
    )
    await test_db_session.flush()
    
    test_db_session.add(
        Engagement(
            engagement_id=3004,
            engagement_code="ENG3004",
            engagement_type="b2b",
            assessment_package_id=104,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    # Test invalid page
    response = await async_client.get("/engagements/code/ENG3004/participants?page=0", headers=_auth_header(7006))
    assert response.status_code == 400

    # Test invalid limit (too small)
    response = await async_client.get("/engagements/code/ENG3004/participants?limit=0", headers=_auth_header(7006))
    assert response.status_code == 400

    # Test invalid limit (too large)
    response = await async_client.get("/engagements/code/ENG3004/participants?limit=101", headers=_auth_header(7006))
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_engagement_code_participants_paginates_results(async_client, test_db_session):
    """Test that the endpoint paginates results correctly."""
    await _seed_employee(test_db_session, user_id=7007, employee_id=206)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=105,
            package_code="PKG105",
            display_name="Test Package 105",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create engagement
    test_db_session.add(
        Engagement(
            engagement_id=3005,
            engagement_code="ENG3005",
            engagement_type="b2b",
            assessment_package_id=105,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=5,
        )
    )
    await test_db_session.flush()

    # Create 5 participants
    for i in range(1, 6):
        user_id = 8100 + i
        test_db_session.add(
            User(
                user_id=user_id,
                age=30,
                first_name=f"User{i}",
                last_name=f"Last{i}",
                phone=f"810{i}000000",
                email=f"user{i}@example.com",
                city=f"City{i}",
                status="active",
            )
        )
    await test_db_session.flush()
    
    for i in range(1, 6):
        user_id = 8100 + i
        test_db_session.add(
            EngagementTimeSlot(
                time_slot_id=4100 + i,
                engagement_id=3005,
                user_id=user_id,
                slot_start_time=time(9, i * 10),
                engagement_date=date(2026, 3, 10),
            )
        )
    await test_db_session.commit()

    # Get page 1 with limit 2
    response = await async_client.get(
        "/engagements/code/ENG3005/participants?page=1&limit=2", headers=_auth_header(7007)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 2

    # Get page 2 with limit 2
    response = await async_client.get(
        "/engagements/code/ENG3005/participants?page=2&limit=2", headers=_auth_header(7007)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 2
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 2

    # Get page 3 with limit 2 (should have 1 item)
    response = await async_client.get(
        "/engagements/code/ENG3005/participants?page=3&limit=2", headers=_auth_header(7007)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 3
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 1


@pytest.mark.asyncio
async def test_get_engagement_code_participants_excludes_other_engagements(async_client, test_db_session):
    """Test that the endpoint only returns participants from the specified engagement."""
    await _seed_employee(test_db_session, user_id=7008, employee_id=207)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=106,
            package_code="PKG106",
            display_name="Test Package 106",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create two engagements
    test_db_session.add(
        Engagement(
            engagement_id=3006,
            engagement_code="ENG3006",
            engagement_type="b2b",
            assessment_package_id=106,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=3007,
            engagement_code="ENG3007",
            engagement_type="b2b",
            assessment_package_id=106,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    await test_db_session.flush()

    # Create participants for each engagement
    test_db_session.add(
        User(
            user_id=8201,
            age=30,
            first_name="EngA_User",
            last_name="Smith",
            phone="8201000000",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=8202,
            age=30,
            first_name="EngB_User",
            last_name="Jones",
            phone="8202000000",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll participants
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=4201,
            engagement_id=3006,
            user_id=8201,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=4202,
            engagement_id=3007,
            user_id=8202,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    await test_db_session.commit()

    # Get participants for ENG3006 (should only get user 8201)
    response = await async_client.get("/engagements/code/ENG3006/participants", headers=_auth_header(7008))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["user_id"] == 8201
    assert body["data"][0]["first_name"] == "EngA_User"

    # Get participants for ENG3007 (should only get user 8202)
    response = await async_client.get("/engagements/code/ENG3007/participants", headers=_auth_header(7008))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["user_id"] == 8202
    assert body["data"][0]["first_name"] == "EngB_User"
