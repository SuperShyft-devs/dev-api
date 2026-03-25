"""Integration tests for public engagement participants endpoint (employee-only)."""

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
from modules.organizations.models import Organization


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
            no_of_tests=1,
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
async def test_get_public_participants_requires_auth(async_client):
    """Test that the endpoint requires authentication."""
    response = await async_client.get("/engagements/public/participants")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_public_participants_requires_employee(async_client, test_db_session):
    """Test that the endpoint requires employee role."""
    test_db_session.add(User(user_id=6001, age=30, phone="6001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/engagements/public/participants", headers=_auth_header(6001))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_public_participants_returns_empty_list_when_no_b2c_engagements(async_client, test_db_session):
    """Test that the endpoint returns empty list when no B2C engagements exist."""
    await _seed_employee(test_db_session, user_id=6002, employee_id=301)

    response = await async_client.get("/engagements/public/participants", headers=_auth_header(6002))
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["meta"]["total"] == 0
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 20


@pytest.mark.asyncio
async def test_get_public_participants_returns_empty_list_when_no_participants(async_client, test_db_session):
    """Test that the endpoint returns empty list when B2C engagements have no participants."""
    await _seed_employee(test_db_session, user_id=6003, employee_id=302)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=201,
            package_code="PKG201",
            display_name="Test Package 201",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create B2C engagement (no organization_id) with no participants
    test_db_session.add(
        Engagement(
            engagement_id=5001,
            organization_id=None,  # B2C engagement
            engagement_code="B2C5001",
            engagement_type="healthcamp",
            assessment_package_id=201,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/public/participants", headers=_auth_header(6003))
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["meta"]["total"] == 0


@pytest.mark.asyncio
async def test_get_public_participants_returns_participants_from_b2c_engagements(
    async_client, test_db_session
):
    """Test that the endpoint returns participants from B2C engagements."""
    await _seed_employee(test_db_session, user_id=6004, employee_id=303)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=202,
            package_code="PKG202",
            display_name="Test Package 202",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create B2C engagement
    test_db_session.add(
        Engagement(
            engagement_id=5002,
            organization_id=None,  # B2C engagement
            engagement_code="B2C5002",
            engagement_type="healthcamp",
            assessment_package_id=202,
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
            user_id=7001,
            age=30,
            first_name="Alice",
            last_name="Public",
            phone="7001111111",
            email="alice.public@example.com",
            city="CityA",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=7002,
            age=30,
            first_name="Bob",
            last_name="Public",
            phone="7002222222",
            email="bob.public@example.com",
            city="CityB",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll participants in B2C engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6001,
            engagement_id=5002,
            user_id=7001,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6002,
            engagement_id=5002,
            user_id=7002,
            slot_start_time=time(9, 30),
            engagement_date=date(2026, 3, 10),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/public/participants", headers=_auth_header(6004))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 2
    assert len(body["data"]) == 2

    # Check participant details
    user_ids = [p["user_id"] for p in body["data"]]
    assert 7001 in user_ids
    assert 7002 in user_ids

    alice = next(p for p in body["data"] if p["user_id"] == 7001)
    assert alice["first_name"] == "Alice"
    assert alice["last_name"] == "Public"
    assert alice["phone"] == "7001111111"
    assert alice["email"] == "alice.public@example.com"
    assert alice["city"] == "CityA"
    assert alice["status"] == "active"


@pytest.mark.asyncio
async def test_get_public_participants_returns_distinct_users_across_multiple_b2c_engagements(
    async_client, test_db_session
):
    """Test that the endpoint returns distinct users across multiple B2C engagements."""
    await _seed_employee(test_db_session, user_id=6005, employee_id=304)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=203,
            package_code="PKG203",
            display_name="Test Package 203",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create two B2C engagements
    test_db_session.add(
        Engagement(
            engagement_id=5003,
            organization_id=None,  # B2C engagement
            engagement_code="B2C5003",
            engagement_type="healthcamp",
            assessment_package_id=203,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=2,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=5004,
            organization_id=None,  # B2C engagement
            engagement_code="B2C5004",
            engagement_type="healthcamp",
            assessment_package_id=203,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 30),
            status="active",
            participant_count=2,
        )
    )
    await test_db_session.flush()

    # Create participants
    test_db_session.add(
        User(
            user_id=7003,
            age=30,
            first_name="Charlie",
            last_name="Public",
            phone="7003333333",
            email="charlie.public@example.com",
            city="CityC",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=7004,
            age=30,
            first_name="Diana",
            last_name="Public",
            phone="7004444444",
            email="diana.public@example.com",
            city="CityD",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=7005,
            age=30,
            first_name="Eve",
            last_name="Public",
            phone="7005555555",
            email="eve.public@example.com",
            city="CityE",
            status="active",
        )
    )
    await test_db_session.flush()

    # User 7003 enrolled in both engagements (should appear only once)
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6003,
            engagement_id=5003,
            user_id=7003,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6004,
            engagement_id=5004,
            user_id=7003,
            slot_start_time=time(10, 0),
            engagement_date=date(2026, 4, 10),
        )
    )

    # User 7004 enrolled only in first engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6005,
            engagement_id=5003,
            user_id=7004,
            slot_start_time=time(9, 30),
            engagement_date=date(2026, 3, 10),
        )
    )

    # User 7005 enrolled only in second engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6006,
            engagement_id=5004,
            user_id=7005,
            slot_start_time=time(10, 30),
            engagement_date=date(2026, 4, 10),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/public/participants", headers=_auth_header(6005))
    assert response.status_code == 200
    body = response.json()
    
    # Should return 3 distinct users (7003, 7004, 7005) even though 7003 is in 2 engagements
    assert body["meta"]["total"] == 3
    assert len(body["data"]) == 3

    user_ids = [p["user_id"] for p in body["data"]]
    assert 7003 in user_ids
    assert 7004 in user_ids
    assert 7005 in user_ids


@pytest.mark.asyncio
async def test_get_public_participants_excludes_b2b_participants(async_client, test_db_session):
    """Test that the endpoint only returns B2C participants, not B2B participants."""
    await _seed_employee(test_db_session, user_id=6006, employee_id=305)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=204,
            package_code="PKG204",
            display_name="Test Package 204",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create organization for B2B engagement
    test_db_session.add(
        Organization(
            organization_id=2001,
            name="TestOrg",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create B2C engagement
    test_db_session.add(
        Engagement(
            engagement_id=5005,
            organization_id=None,  # B2C engagement
            engagement_code="B2C5005",
            engagement_type="healthcamp",
            assessment_package_id=204,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    
    # Create B2B engagement
    test_db_session.add(
        Engagement(
            engagement_id=5006,
            organization_id=2001,  # B2B engagement
            engagement_code="B2B5006",
            engagement_type="b2b",
            assessment_package_id=204,
            diagnostic_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    await test_db_session.flush()

    # Create participants
    test_db_session.add(
        User(
            user_id=7006,
            age=30,
            first_name="B2C_User",
            last_name="Public",
            phone="7006666666",
            email="b2c@example.com",
            city="CityF",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=7007,
            age=30,
            first_name="B2B_User",
            last_name="Corporate",
            phone="7007777777",
            email="b2b@example.com",
            city="CityG",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll user in B2C engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6007,
            engagement_id=5005,
            user_id=7006,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    
    # Enroll user in B2B engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=6008,
            engagement_id=5006,
            user_id=7007,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/public/participants", headers=_auth_header(6006))
    assert response.status_code == 200
    body = response.json()
    
    # Should only return B2C participant (user 7006), not B2B participant (user 7007)
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["user_id"] == 7006
    assert body["data"][0]["first_name"] == "B2C_User"


@pytest.mark.asyncio
async def test_get_public_participants_validates_pagination_params(async_client, test_db_session):
    """Test that the endpoint validates pagination parameters."""
    await _seed_employee(test_db_session, user_id=6007, employee_id=306)

    # Test invalid page
    response = await async_client.get("/engagements/public/participants?page=0", headers=_auth_header(6007))
    assert response.status_code == 400

    # Test invalid limit (too small)
    response = await async_client.get("/engagements/public/participants?limit=0", headers=_auth_header(6007))
    assert response.status_code == 400

    # Test invalid limit (too large)
    response = await async_client.get("/engagements/public/participants?limit=101", headers=_auth_header(6007))
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_public_participants_paginates_results(async_client, test_db_session):
    """Test that the endpoint paginates results correctly."""
    await _seed_employee(test_db_session, user_id=6008, employee_id=307)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=205,
            package_code="PKG205",
            display_name="Test Package 205",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create B2C engagement
    test_db_session.add(
        Engagement(
            engagement_id=5007,
            organization_id=None,  # B2C engagement
            engagement_code="B2C5007",
            engagement_type="healthcamp",
            assessment_package_id=205,
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
        user_id = 7100 + i
        test_db_session.add(
            User(
                user_id=user_id,
                age=30,
                first_name=f"User{i}",
                last_name=f"Last{i}",
                phone=f"710{i}000000",
                email=f"user{i}@example.com",
                city=f"City{i}",
                status="active",
            )
        )
    await test_db_session.flush()
    
    for i in range(1, 6):
        user_id = 7100 + i
        test_db_session.add(
            EngagementTimeSlot(
                time_slot_id=6100 + i,
                engagement_id=5007,
                user_id=user_id,
                slot_start_time=time(9, i * 10),
                engagement_date=date(2026, 3, 10),
            )
        )
    await test_db_session.commit()

    # Get page 1 with limit 2
    response = await async_client.get(
        "/engagements/public/participants?page=1&limit=2", headers=_auth_header(6008)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 2

    # Get page 2 with limit 2
    response = await async_client.get(
        "/engagements/public/participants?page=2&limit=2", headers=_auth_header(6008)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 2
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 2

    # Get page 3 with limit 2 (should have 1 item)
    response = await async_client.get(
        "/engagements/public/participants?page=3&limit=2", headers=_auth_header(6008)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 3
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 1
