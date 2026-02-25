"""Integration tests for organizations participants endpoint (employee-only)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.employee.models import Employee
from modules.organizations.models import Organization
from modules.users.models import User
from modules.engagements.models import Engagement, EngagementTimeSlot
from modules.assessments.models import AssessmentPackage
from datetime import date, time


def _auth_header(user_id: int) -> dict[str, str]:
    """Create authentication header for testing."""
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int = 1):
    """Seed a test employee."""
    user = User(user_id=user_id, phone=f"{user_id}000000000", status="active")
    test_db_session.add(user)
    await test_db_session.flush()
    
    employee = Employee(employee_id=employee_id, user_id=user_id, role="admin", status="active")
    test_db_session.add(employee)
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_get_organization_participants_requires_auth(async_client):
    """Test that the endpoint requires authentication."""
    response = await async_client.get("/organizations/1/participants")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_organization_participants_requires_employee(async_client, test_db_session):
    """Test that the endpoint requires employee role."""
    test_db_session.add(User(user_id=8001, phone="8001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/organizations/1/participants", headers=_auth_header(8001))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_get_organization_participants_returns_404_for_nonexistent_organization(async_client, test_db_session):
    """Test that the endpoint returns 404 for non-existent organization."""
    await _seed_employee(test_db_session, user_id=8002, employee_id=101)

    response = await async_client.get("/organizations/99999/participants", headers=_auth_header(8002))
    assert response.status_code == 404
    assert response.json()["error_code"] == "ORGANIZATION_NOT_FOUND"


@pytest.mark.asyncio
async def test_get_organization_participants_returns_empty_list_when_no_engagements(async_client, test_db_session):
    """Test that the endpoint returns empty list when organization has no engagements."""
    await _seed_employee(test_db_session, user_id=8003, employee_id=102)

    # Create organization with no engagements
    test_db_session.add(
        Organization(
            organization_id=1001,
            name="OrgWithNoEngagements",
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/1001/participants", headers=_auth_header(8003))
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["meta"]["total"] == 0
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 20


@pytest.mark.asyncio
async def test_get_organization_participants_returns_empty_list_when_no_participants(async_client, test_db_session):
    """Test that the endpoint returns empty list when engagements have no participants."""
    await _seed_employee(test_db_session, user_id=8004, employee_id=103)

    # Create assessment package (required for engagements)
    test_db_session.add(
        AssessmentPackage(
            package_id=1,
            package_code="PKG001",
            display_name="Test Package",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create organization with engagement but no participants
    test_db_session.add(
        Organization(
            organization_id=1002,
            name="OrgWithNoParticipants",
            status="active",
        )
    )
    await test_db_session.flush()
    
    test_db_session.add(
        Engagement(
            engagement_id=2001,
            organization_id=1002,
            engagement_code="ENG2001",
            engagement_type="b2b",
            assessment_package_id=1,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/1002/participants", headers=_auth_header(8004))
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["meta"]["total"] == 0


@pytest.mark.asyncio
async def test_get_organization_participants_returns_participants_from_single_engagement(
    async_client, test_db_session
):
    """Test that the endpoint returns participants from a single engagement."""
    await _seed_employee(test_db_session, user_id=8005, employee_id=104)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=2,
            package_code="PKG002",
            display_name="Test Package 2",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create organization with engagement and participants
    test_db_session.add(
        Organization(
            organization_id=1003,
            name="OrgWithParticipants",
            status="active",
        )
    )
    await test_db_session.flush()
    
    test_db_session.add(
        Engagement(
            engagement_id=2002,
            organization_id=1003,
            engagement_code="ENG2002",
            engagement_type="b2b",
            assessment_package_id=2,
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
            user_id=9001,
            first_name="Alice",
            last_name="Smith",
            phone="9001111111",
            email="alice@example.com",
            city="CityA",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=9002,
            first_name="Bob",
            last_name="Jones",
            phone="9002222222",
            email="bob@example.com",
            city="CityB",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll participants in engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3001,
            engagement_id=2002,
            user_id=9001,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3002,
            engagement_id=2002,
            user_id=9002,
            slot_start_time=time(9, 30),
            engagement_date=date(2026, 3, 10),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/1003/participants", headers=_auth_header(8005))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 2
    assert len(body["data"]) == 2

    # Check participant details
    user_ids = [p["user_id"] for p in body["data"]]
    assert 9001 in user_ids
    assert 9002 in user_ids

    alice = next(p for p in body["data"] if p["user_id"] == 9001)
    assert alice["first_name"] == "Alice"
    assert alice["last_name"] == "Smith"
    assert alice["phone"] == "9001111111"
    assert alice["email"] == "alice@example.com"
    assert alice["city"] == "CityA"
    assert alice["status"] == "active"


@pytest.mark.asyncio
async def test_get_organization_participants_returns_distinct_users_across_multiple_engagements(
    async_client, test_db_session
):
    """Test that the endpoint returns distinct users across multiple engagements."""
    await _seed_employee(test_db_session, user_id=8006, employee_id=105)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=3,
            package_code="PKG003",
            display_name="Test Package 3",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create organization
    test_db_session.add(
        Organization(
            organization_id=1004,
            name="OrgMultiEngagement",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create two engagements
    test_db_session.add(
        Engagement(
            engagement_id=2003,
            organization_id=1004,
            engagement_code="ENG2003",
            engagement_type="b2b",
            assessment_package_id=3,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=2,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=2004,
            organization_id=1004,
            engagement_code="ENG2004",
            engagement_type="b2b",
            assessment_package_id=3,
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
            user_id=9003,
            first_name="Charlie",
            last_name="Brown",
            phone="9003333333",
            email="charlie@example.com",
            city="CityC",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=9004,
            first_name="Diana",
            last_name="Prince",
            phone="9004444444",
            email="diana@example.com",
            city="CityD",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=9005,
            first_name="Eve",
            last_name="Adams",
            phone="9005555555",
            email="eve@example.com",
            city="CityE",
            status="active",
        )
    )
    await test_db_session.flush()

    # User 9003 enrolled in both engagements (should appear only once)
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3003,
            engagement_id=2003,
            user_id=9003,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3004,
            engagement_id=2004,
            user_id=9003,
            slot_start_time=time(10, 0),
            engagement_date=date(2026, 4, 10),
        )
    )

    # User 9004 enrolled only in first engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3005,
            engagement_id=2003,
            user_id=9004,
            slot_start_time=time(9, 30),
            engagement_date=date(2026, 3, 10),
        )
    )

    # User 9005 enrolled only in second engagement
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3006,
            engagement_id=2004,
            user_id=9005,
            slot_start_time=time(10, 30),
            engagement_date=date(2026, 4, 10),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/organizations/1004/participants", headers=_auth_header(8006))
    assert response.status_code == 200
    body = response.json()
    
    # Should return 3 distinct users (9003, 9004, 9005) even though 9003 is in 2 engagements
    assert body["meta"]["total"] == 3
    assert len(body["data"]) == 3

    user_ids = [p["user_id"] for p in body["data"]]
    assert 9003 in user_ids
    assert 9004 in user_ids
    assert 9005 in user_ids


@pytest.mark.asyncio
async def test_get_organization_participants_validates_pagination_params(async_client, test_db_session):
    """Test that the endpoint validates pagination parameters."""
    await _seed_employee(test_db_session, user_id=8007, employee_id=106)

    test_db_session.add(
        Organization(
            organization_id=1005,
            name="OrgPagination",
            status="active",
        )
    )
    await test_db_session.commit()

    # Test invalid page
    response = await async_client.get("/organizations/1005/participants?page=0", headers=_auth_header(8007))
    assert response.status_code == 400

    # Test invalid limit (too small)
    response = await async_client.get("/organizations/1005/participants?limit=0", headers=_auth_header(8007))
    assert response.status_code == 400

    # Test invalid limit (too large)
    response = await async_client.get("/organizations/1005/participants?limit=101", headers=_auth_header(8007))
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_organization_participants_paginates_results(async_client, test_db_session):
    """Test that the endpoint paginates results correctly."""
    await _seed_employee(test_db_session, user_id=8008, employee_id=107)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=4,
            package_code="PKG004",
            display_name="Test Package 4",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create organization and engagement
    test_db_session.add(
        Organization(
            organization_id=1006,
            name="OrgPaginationTest",
            status="active",
        )
    )
    await test_db_session.flush()
    
    test_db_session.add(
        Engagement(
            engagement_id=2005,
            organization_id=1006,
            engagement_code="ENG2005",
            engagement_type="b2b",
            assessment_package_id=4,
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
        user_id = 9100 + i
        test_db_session.add(
            User(
                user_id=user_id,
                first_name=f"User{i}",
                last_name=f"Last{i}",
                phone=f"910{i}000000",
                email=f"user{i}@example.com",
                city=f"City{i}",
                status="active",
            )
        )
    await test_db_session.flush()
    
    for i in range(1, 6):
        user_id = 9100 + i
        test_db_session.add(
            EngagementTimeSlot(
                time_slot_id=3100 + i,
                engagement_id=2005,
                user_id=user_id,
                slot_start_time=time(9, i * 10),
                engagement_date=date(2026, 3, 10),
            )
        )
    await test_db_session.commit()

    # Get page 1 with limit 2
    response = await async_client.get(
        "/organizations/1006/participants?page=1&limit=2", headers=_auth_header(8008)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 2

    # Get page 2 with limit 2
    response = await async_client.get(
        "/organizations/1006/participants?page=2&limit=2", headers=_auth_header(8008)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 2
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 2

    # Get page 3 with limit 2 (should have 1 item)
    response = await async_client.get(
        "/organizations/1006/participants?page=3&limit=2", headers=_auth_header(8008)
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 5
    assert body["meta"]["page"] == 3
    assert body["meta"]["limit"] == 2
    assert len(body["data"]) == 1


@pytest.mark.asyncio
async def test_get_organization_participants_excludes_other_organizations(async_client, test_db_session):
    """Test that the endpoint only returns participants from the specified organization."""
    await _seed_employee(test_db_session, user_id=8009, employee_id=108)

    # Create assessment package
    test_db_session.add(
        AssessmentPackage(
            package_id=5,
            package_code="PKG005",
            display_name="Test Package 5",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create two organizations
    test_db_session.add(
        Organization(
            organization_id=1007,
            name="OrgA",
            status="active",
        )
    )
    test_db_session.add(
        Organization(
            organization_id=1008,
            name="OrgB",
            status="active",
        )
    )
    await test_db_session.flush()

    # Create engagements for both organizations
    test_db_session.add(
        Engagement(
            engagement_id=2006,
            organization_id=1007,
            engagement_code="ENG2006",
            engagement_type="b2b",
            assessment_package_id=5,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=2007,
            organization_id=1008,
            engagement_code="ENG2007",
            engagement_type="b2b",
            assessment_package_id=5,
            slot_duration=20,
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 31),
            status="active",
            participant_count=1,
        )
    )
    await test_db_session.flush()

    # Create participants for each organization
    test_db_session.add(
        User(
            user_id=9201,
            first_name="OrgA_User",
            last_name="Smith",
            phone="9201000000",
            status="active",
        )
    )
    test_db_session.add(
        User(
            user_id=9202,
            first_name="OrgB_User",
            last_name="Jones",
            phone="9202000000",
            status="active",
        )
    )
    await test_db_session.flush()

    # Enroll participants
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3201,
            engagement_id=2006,
            user_id=9201,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    test_db_session.add(
        EngagementTimeSlot(
            time_slot_id=3202,
            engagement_id=2007,
            user_id=9202,
            slot_start_time=time(9, 0),
            engagement_date=date(2026, 3, 10),
        )
    )
    await test_db_session.commit()

    # Get participants for OrgA (should only get user 9201)
    response = await async_client.get("/organizations/1007/participants", headers=_auth_header(8009))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["user_id"] == 9201
    assert body["data"][0]["first_name"] == "OrgA_User"

    # Get participants for OrgB (should only get user 9202)
    response = await async_client.get("/organizations/1008/participants", headers=_auth_header(8009))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 1
    assert len(body["data"]) == 1
    assert body["data"][0]["user_id"] == 9202
    assert body["data"][0]["first_name"] == "OrgB_User"
