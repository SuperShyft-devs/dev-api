"""Integration tests for engagement onboarding assistant assignment routes (employee-only)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage
from modules.employee.models import Employee
from modules.engagements.models import Engagement, OnboardingAssistantAssignment
from modules.diagnostics.models import DiagnosticPackage
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    """Generate authorization header with JWT token."""
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int, role: str = "admin"):
    """Seed a user and employee record."""
    # Ensure required diagnostic package exists for b2b engagements.
    existing_diag = await test_db_session.get(DiagnosticPackage, 1)
    if existing_diag is None:
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

    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role=role, status="active"))
    await test_db_session.commit()


async def _ensure_assessment_package(test_db_session, package_id: int = 1):
    """Ensure assessment package exists for foreign key constraint."""
    existing = await test_db_session.get(AssessmentPackage, package_id)
    if existing is None:
        test_db_session.add(
            AssessmentPackage(
                package_id=package_id,
                package_code=f"PKG{package_id:03d}",
                display_name=f"Test Package {package_id}",
                status="active",
            )
        )
        await test_db_session.flush()


# ============================================================================
# GET /engagements/{engagement_id}/onboarding-assistants - List Tests
# ============================================================================


@pytest.mark.asyncio
async def test_list_onboarding_assistants_requires_auth(async_client):
    """Test that listing onboarding assistants requires authentication."""
    response = await async_client.get("/engagements/1/onboarding-assistants")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_onboarding_assistants_requires_employee(async_client, test_db_session):
    """Test that listing onboarding assistants requires employee role."""
    test_db_session.add(User(user_id=9001, age=30, phone="9001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/engagements/1/onboarding-assistants", headers=_auth_header(9001))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_empty_list(async_client, test_db_session):
    """Test that listing onboarding assistants returns empty list when none assigned."""
    await _seed_employee(test_db_session, user_id=9002, employee_id=101)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5001,
            engagement_name="Test Engagement",
            engagement_code="ENG001",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/engagements/5001/onboarding-assistants", headers=_auth_header(9002))
    assert response.status_code == 200

    body = response.json()["data"]
    assert isinstance(body, list)
    assert len(body) == 0


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_assigned_employees(async_client, test_db_session):
    """Test that listing onboarding assistants returns all assigned employees."""
    await _seed_employee(test_db_session, user_id=9003, employee_id=102)
    await _seed_employee(test_db_session, user_id=9004, employee_id=103)
    await _seed_employee(test_db_session, user_id=9005, employee_id=104)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5002,
            engagement_name="Test Engagement",
            engagement_code="ENG002",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    # Assign two employees
    test_db_session.add(OnboardingAssistantAssignment(onboarding_assistant_id=1, employee_id=103, engagement_id=5002))
    test_db_session.add(OnboardingAssistantAssignment(onboarding_assistant_id=2, employee_id=104, engagement_id=5002))
    await test_db_session.commit()

    response = await async_client.get("/engagements/5002/onboarding-assistants", headers=_auth_header(9003))
    assert response.status_code == 200

    body = response.json()["data"]
    assert isinstance(body, list)
    assert len(body) == 2
    assert {row["employee_id"] for row in body} == {103, 104}


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_404_when_engagement_missing(async_client, test_db_session):
    """Test that listing onboarding assistants returns 404 when engagement does not exist."""
    await _seed_employee(test_db_session, user_id=9006, employee_id=105)
    await _ensure_assessment_package(test_db_session)

    response = await async_client.get("/engagements/999999/onboarding-assistants", headers=_auth_header(9006))
    assert response.status_code == 404
    assert response.json() == {"error_code": "ENGAGEMENT_NOT_FOUND", "message": "Engagement does not exist"}


# ============================================================================
# POST /engagements/{engagement_id}/onboarding-assistants - Add Tests
# ============================================================================


@pytest.mark.asyncio
async def test_add_onboarding_assistants_requires_auth(async_client):
    """Test that adding onboarding assistants requires authentication."""
    response = await async_client.post("/engagements/1/onboarding-assistants", json={"employee_ids": [1]})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_add_onboarding_assistants_requires_employee(async_client, test_db_session):
    """Test that adding onboarding assistants requires employee role."""
    test_db_session.add(User(user_id=9007, age=30, phone="9007000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/1/onboarding-assistants",
        headers=_auth_header(9007),
        json={"employee_ids": [1]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_add_onboarding_assistants_creates_assignment(async_client, test_db_session):
    """Test that adding onboarding assistants creates assignment records."""
    await _seed_employee(test_db_session, user_id=9008, employee_id=106)
    await _seed_employee(test_db_session, user_id=9009, employee_id=107)
    await _ensure_assessment_package(test_db_session)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5003,
            engagement_name="Test Engagement",
            engagement_code="ENG003",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5003/onboarding-assistants",
        headers=_auth_header(9008),
        json={"employee_ids": [107]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert data["engagement_id"] == 5003
    assert data["added_employee_ids"] == [107]
    assert data["skipped_employee_ids"] == []

    # Verify assignment was created
    result = await test_db_session.execute(
        OnboardingAssistantAssignment.__table__.select().where(
            OnboardingAssistantAssignment.engagement_id == 5003
        )
    )
    rows = list(result.all())
    assert len(rows) == 1
    assert rows[0].employee_id == 107


@pytest.mark.asyncio
async def test_add_onboarding_assistants_skips_duplicates(async_client, test_db_session):
    """Test that adding onboarding assistants skips duplicate assignments."""
    await _seed_employee(test_db_session, user_id=9010, employee_id=108)
    await _seed_employee(test_db_session, user_id=9011, employee_id=109)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5004,
            engagement_name="Test Engagement",
            engagement_code="ENG004",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    # Pre-existing assignment
    test_db_session.add(OnboardingAssistantAssignment(onboarding_assistant_id=10, employee_id=109, engagement_id=5004))
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5004/onboarding-assistants",
        headers=_auth_header(9010),
        json={"employee_ids": [109, 109]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert data["added_employee_ids"] == []
    assert data["skipped_employee_ids"] == [109]


@pytest.mark.asyncio
async def test_add_onboarding_assistants_handles_multiple(async_client, test_db_session):
    """Test that adding multiple onboarding assistants works correctly."""
    await _seed_employee(test_db_session, user_id=9012, employee_id=110)
    await _seed_employee(test_db_session, user_id=9013, employee_id=111)
    await _seed_employee(test_db_session, user_id=9014, employee_id=112)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5005,
            engagement_name="Test Engagement",
            engagement_code="ENG005",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5005/onboarding-assistants",
        headers=_auth_header(9012),
        json={"employee_ids": [111, 112]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert set(data["added_employee_ids"]) == {111, 112}
    assert data["skipped_employee_ids"] == []


@pytest.mark.asyncio
async def test_add_onboarding_assistants_returns_404_when_employee_missing(async_client, test_db_session):
    """Test that adding onboarding assistants returns 404 when employee does not exist."""
    await _seed_employee(test_db_session, user_id=9015, employee_id=113)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5006,
            engagement_name="Test Engagement",
            engagement_code="ENG006",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5006/onboarding-assistants",
        headers=_auth_header(9015),
        json={"employee_ids": [999999]},
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "EMPLOYEE_NOT_FOUND", "message": "Employee does not exist"}


@pytest.mark.asyncio
async def test_add_onboarding_assistants_returns_404_when_engagement_missing(async_client, test_db_session):
    """Test that adding onboarding assistants returns 404 when engagement does not exist."""
    await _seed_employee(test_db_session, user_id=9016, employee_id=114)
    await _ensure_assessment_package(test_db_session)

    response = await async_client.post(
        "/engagements/999999/onboarding-assistants",
        headers=_auth_header(9016),
        json={"employee_ids": [114]},
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "ENGAGEMENT_NOT_FOUND", "message": "Engagement does not exist"}


@pytest.mark.asyncio
async def test_add_onboarding_assistants_validates_empty_list(async_client, test_db_session):
    """Test that adding onboarding assistants validates non-empty employee list."""
    await _seed_employee(test_db_session, user_id=9017, employee_id=115)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5007,
            engagement_name="Test Engagement",
            engagement_code="ENG007",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5007/onboarding-assistants",
        headers=_auth_header(9017),
        json={"employee_ids": []},
    )
    assert response.status_code == 400


# ============================================================================
# DELETE /engagements/{engagement_id}/onboarding-assistants/{employee_id} - Remove Tests
# ============================================================================


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_requires_auth(async_client):
    """Test that removing onboarding assistant requires authentication."""
    response = await async_client.delete("/engagements/1/onboarding-assistants/1")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_requires_employee(async_client, test_db_session):
    """Test that removing onboarding assistant requires employee role."""
    test_db_session.add(User(user_id=9018, age=30, phone="9018000000", status="active"))
    await test_db_session.commit()

    response = await async_client.delete("/engagements/1/onboarding-assistants/1", headers=_auth_header(9018))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_deletes_assignment(async_client, test_db_session):
    """Test that removing onboarding assistant deletes assignment record."""
    await _seed_employee(test_db_session, user_id=9019, employee_id=116)
    await _seed_employee(test_db_session, user_id=9020, employee_id=117)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5008,
            engagement_name="Test Engagement",
            engagement_code="ENG008",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    test_db_session.add(OnboardingAssistantAssignment(onboarding_assistant_id=20, employee_id=117, engagement_id=5008))
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/5008/onboarding-assistants/117",
        headers=_auth_header(9019),
    )
    assert response.status_code == 200

    data = response.json()["data"]
    assert data == {"engagement_id": 5008, "removed_employee_id": 117}

    # Verify assignment was deleted
    link = await test_db_session.execute(
        OnboardingAssistantAssignment.__table__.select().where(
            (OnboardingAssistantAssignment.engagement_id == 5008)
            & (OnboardingAssistantAssignment.employee_id == 117)
        )
    )
    assert link.first() is None


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_returns_404_when_assignment_missing(async_client, test_db_session):
    """Test that removing onboarding assistant returns 404 when assignment does not exist."""
    await _seed_employee(test_db_session, user_id=9021, employee_id=118)
    await _ensure_assessment_package(test_db_session)

    test_db_session.add(
        Engagement(
            engagement_id=5009,
            engagement_name="Test Engagement",
            engagement_code="ENG009",
            engagement_type="b2b",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
            participant_count=0,
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/5009/onboarding-assistants/999999",
        headers=_auth_header(9021),
    )
    assert response.status_code == 404
    assert response.json() == {
        "error_code": "ONBOARDING_ASSISTANT_ASSIGNMENT_NOT_FOUND",
        "message": "Employee is not assigned to this engagement",
    }


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_returns_404_when_engagement_missing(async_client, test_db_session):
    """Test that removing onboarding assistant returns 404 when engagement does not exist."""
    await _seed_employee(test_db_session, user_id=9022, employee_id=119)
    await _ensure_assessment_package(test_db_session)

    response = await async_client.delete(
        "/engagements/999999/onboarding-assistants/119",
        headers=_auth_header(9022),
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "ENGAGEMENT_NOT_FOUND", "message": "Engagement does not exist"}

