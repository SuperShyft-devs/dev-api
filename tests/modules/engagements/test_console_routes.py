"""Integration tests for engagement console routes."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.models import Employee
from modules.engagements.models import Engagement, OnboardingAssistantAssignment
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_employee(test_db_session, *, user_id: int, employee_id: int, role: str = "admin"):
    existing_diag = await test_db_session.get(DiagnosticPackage, 1)
    if existing_diag is None:
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

    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=employee_id, user_id=user_id, role=role, status="active"))
    await test_db_session.commit()


async def _ensure_assessment_package(test_db_session, package_id: int = 1):
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


async def _seed_engagement(
    test_db_session,
    *,
    engagement_id: int,
    status: str = "running",
    engagement_code: str | None = None,
) -> Engagement:
    await _ensure_assessment_package(test_db_session)
    engagement = Engagement(
        engagement_id=engagement_id,
        engagement_name=f"Engagement {engagement_id}",
        engagement_code=engagement_code or f"ENG{engagement_id}",
        engagement_type="doctor",
        assessment_package_id=1,
        diagnostic_package_id=1,
        status=status,
        participant_count=0,
        start_date=date.today(),
        end_date=date.today(),
    )
    test_db_session.add(engagement)
    await test_db_session.flush()
    return engagement


async def _assign_assistant(
    test_db_session,
    *,
    assignment_id: int,
    employee_id: int,
    engagement_id: int,
) -> None:
    test_db_session.add(
        OnboardingAssistantAssignment(
            onboarding_assistant_id=assignment_id,
            employee_id=employee_id,
            engagement_id=engagement_id,
        )
    )
    await test_db_session.commit()


# ============================================================================
# Console routes — assigned + running
# ============================================================================


@pytest.mark.asyncio
async def test_console_routes_assigned_onboarding_assistant_running(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9101, employee_id=201, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=6001, status="running")
    await _assign_assistant(test_db_session, assignment_id=1, employee_id=201, engagement_id=6001)

    headers = _auth_header(9101)

    list_res = await async_client.get("/engagements/console/engagements", headers=headers)
    assert list_res.status_code == 200
    assert len(list_res.json()["data"]) == 1
    assert list_res.json()["data"][0]["engagement_id"] == 6001

    detail_res = await async_client.get("/engagements/6001/console", headers=headers)
    assert detail_res.status_code == 200
    assert detail_res.json()["data"]["engagement_id"] == 6001

    parts_res = await async_client.get("/engagements/6001/console/participants", headers=headers)
    assert parts_res.status_code == 200
    assert isinstance(parts_res.json()["data"], list)


@pytest.mark.asyncio
async def test_console_routes_assigned_admin_running(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9102, employee_id=202, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6002, status="running")
    await _assign_assistant(test_db_session, assignment_id=2, employee_id=202, engagement_id=6002)

    headers = _auth_header(9102)

    list_res = await async_client.get("/engagements/console/engagements", headers=headers)
    assert list_res.status_code == 200

    detail_res = await async_client.get("/engagements/6002/console", headers=headers)
    assert detail_res.status_code == 200

    parts_res = await async_client.get("/engagements/6002/console/participants", headers=headers)
    assert parts_res.status_code == 200


# ============================================================================
# Console routes — not assigned
# ============================================================================


@pytest.mark.asyncio
async def test_console_routes_not_assigned_onboarding_assistant_403(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9103, employee_id=203, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=6003, status="running")
    await test_db_session.commit()

    headers = _auth_header(9103)

    assert (await async_client.get("/engagements/console/engagements", headers=headers)).status_code == 200
    assert (await async_client.get("/engagements/6003/console", headers=headers)).status_code == 403
    assert (await async_client.get("/engagements/6003/console/participants", headers=headers)).status_code == 403


@pytest.mark.asyncio
async def test_console_routes_not_assigned_admin_403(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9104, employee_id=204, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6004, status="running")
    await test_db_session.commit()

    headers = _auth_header(9104)

    assert (await async_client.get("/engagements/6004/console", headers=headers)).status_code == 403
    assert (await async_client.get("/engagements/6004/console/participants", headers=headers)).status_code == 403


# ============================================================================
# Console routes — not running
# ============================================================================


@pytest.mark.asyncio
async def test_console_routes_assigned_completed_422(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9105, employee_id=205, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=6005, status="completed")
    await _assign_assistant(test_db_session, assignment_id=3, employee_id=205, engagement_id=6005)

    headers = _auth_header(9105)

    detail_res = await async_client.get("/engagements/6005/console", headers=headers)
    assert detail_res.status_code == 422
    assert detail_res.json()["error_code"] == "ENGAGEMENT_NOT_RUNNING"

    parts_res = await async_client.get("/engagements/6005/console/participants", headers=headers)
    assert parts_res.status_code == 422
    assert parts_res.json()["error_code"] == "ENGAGEMENT_NOT_RUNNING"


# ============================================================================
# Admin endpoints — role restrictions
# ============================================================================


@pytest.mark.asyncio
async def test_admin_participants_onboarding_assistant_403(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9106, employee_id=206, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=6006, status="running")
    await _assign_assistant(test_db_session, assignment_id=4, employee_id=206, engagement_id=6006)

    headers = _auth_header(9106)

    detail_res = await async_client.get("/engagements/6006", headers=headers)
    assert detail_res.status_code == 403

    parts_res = await async_client.get("/engagements/6006/participants", headers=headers)
    assert parts_res.status_code == 403


@pytest.mark.asyncio
async def test_admin_participants_admin_200(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9107, employee_id=207, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6007, status="running")
    await test_db_session.commit()

    headers = _auth_header(9107)

    detail_res = await async_client.get("/engagements/6007", headers=headers)
    assert detail_res.status_code == 200

    parts_res = await async_client.get("/engagements/6007/participants", headers=headers)
    assert parts_res.status_code == 200


# ============================================================================
# Console list — only running + assigned
# ============================================================================


@pytest.mark.asyncio
async def test_console_list_returns_only_running_assigned(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9108, employee_id=208, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=6008, status="running", engagement_code="RUN001")
    await _seed_engagement(test_db_session, engagement_id=6009, status="completed", engagement_code="DONE001")
    await _seed_engagement(test_db_session, engagement_id=6010, status="running", engagement_code="RUN002")
    await _assign_assistant(test_db_session, assignment_id=5, employee_id=208, engagement_id=6008)
    await _assign_assistant(test_db_session, assignment_id=6, employee_id=208, engagement_id=6009)
    # 6010 is running but not assigned to employee 208

    headers = _auth_header(9108)
    response = await async_client.get("/engagements/console/engagements", headers=headers)
    assert response.status_code == 200

    ids = {row["engagement_id"] for row in response.json()["data"]}
    assert ids == {6008}
