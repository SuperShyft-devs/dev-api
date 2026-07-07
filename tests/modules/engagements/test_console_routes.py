"""Integration tests for engagement console routes."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentPackage
from modules.diagnostics.models import DiagnosticPackage
from modules.employee.models import Employee
from modules.engagements.models import Engagement, OnboardingAssistantAssignment
from modules.organizations.models import Organization
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


async def _seed_org_manager_with_engagement(
    test_db_session,
    *,
    manager_user_id: int,
    employee_id: int,
    organization_id: int,
    engagement_id: int,
    status: str = "running",
    contact_person_user_id: int | None = None,
) -> Engagement:
    await _ensure_assessment_package(test_db_session)
    test_db_session.add(
        User(user_id=manager_user_id, age=30, phone=f"{manager_user_id}000000000", status="active")
    )
    await test_db_session.flush()
    test_db_session.add(
        Employee(
            employee_id=employee_id,
            user_id=manager_user_id,
            role="organization_manager",
            status="active",
        )
    )
    test_db_session.add(
        Organization(
            organization_id=organization_id,
            name=f"Org {organization_id}",
            organization_type="corporate",
            status="active",
            contact_person_user_id=contact_person_user_id if contact_person_user_id is not None else manager_user_id,
        )
    )
    engagement = await _seed_engagement(
        test_db_session,
        engagement_id=engagement_id,
        status=status,
        engagement_code=f"ORG{engagement_id}",
    )
    engagement.organization_id = organization_id
    await test_db_session.flush()
    return engagement


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
async def test_console_routes_not_assigned_admin_200(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9104, employee_id=204, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6004, status="running")
    await test_db_session.commit()

    headers = _auth_header(9104)

    assert (await async_client.get("/engagements/6004/console", headers=headers)).status_code == 200
    assert (await async_client.get("/engagements/6004/console/participants", headers=headers)).status_code == 200


# ============================================================================
# Console routes — not running
# ============================================================================


@pytest.mark.asyncio
async def test_console_routes_assigned_onboarding_assistant_completed_422(async_client, test_db_session):
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


@pytest.mark.asyncio
async def test_console_routes_admin_completed_200(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9111, employee_id=211, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6011, status="completed")
    await test_db_session.commit()

    headers = _auth_header(9111)

    detail_res = await async_client.get("/engagements/6011/console", headers=headers)
    assert detail_res.status_code == 200
    assert detail_res.json()["data"]["engagement_id"] == 6011

    parts_res = await async_client.get("/engagements/6011/console/participants", headers=headers)
    assert parts_res.status_code == 200


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


@pytest.mark.asyncio
async def test_console_list_admin_returns_all_running(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9109, employee_id=209, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6012, status="running", engagement_code="ADMINRUN1")
    await _seed_engagement(test_db_session, engagement_id=6013, status="completed", engagement_code="ADMINDONE1")
    await _seed_engagement(test_db_session, engagement_id=6014, status="running", engagement_code="ADMINRUN2")
    await test_db_session.commit()

    headers = _auth_header(9109)
    response = await async_client.get("/engagements/console/engagements", headers=headers)
    assert response.status_code == 200

    ids = {row["engagement_id"] for row in response.json()["data"]}
    assert 6012 in ids
    assert 6014 in ids
    assert 6013 not in ids


@pytest.mark.asyncio
async def test_console_participants_includes_age(async_client, test_db_session):
    from modules.engagements.models import EngagementParticipant

    await _seed_employee(test_db_session, user_id=9110, employee_id=210, role="admin")
    await _seed_engagement(test_db_session, engagement_id=6015, status="running")
    participant_user = User(user_id=9201, age=42, phone="9201000000", status="active", first_name="Age", last_name="Test")
    test_db_session.add(participant_user)
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=9001,
            engagement_id=6015,
            user_id=9201,
            engagement_date=date.today(),
            slot_start_time=time(10, 0),
        )
    )
    await test_db_session.commit()

    headers = _auth_header(9110)
    parts_res = await async_client.get("/engagements/6015/console/participants", headers=headers)
    assert parts_res.status_code == 200
    data = parts_res.json()["data"]
    assert len(data) == 1
    assert data[0]["age"] == 42


@pytest.mark.asyncio
async def test_console_routes_org_manager_assigned_contact_person_running(async_client, test_db_session):
    await _seed_org_manager_with_engagement(
        test_db_session,
        manager_user_id=9112,
        employee_id=212,
        organization_id=9401,
        engagement_id=6016,
        status="running",
    )
    await _assign_assistant(test_db_session, assignment_id=7, employee_id=212, engagement_id=6016)

    headers = _auth_header(9112)

    list_res = await async_client.get("/engagements/console/engagements", headers=headers)
    assert list_res.status_code == 200
    assert len(list_res.json()["data"]) == 1
    assert list_res.json()["data"][0]["engagement_id"] == 6016

    assert (await async_client.get("/engagements/6016/console", headers=headers)).status_code == 200
    assert (
        await async_client.get("/engagements/6016/console/participants", headers=headers)
    ).status_code == 200


@pytest.mark.asyncio
async def test_console_routes_org_manager_assigned_contact_person_completed(async_client, test_db_session):
    await _seed_org_manager_with_engagement(
        test_db_session,
        manager_user_id=9113,
        employee_id=213,
        organization_id=9402,
        engagement_id=6017,
        status="completed",
    )
    await _assign_assistant(test_db_session, assignment_id=8, employee_id=213, engagement_id=6017)

    headers = _auth_header(9113)

    list_res = await async_client.get("/engagements/console/engagements", headers=headers)
    assert list_res.status_code == 200
    assert {row["engagement_id"] for row in list_res.json()["data"]} == {6017}

    assert (await async_client.get("/engagements/6017/console", headers=headers)).status_code == 200
    assert (
        await async_client.get("/engagements/6017/console/participants", headers=headers)
    ).status_code == 200


@pytest.mark.asyncio
async def test_console_routes_org_manager_assigned_wrong_org(async_client, test_db_session):
    other_contact_user_id = 9199
    test_db_session.add(
        User(user_id=other_contact_user_id, age=30, phone="919900000000", status="active")
    )
    await test_db_session.flush()
    await _seed_org_manager_with_engagement(
        test_db_session,
        manager_user_id=9114,
        employee_id=214,
        organization_id=9403,
        engagement_id=6018,
        status="running",
        contact_person_user_id=other_contact_user_id,
    )
    await _assign_assistant(test_db_session, assignment_id=9, employee_id=214, engagement_id=6018)

    headers = _auth_header(9114)

    assert (await async_client.get("/engagements/6018/console", headers=headers)).status_code == 403
    assert (
        await async_client.get("/engagements/6018/console/participants", headers=headers)
    ).status_code == 403
    assert (await async_client.get("/engagements/console/engagements", headers=headers)).status_code == 200
    assert (await async_client.get("/engagements/console/engagements", headers=headers)).json()["data"] == []


@pytest.mark.asyncio
async def test_console_routes_org_manager_contact_person_not_assigned(async_client, test_db_session):
    await _seed_org_manager_with_engagement(
        test_db_session,
        manager_user_id=9115,
        employee_id=215,
        organization_id=9404,
        engagement_id=6019,
        status="running",
    )

    headers = _auth_header(9115)

    assert (await async_client.get("/engagements/6019/console", headers=headers)).status_code == 403
    assert (
        await async_client.get("/engagements/6019/console/participants", headers=headers)
    ).status_code == 403
    assert (await async_client.get("/engagements/console/engagements", headers=headers)).json()["data"] == []


@pytest.mark.asyncio
async def test_console_routes_org_manager_assigned_no_organization_id(async_client, test_db_session):
    await _seed_employee(test_db_session, user_id=9116, employee_id=216, role="organization_manager")
    await _seed_engagement(test_db_session, engagement_id=6020, status="running")
    await _assign_assistant(test_db_session, assignment_id=10, employee_id=216, engagement_id=6020)

    headers = _auth_header(9116)

    assert (await async_client.get("/engagements/6020/console", headers=headers)).status_code == 403
    assert (
        await async_client.get("/engagements/6020/console/participants", headers=headers)
    ).status_code == 403
