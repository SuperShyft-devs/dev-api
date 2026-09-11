"""Integration tests for engagement onboarding assistant assignment routes (phlebo partners)."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import select

from modules.assessments.models import AssessmentPackage
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import Engagement, OnboardingAssistantAssignment
from modules.organizations.models import Organization
from modules.partners.models import Partner
from modules.users.models import User
from tests.helpers.auth import (
    employee_auth_header,
    seed_employee,
    seed_partner,
    user_auth_header,
)
from tests.helpers.engagement_types import engagement_type_id


async def _ensure_packages(test_db_session) -> None:
    """Ensure diagnostic and assessment packages exist for engagement FKs."""
    if await test_db_session.get(DiagnosticPackage, 1) is None:
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
    if await test_db_session.get(AssessmentPackage, 1) is None:
        test_db_session.add(
            AssessmentPackage(
                package_id=1,
                package_code="PKG001",
                display_name="Test Package 1",
                status="active",
            )
        )
    await test_db_session.flush()


async def _seed_admin(test_db_session, *, employee_id: int) -> None:
    await seed_employee(test_db_session, employee_id=employee_id, role="admin")


async def _seed_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    organization_id: int | None = None,
) -> None:
    await _ensure_packages(test_db_session)
    type_id = await engagement_type_id(test_db_session, "bio_ai")
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Test Engagement",
            engagement_code=engagement_code,
            engagement_type=type_id,
            assessment_package_id=1,
            diagnostic_package_id=1,
            organization_id=organization_id,
            status="running",
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()


# ============================================================================
# GET /engagements/{engagement_id}/onboarding-assistants - List Tests
# ============================================================================


@pytest.mark.asyncio
async def test_list_onboarding_assistants_requires_auth(async_client):
    """Listing onboarding assistants requires authentication."""
    response = await async_client.get("/engagements/1/onboarding-assistants")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_onboarding_assistants_requires_employee(async_client, test_db_session):
    """Listing onboarding assistants requires an employee JWT."""
    test_db_session.add(User(user_id=9001, age=30, phone="9001000000", status="active"))
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/1/onboarding-assistants",
        headers=user_auth_header(9001),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_empty_list(async_client, test_db_session):
    """Listing returns an empty list when no phlebo partners are assigned."""
    await _seed_admin(test_db_session, employee_id=101)
    await _seed_engagement(test_db_session, engagement_id=5001, engagement_code="ENG001")

    response = await async_client.get(
        "/engagements/5001/onboarding-assistants",
        headers=employee_auth_header(101),
    )
    assert response.status_code == 200

    body = response.json()["data"]
    assert isinstance(body, list)
    assert len(body) == 0


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_assigned_partners(async_client, test_db_session):
    """Listing returns all assigned phlebo partners with contact fields."""
    await _seed_admin(test_db_session, employee_id=102)
    await seed_partner(
        test_db_session,
        partner_id=201,
        role="phlebo",
        name="Phlebo One",
        phone="8800200001",
        commit=False,
    )
    await seed_partner(
        test_db_session,
        partner_id=202,
        role="phlebo",
        name="Phlebo Two",
        phone="8800200002",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5002, engagement_code="ENG002")

    test_db_session.add(
        OnboardingAssistantAssignment(onboarding_assistant_id=1, partner_id=201, engagement_id=5002)
    )
    test_db_session.add(
        OnboardingAssistantAssignment(onboarding_assistant_id=2, partner_id=202, engagement_id=5002)
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/5002/onboarding-assistants",
        headers=employee_auth_header(102),
    )
    assert response.status_code == 200

    body = response.json()["data"]
    assert isinstance(body, list)
    assert len(body) == 2
    assert {row["partner_id"] for row in body} == {201, 202}
    for row in body:
        assert row["role"] == "phlebo"
        assert "name" in row
        assert "phone" in row
        assert "email" in row
        assert "status" in row


@pytest.mark.asyncio
async def test_list_onboarding_assistants_returns_404_when_engagement_missing(async_client, test_db_session):
    """Listing returns 404 when the engagement does not exist."""
    await _seed_admin(test_db_session, employee_id=103)

    response = await async_client.get(
        "/engagements/999999/onboarding-assistants",
        headers=employee_auth_header(103),
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "ENGAGEMENT_NOT_FOUND", "message": "Engagement does not exist"}


# ============================================================================
# POST /engagements/{engagement_id}/onboarding-assistants - Add Tests
# ============================================================================


@pytest.mark.asyncio
async def test_add_onboarding_assistants_requires_auth(async_client):
    """Adding onboarding assistants requires authentication."""
    response = await async_client.post("/engagements/1/onboarding-assistants", json={"partner_ids": [1]})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_add_onboarding_assistants_requires_employee(async_client, test_db_session):
    """Adding onboarding assistants requires an employee JWT."""
    test_db_session.add(User(user_id=9007, age=30, phone="9007000000", status="active"))
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/1/onboarding-assistants",
        headers=user_auth_header(9007),
        json={"partner_ids": [1]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_add_onboarding_assistants_creates_assignment(async_client, test_db_session):
    """Adding phlebo partners creates assignment records."""
    await _seed_admin(test_db_session, employee_id=106)
    await seed_partner(test_db_session, partner_id=301, role="phlebo", phone="8800300001")
    await _seed_engagement(test_db_session, engagement_id=5003, engagement_code="ENG003")

    response = await async_client.post(
        "/engagements/5003/onboarding-assistants",
        headers=employee_auth_header(106),
        json={"partner_ids": [301]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert data["engagement_id"] == 5003
    assert data["added_partner_ids"] == [301]
    assert data["skipped_partner_ids"] == []

    result = await test_db_session.execute(
        OnboardingAssistantAssignment.__table__.select().where(
            OnboardingAssistantAssignment.engagement_id == 5003
        )
    )
    rows = list(result.all())
    assert len(rows) == 1
    assert rows[0].partner_id == 301


@pytest.mark.asyncio
async def test_add_onboarding_assistants_allows_expert_partner(async_client, test_db_session):
    """Expert partners can be assigned for camp/consult workflows."""
    await _seed_admin(test_db_session, employee_id=113)
    await seed_partner(test_db_session, partner_id=302, role="expert", phone="8800300002")
    await _seed_engagement(test_db_session, engagement_id=5005, engagement_code="ENG005")

    response = await async_client.post(
        "/engagements/5005/onboarding-assistants",
        headers=employee_auth_header(113),
        json={"partner_ids": [302]},
    )
    assert response.status_code == 201
    data = response.json()["data"]
    assert data["added_partner_ids"] == [302]


@pytest.mark.asyncio
async def test_add_onboarding_assistants_skips_duplicates(async_client, test_db_session):
    """Duplicate partner assignments are skipped."""
    await _seed_admin(test_db_session, employee_id=108)
    await seed_partner(test_db_session, partner_id=303, role="phlebo", phone="8800300003")
    await _seed_engagement(test_db_session, engagement_id=5004, engagement_code="ENG004")

    test_db_session.add(
        OnboardingAssistantAssignment(onboarding_assistant_id=10, partner_id=303, engagement_id=5004)
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5004/onboarding-assistants",
        headers=employee_auth_header(108),
        json={"partner_ids": [303, 303]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert data["added_partner_ids"] == []
    assert data["skipped_partner_ids"] == [303]


@pytest.mark.asyncio
async def test_add_onboarding_assistants_handles_multiple(async_client, test_db_session):
    """Multiple phlebo partners can be assigned in one request."""
    await _seed_admin(test_db_session, employee_id=110)
    await seed_partner(test_db_session, partner_id=304, role="phlebo", phone="8800300004", commit=False)
    await seed_partner(test_db_session, partner_id=305, role="phlebo", phone="8800300005")
    await _seed_engagement(test_db_session, engagement_id=5006, engagement_code="ENG006A")

    response = await async_client.post(
        "/engagements/5006/onboarding-assistants",
        headers=employee_auth_header(110),
        json={"partner_ids": [304, 305]},
    )
    assert response.status_code == 201

    data = response.json()["data"]
    assert set(data["added_partner_ids"]) == {304, 305}
    assert data["skipped_partner_ids"] == []


@pytest.mark.asyncio
async def test_add_onboarding_assistants_returns_404_when_partner_missing(async_client, test_db_session):
    """Adding returns 404 when a partner does not exist."""
    await _seed_admin(test_db_session, employee_id=111)
    await _seed_engagement(test_db_session, engagement_id=5013, engagement_code="ENG013")

    response = await async_client.post(
        "/engagements/5013/onboarding-assistants",
        headers=employee_auth_header(111),
        json={"partner_ids": [999999]},
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "PARTNER_NOT_FOUND", "message": "Partner does not exist"}


@pytest.mark.asyncio
async def test_add_onboarding_assistants_returns_404_when_engagement_missing(async_client, test_db_session):
    """Adding returns 404 when the engagement does not exist."""
    await _seed_admin(test_db_session, employee_id=114)
    await seed_partner(test_db_session, partner_id=306, role="phlebo", phone="8800300006")

    response = await async_client.post(
        "/engagements/999999/onboarding-assistants",
        headers=employee_auth_header(114),
        json={"partner_ids": [306]},
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "ENGAGEMENT_NOT_FOUND", "message": "Engagement does not exist"}


@pytest.mark.asyncio
async def test_add_onboarding_assistants_validates_empty_list(async_client, test_db_session):
    """Adding validates a non-empty partner_ids list."""
    await _seed_admin(test_db_session, employee_id=115)
    await _seed_engagement(test_db_session, engagement_id=5007, engagement_code="ENG007")

    response = await async_client.post(
        "/engagements/5007/onboarding-assistants",
        headers=employee_auth_header(115),
        json={"partner_ids": []},
    )
    assert response.status_code == 400


# ============================================================================
# DELETE /engagements/{engagement_id}/onboarding-assistants/{partner_id} - Remove Tests
# ============================================================================


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_requires_auth(async_client):
    """Removing an onboarding assistant requires authentication."""
    response = await async_client.delete("/engagements/1/onboarding-assistants/1")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_requires_employee(async_client, test_db_session):
    """Removing an onboarding assistant requires an employee JWT."""
    test_db_session.add(User(user_id=9018, age=30, phone="9018000000", status="active"))
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/1/onboarding-assistants/1",
        headers=user_auth_header(9018),
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_deletes_assignment(async_client, test_db_session):
    """Removing a partner deletes the assignment record."""
    await _seed_admin(test_db_session, employee_id=116)
    await seed_partner(test_db_session, partner_id=307, role="phlebo", phone="8800300007")
    await _seed_engagement(test_db_session, engagement_id=5008, engagement_code="ENG008")

    test_db_session.add(
        OnboardingAssistantAssignment(onboarding_assistant_id=20, partner_id=307, engagement_id=5008)
    )
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/5008/onboarding-assistants/307",
        headers=employee_auth_header(116),
    )
    assert response.status_code == 200

    data = response.json()["data"]
    assert data == {
        "engagement_id": 5008,
        "removed_partner_id": 307,
        "removed_employee_id": None,
    }

    link = await test_db_session.execute(
        OnboardingAssistantAssignment.__table__.select().where(
            (OnboardingAssistantAssignment.engagement_id == 5008)
            & (OnboardingAssistantAssignment.partner_id == 307)
        )
    )
    assert link.first() is None


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_returns_404_when_assignment_missing(async_client, test_db_session):
    """Removing returns 404 when the partner is not assigned."""
    await _seed_admin(test_db_session, employee_id=118)
    await _seed_engagement(test_db_session, engagement_id=5009, engagement_code="ENG009")

    response = await async_client.delete(
        "/engagements/5009/onboarding-assistants/999999",
        headers=employee_auth_header(118),
    )
    assert response.status_code == 404
    assert response.json() == {
        "error_code": "ONBOARDING_ASSISTANT_ASSIGNMENT_NOT_FOUND",
        "message": "Partner is not assigned to this engagement",
    }


@pytest.mark.asyncio
async def test_remove_onboarding_assistant_returns_404_when_engagement_missing(async_client, test_db_session):
    """Removing returns 404 when the engagement does not exist."""
    await _seed_admin(test_db_session, employee_id=119)

    response = await async_client.delete(
        "/engagements/999999/onboarding-assistants/119",
        headers=employee_auth_header(119),
    )
    assert response.status_code == 404
    assert response.json() == {"error_code": "ENGAGEMENT_NOT_FOUND", "message": "Engagement does not exist"}


@pytest.mark.asyncio
async def test_onboarding_assistant_routes_require_admin(async_client, test_db_session):
    """Non-admin employees cannot manage onboarding assistant assignments."""
    await _seed_admin(test_db_session, employee_id=120)
    await seed_employee(test_db_session, employee_id=121, role="inferior_admin")
    await seed_partner(test_db_session, partner_id=308, role="phlebo", phone="8800300008")
    await _seed_engagement(test_db_session, engagement_id=5010, engagement_code="ENG010")

    headers = employee_auth_header(121)

    assert (
        await async_client.get("/engagements/5010/onboarding-assistants", headers=headers)
    ).status_code == 403
    assert (
        await async_client.post(
            "/engagements/5010/onboarding-assistants",
            headers=headers,
            json={"partner_ids": [308]},
        )
    ).status_code == 403
    assert (
        await async_client.delete("/engagements/5010/onboarding-assistants/308", headers=headers)
    ).status_code == 403


@pytest.mark.asyncio
async def test_add_onboarding_assistants_accepts_phlebo_partner_on_org_engagement(
    async_client, test_db_session
):
    """Phlebo partners can be assigned to organization engagements."""
    await _seed_admin(test_db_session, employee_id=122)
    await seed_partner(test_db_session, partner_id=123, role="organization_manager", phone="8800300123")
    await seed_partner(test_db_session, partner_id=309, role="phlebo", phone="8800300009")
    await _ensure_packages(test_db_session)
    type_id = await engagement_type_id(test_db_session, "bio_ai")

    test_db_session.add(
        Organization(
            organization_id=9501,
            name="Assignable Org",
            organization_type="corporate",
            status="active",
            contact_person_user_ids={"organization_managers": [123]},
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=5011,
            engagement_name="Org Engagement",
            engagement_code="ENG011",
            engagement_type=type_id,
            assessment_package_id=1,
            diagnostic_package_id=1,
            organization_id=9501,
            status="running",
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5011/onboarding-assistants",
        headers=employee_auth_header(122),
        json={"partner_ids": [309]},
    )
    assert response.status_code == 201
    assert response.json()["data"]["added_partner_ids"] == [309]


@pytest.mark.asyncio
async def test_add_onboarding_assistants_allows_expert_partner_on_org_engagement(
    async_client, test_db_session
):
    """Expert partners can be assigned on organization engagements."""
    await _seed_admin(test_db_session, employee_id=124)
    await seed_partner(test_db_session, partner_id=310, role="expert", phone="8800300010")
    await _ensure_packages(test_db_session)
    type_id = await engagement_type_id(test_db_session, "bio_ai")

    test_db_session.add(
        Organization(
            organization_id=9502,
            name="Other Org",
            organization_type="corporate",
            status="active",
            contact_person_user_ids={"organization_managers": [124]},
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=5012,
            engagement_name="Org Engagement 2",
            engagement_code="ENG012",
            engagement_type=type_id,
            assessment_package_id=1,
            diagnostic_package_id=1,
            organization_id=9502,
            status="running",
            start_date=date.today(),
            end_date=date.today(),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5012/onboarding-assistants",
        headers=employee_auth_header(124),
        json={"partner_ids": [310]},
    )
    assert response.status_code == 201
    assert response.json()["data"]["added_partner_ids"] == [310]


# ============================================================================
# POST /engagements/{engagement_id}/onboarding-assistants/create-phlebo
# ============================================================================


@pytest.mark.asyncio
async def test_create_phlebo_requires_auth(async_client):
    response = await async_client.post(
        "/engagements/1/onboarding-assistants/create-phlebo",
        json={"name": "Test Phlebo", "phone": "8800100001"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_phlebo_requires_admin(async_client, test_db_session):
    await seed_employee(test_db_session, employee_id=200, role="organization_manager")
    await _seed_engagement(test_db_session, engagement_id=5020, engagement_code="ENG020")

    response = await async_client.post(
        "/engagements/5020/onboarding-assistants/create-phlebo",
        headers=employee_auth_header(200),
        json={"name": "Test Phlebo", "phone": "8800100001"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_create_phlebo_creates_partner_and_assignment(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=201)
    await _seed_engagement(test_db_session, engagement_id=5021, engagement_code="ENG021")

    response = await async_client.post(
        "/engagements/5021/onboarding-assistants/create-phlebo",
        headers=employee_auth_header(201),
        json={"name": "New Phlebo", "phone": "8800100001"},
    )
    assert response.status_code == 201

    body = response.json()["data"]
    assert body["partner_created"] is True
    assert body["role"] == "phlebo"
    assert body["name"] == "New Phlebo"
    assert body["added_partner_ids"] == [body["partner_id"]]
    assert body["skipped_partner_ids"] == []

    created_partner = await test_db_session.get(Partner, body["partner_id"])
    assert created_partner is not None
    assert created_partner.role == "phlebo"
    assert created_partner.name == "New Phlebo"

    assignment = await test_db_session.execute(
        select(OnboardingAssistantAssignment).where(
            OnboardingAssistantAssignment.engagement_id == 5021,
            OnboardingAssistantAssignment.partner_id == body["partner_id"],
        )
    )
    assert assignment.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_create_phlebo_existing_phone_returns_confirmation(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=202)
    await _seed_engagement(test_db_session, engagement_id=5022, engagement_code="ENG022")
    await seed_partner(
        test_db_session,
        partner_id=401,
        role="phlebo",
        name="Existing Phlebo",
        phone="8801020001",
    )

    response = await async_client.post(
        "/engagements/5022/onboarding-assistants/create-phlebo",
        headers=employee_auth_header(202),
        json={"name": "Ignored Name", "phone": "8801020001"},
    )
    assert response.status_code == 201

    body = response.json()["data"]
    assert body["status"] == "confirmation_required"
    assert body["existing_partner"]["partner_id"] == 401
    assert body["existing_partner"]["phone"] == "8801020001"
    assert body["existing_partner"]["role"] == "phlebo"


@pytest.mark.asyncio
async def test_create_phlebo_confirm_existing_assigns_existing_phlebo(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=203)
    await _seed_engagement(test_db_session, engagement_id=5023, engagement_code="ENG023")
    await seed_partner(
        test_db_session,
        partner_id=402,
        role="phlebo",
        name="Unassigned Phlebo",
        phone="8801030001",
    )

    response = await async_client.post(
        "/engagements/5023/onboarding-assistants/create-phlebo",
        headers=employee_auth_header(203),
        json={"name": "Unassigned Phlebo", "phone": "8801030001", "confirm_existing": True},
    )
    assert response.status_code == 201

    body = response.json()["data"]
    assert body["partner_created"] is False
    assert body["partner_id"] == 402
    assert body["added_partner_ids"] == [402]

    assignment = await test_db_session.execute(
        select(OnboardingAssistantAssignment).where(
            OnboardingAssistantAssignment.engagement_id == 5023,
            OnboardingAssistantAssignment.partner_id == 402,
        )
    )
    assert assignment.scalar_one_or_none() is not None


@pytest.mark.asyncio
async def test_create_phlebo_confirm_existing_rejects_expert_partner(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=204)
    await _seed_engagement(test_db_session, engagement_id=5024, engagement_code="ENG024")
    await seed_partner(
        test_db_session,
        partner_id=403,
        role="expert",
        name="Expert Partner",
        phone="8801040001",
    )

    response = await async_client.post(
        "/engagements/5024/onboarding-assistants/create-phlebo",
        headers=employee_auth_header(204),
        json={"name": "Expert Partner", "phone": "8801040001", "confirm_existing": True},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_create_phlebo_confirm_existing_skips_when_already_assigned(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=206)
    await seed_partner(
        test_db_session,
        partner_id=404,
        role="phlebo",
        name="Already Assigned",
        phone="8801050001",
    )
    await _seed_engagement(test_db_session, engagement_id=5025, engagement_code="ENG025")

    test_db_session.add(
        OnboardingAssistantAssignment(onboarding_assistant_id=99, partner_id=404, engagement_id=5025)
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/engagements/5025/onboarding-assistants/create-phlebo",
        headers=employee_auth_header(206),
        json={"name": "Already Assigned", "phone": "8801050001", "confirm_existing": True},
    )
    assert response.status_code == 201

    body = response.json()["data"]
    assert body["partner_id"] == 404
    assert body["added_partner_ids"] == []
    assert body["skipped_partner_ids"] == [404]


# ============================================================================
# Mixed employee + expert assignment
# ============================================================================


@pytest.mark.asyncio
async def test_add_employee_assignee_admin(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=301)
    await seed_employee(
        test_db_session,
        employee_id=302,
        role="admin",
        name="Staff Admin",
        phone="8802000001",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5101, engagement_code="ENG101")

    response = await async_client.post(
        "/engagements/5101/onboarding-assistants",
        headers=employee_auth_header(301),
        json={"employee_ids": [302]},
    )
    assert response.status_code == 201
    data = response.json()["data"]
    assert data["added_employee_ids"] == [302]
    assert data["added_partner_ids"] == []

    listed = await async_client.get(
        "/engagements/5101/onboarding-assistants",
        headers=employee_auth_header(301),
    )
    assert listed.status_code == 200
    rows = listed.json()["data"]
    assert len(rows) == 1
    assert rows[0]["kind"] == "employee"
    assert rows[0]["employee_id"] == 302
    assert rows[0]["role"] == "admin"


@pytest.mark.asyncio
async def test_add_employee_assignee_inferior_admin(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=303)
    await seed_employee(
        test_db_session,
        employee_id=304,
        role="inferior_admin",
        name="Inferior",
        phone="8802000002",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5102, engagement_code="ENG102")

    response = await async_client.post(
        "/engagements/5102/onboarding-assistants",
        headers=employee_auth_header(303),
        json={"employee_ids": [304]},
    )
    assert response.status_code == 201
    assert response.json()["data"]["added_employee_ids"] == [304]


@pytest.mark.asyncio
async def test_add_employee_rejects_organization_manager(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=305)
    await seed_employee(
        test_db_session,
        employee_id=306,
        role="organization_manager",
        name="Org Mgr",
        phone="8802000003",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5103, engagement_code="ENG103")

    response = await async_client.post(
        "/engagements/5103/onboarding-assistants",
        headers=employee_auth_header(305),
        json={"employee_ids": [306]},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_add_partner_rejects_organization_manager(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=315)
    await seed_partner(
        test_db_session,
        partner_id=316,
        role="organization_manager",
        phone="8802000316",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5113, engagement_code="ENG113")

    response = await async_client.post(
        "/engagements/5113/onboarding-assistants",
        headers=employee_auth_header(315),
        json={"partner_ids": [316]},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_remove_employee_assignee(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=307)
    await seed_employee(
        test_db_session,
        employee_id=308,
        role="admin",
        name="Removable",
        phone="8802000004",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5104, engagement_code="ENG104")
    test_db_session.add(
        OnboardingAssistantAssignment(
            onboarding_assistant_id=501,
            engagement_id=5104,
            partner_id=None,
            employee_id=308,
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        "/engagements/5104/onboarding-assistants/employees/308",
        headers=employee_auth_header(307),
    )
    assert response.status_code == 200
    assert response.json()["data"]["removed_employee_id"] == 308

    listed = await async_client.get(
        "/engagements/5104/onboarding-assistants",
        headers=employee_auth_header(307),
    )
    assert listed.json()["data"] == []


@pytest.mark.asyncio
async def test_list_mixed_partner_and_employee_assignees(async_client, test_db_session):
    await _seed_admin(test_db_session, employee_id=309)
    await seed_partner(
        test_db_session, partner_id=501, role="phlebo", phone="8802000010", commit=False
    )
    await seed_partner(
        test_db_session, partner_id=502, role="expert", phone="8802000011", commit=False
    )
    await seed_employee(
        test_db_session,
        employee_id=310,
        role="inferior_admin",
        name="Listed IA",
        phone="8802000012",
        commit=False,
    )
    await _seed_engagement(test_db_session, engagement_id=5105, engagement_code="ENG105")
    test_db_session.add_all(
        [
            OnboardingAssistantAssignment(
                onboarding_assistant_id=601, engagement_id=5105, partner_id=501
            ),
            OnboardingAssistantAssignment(
                onboarding_assistant_id=602, engagement_id=5105, partner_id=502
            ),
            OnboardingAssistantAssignment(
                onboarding_assistant_id=603,
                engagement_id=5105,
                partner_id=None,
                employee_id=310,
            ),
        ]
    )
    await test_db_session.commit()

    response = await async_client.get(
        "/engagements/5105/onboarding-assistants",
        headers=employee_auth_header(309),
    )
    assert response.status_code == 200
    rows = response.json()["data"]
    assert len(rows) == 3
    kinds = {(r.get("kind"), r.get("partner_id"), r.get("employee_id"), r.get("role")) for r in rows}
    assert ("partner", 501, None, "phlebo") in kinds
    assert ("partner", 502, None, "expert") in kinds
    assert ("employee", None, 310, "inferior_admin") in kinds
