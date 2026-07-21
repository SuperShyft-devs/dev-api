"""Integration tests for internal employee users routes."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest
from sqlalchemy import select, text

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.auth.models import AuthOtpSession, AuthToken
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementParticipant, OnboardingAssistantAssignment
from modules.notifications.models import Notification
from modules.organizations.models import Organization
from modules.payments.models import Booking, Order, Payment
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireDefinition, QuestionnaireResponse
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
from modules.support.models import SupportTicket
from modules.users import service as users_service_module
from modules.users.models import User, UserPreference


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_notification_service(test_db_session, service_key: str = "booking-alert-whatsapp") -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, "
            "require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'booking-alert', true, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": service_key, "dn": service_key},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_create_user_is_public_without_auth(async_client, test_db_session):
    # Avoid 1234567890 (often already present); teardown deletes by phone for high user_id rows.
    phone = "8877665501"
    response = await async_client.post("/users", json={"phone": phone, "age": 25})
    assert response.status_code == 200
    user_id = response.json()["data"]["user_id"]
    created = await test_db_session.get(User, user_id)
    assert created is not None
    assert created.phone == phone


@pytest.mark.asyncio
async def test_create_user_does_not_require_employee(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9001, phone="9001000000", status="active"))
    await test_db_session.commit()

    payload = {"phone": "1234567891", "first_name": "Public", "age": 28}
    response = await async_client.post("/users", headers=_auth_header(9001), json=payload)
    assert response.status_code == 200
    user_id = response.json()["data"]["user_id"]
    created = await test_db_session.get(User, user_id)
    assert created is not None
    assert created.first_name == "Public"


@pytest.mark.asyncio
async def test_employee_create_user_creates_user(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9002, phone="9002000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10001, user_id=9002, role="admin", status="active"))
    await test_db_session.commit()

    payload = {"phone": "5550000998", "first_name": "A", "status": "active", "age": 22}
    response = await async_client.post("/users", headers=_auth_header(9002), json=payload)

    assert response.status_code == 200
    user_id = response.json()["data"]["user_id"]
    assert isinstance(user_id, int)

    created = await test_db_session.get(User, user_id)
    assert created is not None
    assert created.phone == "5550000998"


@pytest.mark.asyncio
async def test_employee_list_users_paginates_and_filters(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9003, phone="9003000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10002, user_id=9003, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=9101, phone="9101000000", status="active", city="Pune"))
    test_db_session.add(User(age=30, user_id=9102, phone="9102000000", status="inactive", city="Pune"))
    await test_db_session.commit()

    response = await async_client.get("/users?page=1&limit=10&status=active", headers=_auth_header(9003))
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["page"] == 1
    assert body["meta"]["limit"] == 10
    assert body["meta"]["total"] >= 1

    for row in body["data"]:
        assert row["status"] == "active"


@pytest.mark.asyncio
async def test_employee_get_user_returns_details(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9004, phone="9004000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10003, user_id=9004, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=9201, phone="9201000000", status="active", first_name="X"))
    await test_db_session.commit()

    response = await async_client.get("/users/9201", headers=_auth_header(9004))
    assert response.status_code == 200
    assert response.json()["data"]["user_id"] == 9201
    assert response.json()["data"]["first_name"] == "X"


@pytest.mark.asyncio
async def test_employee_update_user_updates_fields(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9005, phone="9005000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10004, user_id=9005, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=9301, phone="9301000000", status="active", first_name="Old"))
    await test_db_session.commit()

    payload = {"phone": "9301000000", "first_name": "New", "status": "active", "age": 30}
    response = await async_client.put("/users/9301", headers=_auth_header(9005), json=payload)
    assert response.status_code == 200

    updated = await test_db_session.get(User, 9301)
    assert updated is not None
    assert updated.first_name == "New"


@pytest.mark.asyncio
async def test_employee_update_metsights_profile_id(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9005, phone="9005000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10004, user_id=9005, role="admin", status="active"))

    test_db_session.add(
        User(age=30, user_id=9302, phone="9302000000", status="active", metsights_profile_id=None)
    )
    await test_db_session.commit()

    response = await async_client.put(
        "/users/9302/metsights-profile-id",
        headers=_auth_header(9005),
        json={"metsights_profile_id": "ms-profile-9302"},
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["user_id"] == 9302
    assert body["metsights_profile_id"] == "ms-profile-9302"

    clear = await async_client.put(
        "/users/9302/metsights-profile-id",
        headers=_auth_header(9005),
        json={"metsights_profile_id": ""},
    )
    assert clear.status_code == 200
    assert clear.json()["data"]["metsights_profile_id"] is None

    row = await test_db_session.get(User, 9302)
    assert row is not None
    assert row.metsights_profile_id is None


@pytest.mark.asyncio
async def test_employee_update_metsights_profile_id_rejects_duplicate(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9005, phone="9005000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10004, user_id=9005, role="admin", status="active"))

    test_db_session.add(
        User(age=30, user_id=9303, phone="9303000000", status="active", metsights_profile_id="taken-ms-id")
    )
    test_db_session.add(
        User(age=30, user_id=9304, phone="9304000000", status="active", metsights_profile_id=None)
    )
    await test_db_session.commit()

    response = await async_client.put(
        "/users/9304/metsights-profile-id",
        headers=_auth_header(9005),
        json={"metsights_profile_id": "taken-ms-id"},
    )
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_employee_deactivate_user_sets_inactive(async_client, test_db_session):
    test_db_session.add(User(age=30, user_id=9006, phone="9006000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10005, user_id=9006, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=9401, phone="9401000000", status="active"))
    await test_db_session.commit()

    response = await async_client.patch("/users/9401/deactivate", headers=_auth_header(9006))
    assert response.status_code == 200

    updated = await test_db_session.get(User, 9401)
    assert updated is not None
    assert (updated.status or "").lower() == "inactive"


@pytest.mark.asyncio
async def test_employee_update_user_rejects_inactive_for_employee_one_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(users_service_module, "_ALWAYS_ACTIVE_EMPLOYEE_ID", 10007)
    test_db_session.add(User(age=30, user_id=9007, phone="9007000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10006, user_id=9007, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=9411, phone="9411000000", status="active", first_name="Rishi"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10007, user_id=9411, role="admin", status="active"))
    await test_db_session.commit()

    payload = {"phone": "9411000000", "first_name": "Rishi", "status": "inactive"}
    response = await async_client.put("/users/9411", headers=_auth_header(9007), json=payload)
    assert response.status_code == 400

    protected_user = await test_db_session.get(User, 9411)
    assert protected_user is not None
    assert (protected_user.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_employee_deactivate_user_rejects_employee_one_user(async_client, test_db_session, monkeypatch):
    monkeypatch.setattr(users_service_module, "_ALWAYS_ACTIVE_EMPLOYEE_ID", 10009)
    test_db_session.add(User(age=30, user_id=9008, phone="9008000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10008, user_id=9008, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=9412, phone="9412000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=10009, user_id=9412, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.patch("/users/9412/deactivate", headers=_auth_header(9008))
    assert response.status_code == 400

    protected_user = await test_db_session.get(User, 9412)
    assert protected_user is not None
    assert (protected_user.status or "").lower() == "active"


@pytest.mark.asyncio
async def test_employee_delete_user_cascades_related_data(async_client, test_db_session):
    # High ids avoid collisions with stale rows from older test runs on shared DBs.
    actor_user_id = 99010
    target_user_id = 99051

    test_db_session.add(User(age=30, user_id=actor_user_id, phone="9901000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19010, user_id=actor_user_id, role="admin", status="active"))

    target = User(age=31, user_id=target_user_id, phone="9905100000", status="active")
    test_db_session.add(target)

    test_db_session.add(
        Engagement(
            engagement_id=9801,
            engagement_code="ENG9801",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
        )
    )
    await test_db_session.commit()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=9901,
            engagement_id=9801,
            user_id=target_user_id,
            engagement_date=date(2026, 1, 1),
            slot_start_time=time(10, 0),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=9951,
            user_id=target_user_id,
            package_id=1,
            engagement_id=9801,
            status="assigned",
        )
    )
    test_db_session.add(QuestionnaireCategory(category_id=9961, category_key="lifestyle", display_name="Lifestyle"))
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=9962,
            question_key="sleep_quality",
            question_text="How do you sleep?",
            question_type="single_select",
            status="active",
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        QuestionnaireResponse(
            response_id=9963,
            assessment_instance_id=9951,
            question_id=9962,
            category_id=9961,
            answer={"value": "good"},
        )
    )
    test_db_session.add(
        AssessmentCategoryProgress(
            id=9964,
            assessment_instance_id=9951,
            category_id=9961,
            status="completed",
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=9965,
            user_id=target_user_id,
            assessment_instance_id=9951,
            engagement_id=9801,
            reports={},
            blood_parameters={},
        )
    )
    test_db_session.add(
        ReportsUserSyncState(
            sync_id=9966,
            user_id=target_user_id,
            sync_status="idle",
        )
    )
    test_db_session.add(
        AuthOtpSession(
            session_id=9967,
            user_id=target_user_id,
            otp_hash="hash",
            otp_expires_at=datetime(2099, 1, 1, tzinfo=timezone.utc),
            created_at=datetime(2099, 1, 1, tzinfo=timezone.utc),
        )
    )
    test_db_session.add(
        AuthToken(
            token_id=9968,
            user_id=target_user_id,
            refresh_token_hash="hash",
            issued_at=datetime(2099, 1, 1, tzinfo=timezone.utc),
            expires_at=datetime(2099, 1, 2, tzinfo=timezone.utc),
        )
    )
    test_db_session.add(UserPreference(preference_id=9969, user_id=target_user_id))
    test_db_session.add(
        Booking(
            booking_id=9970,
            user_id=target_user_id,
            entity_type="diagnostic_package",
            entity_id=1,
            entity_name="Basic",
            amount_paise=10000,
            status="pending",
        )
    )
    # Order + payment owned by another user (checkout payer) but tied to this user's booking — must still delete.
    test_db_session.add(
        Order(
            order_id=9971,
            booking_id=9970,
            user_id=actor_user_id,
            razorpay_order_id="order_9971",
            amount_paise=10000,
            status="created",
        )
    )
    test_db_session.add(
        Payment(
            payment_id=9972,
            order_id=9971,
            booking_id=9970,
            user_id=actor_user_id,
            razorpay_order_id="order_9971",
            amount_paise=10000,
            status="created",
        )
    )
    test_db_session.add(
        SupportTicket(
            ticket_id=9973,
            user_id=target_user_id,
            contact_input="9501000000",
            query_text="Need help",
            status="open",
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        f"/users/{target_user_id}?delete_orphan_engagements=false",
        headers=_auth_header(actor_user_id),
    )
    assert response.status_code == 200
    assert response.json()["data"]["deleted_user_id"] == target_user_id

    assert await test_db_session.get(User, target_user_id) is None
    assert await test_db_session.get(Engagement, 9801) is not None
    assert await test_db_session.get(AssessmentInstance, 9951) is None
    assert await test_db_session.get(EngagementParticipant, 9901) is None
    assert await test_db_session.get(QuestionnaireResponse, 9963) is None
    assert await test_db_session.get(AssessmentCategoryProgress, 9964) is None
    assert await test_db_session.get(IndividualHealthReport, 9965) is None
    assert await test_db_session.get(ReportsUserSyncState, 9966) is None
    assert await test_db_session.get(AuthOtpSession, 9967) is None
    assert await test_db_session.get(AuthToken, 9968) is None
    assert await test_db_session.get(UserPreference, 9969) is None
    assert await test_db_session.get(Booking, 9970) is None
    assert await test_db_session.get(Order, 9971) is None
    assert await test_db_session.get(Payment, 9972) is None
    assert await test_db_session.get(SupportTicket, 9973) is None


@pytest.mark.asyncio
async def test_employee_delete_user_clears_notification_assessment_refs(async_client, test_db_session):
    """User delete must detach notifications referencing assessment instances (FK)."""
    actor_user_id = 99013
    target_user_id = 99054
    engagement_id = 9804
    assessment_instance_id = 9954
    notification_id = 9974

    await _seed_notification_service(test_db_session)

    test_db_session.add(User(age=30, user_id=actor_user_id, phone="9901300000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19013, user_id=actor_user_id, role="admin", status="active"))
    test_db_session.add(User(age=31, user_id=target_user_id, phone="9905400000", status="active"))
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code="ENG9804",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
        )
    )
    await test_db_session.commit()

    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=9904,
            engagement_id=engagement_id,
            user_id=target_user_id,
            engagement_date=date(2026, 1, 1),
            slot_start_time=time(10, 0),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_instance_id,
            user_id=target_user_id,
            package_id=1,
            engagement_id=engagement_id,
            status="assigned",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        Notification(
            notification_id=notification_id,
            service_key="booking-alert-whatsapp",
            status="sent",
            channel="whatsapp",
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
            triggered_by_user_id=actor_user_id,
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        f"/users/{target_user_id}?delete_orphan_engagements=true",
        headers=_auth_header(actor_user_id),
    )
    assert response.status_code == 200, response.text
    assert await test_db_session.get(User, target_user_id) is None
    assert await test_db_session.get(AssessmentInstance, assessment_instance_id) is None
    assert await test_db_session.get(Engagement, engagement_id) is None

    notif = await test_db_session.get(Notification, notification_id)
    assert notif is not None
    assert notif.assessment_instance_id is None
    assert notif.engagement_id is None


@pytest.mark.asyncio
async def test_employee_delete_user_impact_lists_engagements_fully_owned(async_client, test_db_session):
    actor_user_id = 99011
    target_user_id = 99052

    test_db_session.add(User(age=30, user_id=actor_user_id, phone="9901100000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19011, user_id=actor_user_id, role="admin", status="active"))
    test_db_session.add(User(age=31, user_id=target_user_id, phone="9905200000", status="active"))
    test_db_session.add(
        Engagement(
            engagement_id=9802,
            engagement_code="ENG9802",
            engagement_name="Solo Corp",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=9902,
            engagement_id=9802,
            user_id=target_user_id,
            engagement_date=date(2026, 1, 1),
            slot_start_time=time(10, 0),
        )
    )
    await test_db_session.commit()

    response = await async_client.get(
        f"/users/{target_user_id}/delete-impact",
        headers=_auth_header(actor_user_id),
    )
    assert response.status_code == 200
    orphans = response.json()["data"]["engagements_to_orphan"]
    assert len(orphans) == 1
    assert orphans[0]["engagement_id"] == 9802
    assert orphans[0]["engagement_code"] == "ENG9802"


@pytest.mark.asyncio
async def test_employee_delete_user_deletes_orphan_engagement_when_requested(async_client, test_db_session):
    actor_user_id = 99012
    target_user_id = 99053

    test_db_session.add(User(age=30, user_id=actor_user_id, phone="9901200000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19012, user_id=actor_user_id, role="admin", status="active"))
    test_db_session.add(User(age=31, user_id=target_user_id, phone="9905300000", status="active"))
    test_db_session.add(
        Engagement(
            engagement_id=9803,
            engagement_code="ENG9803",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        EngagementParticipant(
            engagement_participant_id=9903,
            engagement_id=9803,
            user_id=target_user_id,
            engagement_date=date(2026, 1, 1),
            slot_start_time=time(10, 0),
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(
        f"/users/{target_user_id}?delete_orphan_engagements=true",
        headers=_auth_header(actor_user_id),
    )
    assert response.status_code == 200
    assert await test_db_session.get(User, target_user_id) is None
    assert await test_db_session.get(Engagement, 9803) is None


@pytest.mark.asyncio
async def test_employee_delete_user_removes_employee_row_org_refs_and_onboarding(async_client, test_db_session):
    """Deleting a user who is also an employee clears employee FKs and the employee row."""
    actor_user_id = 99020
    target_user_id = 99071
    engagement_id = 9810
    org_id = 88110
    target_employee_id = 19701

    test_db_session.add(User(age=30, user_id=actor_user_id, phone="9902000000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19120, user_id=actor_user_id, role="admin", status="active"))

    test_db_session.add(User(age=30, user_id=target_user_id, phone="9907100000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=target_employee_id, user_id=target_user_id, role="ops", status="active"))

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_code="ENG9810",
            assessment_package_id=1,
            diagnostic_package_id=1,
            status="active",
        )
    )
    test_db_session.add(
        Organization(
            organization_id=org_id,
            name="DelEmpOrg",
            organization_type="corporate",
            status="active",
            bd_employee_id=target_employee_id,
            created_employee_id=target_employee_id,
            updated_employee_id=target_employee_id,
        )
    )
    test_db_session.add(
        OnboardingAssistantAssignment(
            onboarding_assistant_id=99801,
            employee_id=target_employee_id,
            engagement_id=engagement_id,
        )
    )
    await test_db_session.commit()

    response = await async_client.delete(f"/users/{target_user_id}", headers=_auth_header(actor_user_id))
    assert response.status_code == 200

    assert await test_db_session.get(User, target_user_id) is None
    assert await test_db_session.get(Employee, target_employee_id) is None

    remaining = await test_db_session.get(Organization, org_id)
    assert remaining is not None
    assert remaining.bd_employee_id is None
    assert remaining.created_employee_id is None
    assert remaining.updated_employee_id is None

    rows = (
        await test_db_session.execute(
            select(OnboardingAssistantAssignment).where(OnboardingAssistantAssignment.employee_id == target_employee_id)
        )
    ).scalars().all()
    assert rows == []


@pytest.mark.asyncio
async def test_employee_delete_user_rejects_self_delete(async_client, test_db_session):
    uid = 99202
    test_db_session.add(User(age=30, user_id=uid, phone="9920200000", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19202, user_id=uid, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.delete(f"/users/{uid}", headers=_auth_header(uid))
    assert response.status_code == 400
    assert "own account" in (response.json().get("message") or "").lower()


@pytest.mark.asyncio
async def test_employee_delete_user_rejects_when_actor_in_deleted_subtree(async_client, test_db_session):
    parent_id = 99230
    child_id = 99231
    test_db_session.add(User(age=30, user_id=parent_id, phone="9923000000", status="active"))
    test_db_session.add(
        User(age=30, user_id=child_id, phone="9923100000", status="active", parent_id=parent_id, relationship="child")
    )
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=19231, user_id=child_id, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.delete(f"/users/{parent_id}", headers=_auth_header(child_id))
    assert response.status_code == 400
    assert "family tree" in (response.json().get("message") or "").lower()
