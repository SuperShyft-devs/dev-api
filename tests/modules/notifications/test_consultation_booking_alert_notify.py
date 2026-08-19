"""Unit tests for consultation booking alert notifications."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text

from modules.employee.models import Employee, EmployeeRole
from modules.engagements.enums import ConsultationMode
from modules.engagements.models import Engagement, OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.experts.models import Expert
from modules.experts.repository import ExpertsRepository
from modules.notifications.consultation_booking_alert_notify import (
    notify_onboarding_assistants_on_consultation_booking,
    resolve_consultation_booking_alert_user_ids,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.users.models import User


SERVICE_KEY = "consultation-booking-alert-whatsapp"


async def _seed_notification_service(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, "
            "require_bio_ai_report_url, require_participant_detail, require_session_details) "
            "VALUES (:sk, :dn, 'whatsapp', 'consultation-booking-alert-whatsapp-v1', true, false, false, true, true) "
            "ON CONFLICT (service_key) DO UPDATE SET "
            "require_participant_detail = true, require_session_details = true, is_active = true"
        ),
        {"sk": SERVICE_KEY, "dn": SERVICE_KEY},
    )
    await test_db_session.commit()


async def _seed_diagnostic_package(test_db_session, diagnostic_package_id: int = 1) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, 'REF1', 'Diag', 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": diagnostic_package_id},
    )
    await test_db_session.commit()


async def _seed_event_and_binding(test_db_session, engagement_id: int) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('consultation', 'Consultation', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'consultation'")
        )
    ).one()
    type_id = int(type_row[0])
    await test_db_session.execute(
        text(
            "INSERT INTO auto_notification_events (event_code, display_name, engagement_type_ids, description) "
            "VALUES ('consultation_booking_alert', 'Consultation Booking Alert', :type_ids, 'Test event') "
            "ON CONFLICT (event_code) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"type_ids": [type_id]},
    )
    evt_row = (
        await test_db_session.execute(
            text("SELECT id FROM auto_notification_events WHERE event_code = 'consultation_booking_alert'")
        )
    ).one()
    evt_id = int(evt_row[0])
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_notifications (engagement_id, notification_event_id, notification_services) "
            "VALUES (:eid, :evt, CAST(:services AS jsonb)) "
            "ON CONFLICT (engagement_id, notification_event_id) DO UPDATE SET notification_services = EXCLUDED.notification_services"
        ),
        {
            "eid": engagement_id,
            "evt": evt_id,
            "services": f'[{{"service_key": "{SERVICE_KEY}", "external_link": null}}]',
        },
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_consultation_booking_alert_dispatches_with_session_details(test_db_session, monkeypatch):
    await _seed_notification_service(test_db_session)
    await _seed_diagnostic_package(test_db_session)

    engagement = Engagement(
        engagement_id=99101,
        engagement_name="Consult Alert Test",
        organization_id=None,
        engagement_code="CBAT01",
        engagement_type="consultation",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
        consultations={"doctor": True},
        consultation_mode=ConsultationMode.online,
    )
    test_db_session.add(engagement)
    await test_db_session.commit()
    await _seed_event_and_binding(test_db_session, 99101)

    participant = User(
        user_id=5101,
        age=30,
        phone="5101000000",
        first_name="Pat",
        last_name="User",
        email="pat@example.com",
        status="active",
    )
    test_db_session.add(participant)
    await test_db_session.commit()

    dispatch_mock = AsyncMock()
    notifications_service = NotificationsService(NotificationsRepository())
    monkeypatch.setattr(notifications_service, "dispatch", dispatch_mock)
    monkeypatch.setattr(
        "modules.notifications.consultation_booking_alert_notify.resolve_consultation_booking_alert_user_ids",
        AsyncMock(return_value=[201, 202]),
    )

    await notify_onboarding_assistants_on_consultation_booking(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=NotificationsRepository(),
        engagements_repository=EngagementsRepository(),
        engagement=engagement,
        participant_user=participant,
        participant_user_id=5101,
        expert_type="doctor",
        consultation_date=date(2026, 3, 15),
        consultation_slot="10:30",
    )

    dispatch_mock.assert_awaited_once()
    payload = dispatch_mock.await_args.kwargs["payload"]
    assert payload.service_key == SERVICE_KEY
    assert payload.user_ids == [201, 202]
    assert payload.engagement_id == 99101
    assert payload.participant_details["participant_user_id"] == "5101"
    assert payload.session_details is not None
    assert payload.session_details.want is True
    assert payload.session_details.date == date(2026, 3, 15)
    assert payload.session_details.slot == "10:30"
    assert payload.session_details.expert_type == "doctor"


@pytest.mark.asyncio
async def test_consultation_booking_alert_offline_uses_admin_and_expert_oa_roles(test_db_session):
    engagement = Engagement(
        engagement_id=99102,
        engagement_name="Roles Test",
        organization_id=100,
        engagement_code="CBAT02",
        engagement_type="consultation",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
        consultations={"doctor": True},
        consultation_mode=ConsultationMode.offline,
    )

    engagements_repo = EngagementsRepository()
    experts_repo = ExpertsRepository()
    from modules.employee.repository import EmployeeRepository

    async def list_side_effect(db, *, engagement_id, roles):
        assert engagement_id == 99102
        if roles == frozenset({EmployeeRole.admin}):
            return [301]
        if roles == frozenset({EmployeeRole.expert}):
            return [302]
        return []

    engagements_repo.list_onboarding_assistant_user_ids = AsyncMock(side_effect=list_side_effect)

    user_ids = await resolve_consultation_booking_alert_user_ids(
        test_db_session,
        engagement=engagement,
        expert_type="doctor",
        expert_id=None,
        engagements_repository=engagements_repo,
        experts_repository=experts_repo,
        employee_repository=EmployeeRepository(),
    )

    assert user_ids == [301, 302]
    assert engagements_repo.list_onboarding_assistant_user_ids.await_count == 2


@pytest.mark.asyncio
async def test_consultation_booking_alert_online_without_expert_id_includes_type_experts(test_db_session):
    engagement = Engagement(
        engagement_id=99104,
        engagement_name="Online All Experts",
        organization_id=None,
        engagement_code="CBAT04",
        engagement_type="consultation",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
        consultations={"doctor": True},
        consultation_mode=ConsultationMode.online,
    )

    engagements_repo = EngagementsRepository()
    experts_repo = ExpertsRepository()
    from modules.employee.repository import EmployeeRepository

    engagements_repo.list_onboarding_assistant_user_ids = AsyncMock(return_value=[301])
    experts_repo.list_active_user_ids_by_type = AsyncMock(return_value=[401, 402])

    user_ids = await resolve_consultation_booking_alert_user_ids(
        test_db_session,
        engagement=engagement,
        expert_type="doctor",
        expert_id=None,
        engagements_repository=engagements_repo,
        experts_repository=experts_repo,
        employee_repository=EmployeeRepository(),
    )

    assert user_ids == [301, 401, 402]
    experts_repo.list_active_user_ids_by_type.assert_awaited_once_with(
        test_db_session,
        expert_type="doctor",
    )


@pytest.mark.asyncio
async def test_consultation_booking_alert_online_with_expert_id_auto_assigns_oa(test_db_session):
    engagement_id = 99105
    engagement = Engagement(
        engagement_id=engagement_id,
        engagement_name="Online Expert Assign",
        organization_id=None,
        engagement_code="CBAT05",
        engagement_type="consultation",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
        consultations={"doctor": True},
        consultation_mode=ConsultationMode.online,
    )
    test_db_session.add(engagement)

    admin_user = User(user_id=5201, age=30, phone="5201000000", status="active")
    expert_user = User(user_id=5202, age=35, phone="5202000000", status="active")
    test_db_session.add(admin_user)
    test_db_session.add(expert_user)
    await test_db_session.flush()

    admin_employee = Employee(employee_id=5201, user_id=5201, role=EmployeeRole.admin, status="active")
    expert_employee = Employee(employee_id=5202, user_id=5202, role=EmployeeRole.expert, status="active")
    test_db_session.add(admin_employee)
    test_db_session.add(expert_employee)
    await test_db_session.flush()

    test_db_session.add(
        OnboardingAssistantAssignment(engagement_id=engagement_id, employee_id=5201)
    )
    test_db_session.add(
        Expert(
            expert_id=8801,
            user_id=5202,
            expert_type="doctor",
            specialization="General",
            status="active",
        )
    )
    await test_db_session.commit()

    from modules.employee.repository import EmployeeRepository

    user_ids = await resolve_consultation_booking_alert_user_ids(
        test_db_session,
        engagement=engagement,
        expert_type="doctor",
        expert_id=8801,
        engagements_repository=EngagementsRepository(),
        experts_repository=ExpertsRepository(),
        employee_repository=EmployeeRepository(),
    )

    assert user_ids == [5201, 5202]
    assignment = await test_db_session.scalar(
        select(OnboardingAssistantAssignment).where(
            OnboardingAssistantAssignment.engagement_id == engagement_id,
            OnboardingAssistantAssignment.employee_id == 5202,
        )
    )
    assert assignment is not None


@pytest.mark.asyncio
async def test_consultation_booking_alert_online_with_expert_id_skips_duplicate_assignment(test_db_session):
    engagement_id = 99106
    engagement = Engagement(
        engagement_id=engagement_id,
        engagement_name="Online Expert Already Assigned",
        organization_id=None,
        engagement_code="CBAT06",
        engagement_type="consultation",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
        consultations={"doctor": True},
        consultation_mode=ConsultationMode.online,
    )
    test_db_session.add(engagement)

    expert_user = User(user_id=5203, age=35, phone="5203000000", status="active")
    test_db_session.add(expert_user)
    await test_db_session.flush()

    expert_employee = Employee(employee_id=5203, user_id=5203, role=EmployeeRole.expert, status="active")
    test_db_session.add(expert_employee)
    await test_db_session.flush()

    test_db_session.add(
        OnboardingAssistantAssignment(engagement_id=engagement_id, employee_id=5203)
    )
    test_db_session.add(
        Expert(
            expert_id=8802,
            user_id=5203,
            expert_type="doctor",
            specialization="General",
            status="active",
        )
    )
    await test_db_session.commit()

    from modules.employee.repository import EmployeeRepository

    before = (
        await test_db_session.execute(
            select(OnboardingAssistantAssignment).where(
                OnboardingAssistantAssignment.engagement_id == engagement_id
            )
        )
    ).scalars().all()

    user_ids = await resolve_consultation_booking_alert_user_ids(
        test_db_session,
        engagement=engagement,
        expert_type="doctor",
        expert_id=8802,
        engagements_repository=EngagementsRepository(),
        experts_repository=ExpertsRepository(),
        employee_repository=EmployeeRepository(),
    )

    after = (
        await test_db_session.execute(
            select(OnboardingAssistantAssignment).where(
                OnboardingAssistantAssignment.engagement_id == engagement_id
            )
        )
    ).scalars().all()

    assert user_ids == [5203]
    assert len(before) == len(after) == 1


@pytest.mark.asyncio
async def test_consultation_booking_alert_skips_without_services(test_db_session, monkeypatch):
    await _seed_diagnostic_package(test_db_session)
    engagement = Engagement(
        engagement_id=99103,
        engagement_name="No Services",
        organization_id=None,
        engagement_code="CBAT03",
        engagement_type="consultation",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
        consultations={"doctor": True},
    )
    test_db_session.add(engagement)
    participant = User(user_id=5103, age=30, phone="5103000000", status="active")
    test_db_session.add(participant)
    await test_db_session.commit()

    dispatch_mock = AsyncMock()
    notifications_service = NotificationsService(NotificationsRepository())
    monkeypatch.setattr(notifications_service, "dispatch", dispatch_mock)

    await notify_onboarding_assistants_on_consultation_booking(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=NotificationsRepository(),
        engagements_repository=EngagementsRepository(),
        engagement=engagement,
        participant_user=participant,
        participant_user_id=5103,
        expert_type="doctor",
        consultation_date=date(2026, 3, 16),
        consultation_slot="11:00",
    )

    dispatch_mock.assert_not_awaited()
