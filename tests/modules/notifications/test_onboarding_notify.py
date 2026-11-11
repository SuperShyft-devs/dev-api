"""Unit tests for onboarding enrollment notifications."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import json

import pytest
from sqlalchemy import text

from modules.engagements.constants import DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY
from modules.engagements.models import Engagement
from modules.engagements.repository import EngagementsRepository
from modules.notifications.onboarding_notify import (
    detail_or_hyphen,
    notify_onboarding_assistants_on_enrollment,
    participant_details_from_user,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.users.models import User
from modules.users.schemas import PublicUserOnboardRequest
from modules.users.service import UsersService


async def _seed_notification_service(
    test_db_session,
    *,
    service_key: str,
    require_participant_detail: bool = False,
    is_active: bool = True,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'test-webhook', :active, false, false, :rpd) "
            "ON CONFLICT (service_key) DO UPDATE SET "
            "is_active = :active, require_participant_detail = :rpd"
        ),
        {
            "sk": service_key,
            "dn": service_key,
            "active": is_active,
            "rpd": require_participant_detail,
        },
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def _seed_diagnostic_package(test_db_session, diagnostic_package_id: int = 1) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status, bookings_count) "
            "VALUES (:did, 'REF1', 'Diag', 'healthians', 1, 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": diagnostic_package_id},
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_notify_emits_users_onboarded_without_recipient_filter(test_db_session, monkeypatch):
    captured: dict = {}

    class _FakeEngine:
        async def emit(self, db, **kwargs):
            captured.update(kwargs)
            return []

    monkeypatch.setattr(
        "modules.notifications.onboarding_notify.get_notification_engine",
        lambda: _FakeEngine(),
    )
    engagement = Engagement(
        engagement_id=99000,
        engagement_name="Emit All",
        organization_id=None,
        engagement_code="EMIT01",
        engagement_type="bio_ai",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
    )
    await notify_onboarding_assistants_on_enrollment(
        test_db_session,
        engagement=engagement,
        participant_user_id=42,
        participant_details={"name": "Test", "participant_user_id": "42"},
    )
    assert captured["event_code"] == "users.onboarded"
    assert captured["engagement_id"] == 99000
    assert captured["context"]["participant_user_id"] == 42
    assert "recipient_types" not in captured


@pytest.mark.asyncio
async def test_notify_uses_engagement_service_key(test_db_session, monkeypatch):
    custom_key = "custom-enroll-alert"
    default_key = DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY
    await _seed_notification_service(test_db_session, service_key=default_key)
    await _seed_notification_service(test_db_session, service_key=custom_key)
    await _seed_diagnostic_package(test_db_session)

    engagement = Engagement(
        engagement_id=99001,
        engagement_name="Notify Test",
        organization_id=None,
        engagement_code="NTFY01",
        engagement_type=1,
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
    )
    test_db_session.add(engagement)
    services_json = json.dumps([{"service_key": custom_key}])
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, notification_services,
                engagement_type_id, delivery_channel
            )
            VALUES (
                'users.onboarded', 'User onboarded', 'assistants', CAST(:services AS json), NULL, 'whatsapp'
            )
            ON CONFLICT ON CONSTRAINT uq_notification_events_binding DO UPDATE
            SET notification_services = EXCLUDED.notification_services
            """
        ),
        {"services": services_json},
    )
    await test_db_session.commit()

    contacts_mock = AsyncMock(return_value={"notification_id": 2, "status": "pending"})
    from modules.notifications import engine as engine_module

    engine_module.reset_notification_engine()
    notifications_service = NotificationsService(NotificationsRepository())
    monkeypatch.setattr(notifications_service, "dispatch", AsyncMock())
    monkeypatch.setattr(notifications_service, "dispatch_to_contacts", contacts_mock)

    engagements_repo = EngagementsRepository()
    monkeypatch.setattr(
        engagements_repo,
        "list_onboarding_assistant_notify_contacts",
        AsyncMock(return_value=[{"phone": "9999999999", "name": "OA"}]),
    )

    from modules.notifications.engine import NotificationEngine
    from modules.notifications.recipient_resolver import RecipientResolver

    test_engine = NotificationEngine(
        notifications_service=notifications_service,
        recipient_resolver=RecipientResolver(engagements_repository=engagements_repo),
    )
    monkeypatch.setattr(
        "modules.notifications.onboarding_notify.get_notification_engine",
        lambda: test_engine,
    )

    await notify_onboarding_assistants_on_enrollment(
        test_db_session,
        engagement=engagement,
        participant_user_id=5001,
        participant_details={
            "name": "Test User",
            "email": "t@example.com",
            "participant_user_id": "5001",
        },
    )

    contacts_mock.assert_awaited()
    assert contacts_mock.await_args.kwargs["service_key"] == custom_key
    assert contacts_mock.await_args.kwargs["engagement_id"] == 99001
    assert contacts_mock.await_args.kwargs["participant_details"]["participant_user_id"] == "5001"
    engine_module.reset_notification_engine()


@pytest.mark.asyncio
async def test_notify_dispatches_user_and_assistant_bindings(test_db_session, monkeypatch):
    user_key = "welcome-whatsapp-test"
    assistant_key = "users-onboarded-whatsapp-test"
    await _seed_notification_service(test_db_session, service_key=user_key)
    await _seed_notification_service(test_db_session, service_key=assistant_key)
    await _seed_diagnostic_package(test_db_session)
    await test_db_session.execute(
        text("DELETE FROM notification_events WHERE event_code = 'users.onboarded'")
    )
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, notification_services,
                engagement_type_id, delivery_channel
            )
            VALUES
                ('users.onboarded', 'User onboarded', 'user', CAST(:user_svc AS json), NULL, 'whatsapp'),
                ('users.onboarded', 'User onboarded', 'assistants', CAST(:oa_svc AS json), NULL, 'whatsapp')
            ON CONFLICT ON CONSTRAINT uq_notification_events_binding DO UPDATE
            SET notification_services = EXCLUDED.notification_services
            """
        ),
        {
            "user_svc": json.dumps([{"service_key": user_key}]),
            "oa_svc": json.dumps([{"service_key": assistant_key}]),
        },
    )
    test_db_session.add(
        User(user_id=88001, age=30, phone="8800100000", status="active")
    )
    engagement = Engagement(
        engagement_id=99002,
        engagement_name="Dual bindings",
        organization_id=None,
        engagement_code="NTFY02",
        engagement_type=1,
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="running",
    )
    test_db_session.add(engagement)
    await test_db_session.commit()

    dispatch_mock = AsyncMock(return_value={"notification_id": 1, "status": "pending"})
    contacts_mock = AsyncMock(return_value={"notification_id": 2, "status": "pending"})
    from modules.notifications import engine as engine_module

    engine_module.reset_notification_engine()
    notifications_service = NotificationsService(NotificationsRepository())
    monkeypatch.setattr(notifications_service, "dispatch", dispatch_mock)
    monkeypatch.setattr(notifications_service, "dispatch_to_contacts", contacts_mock)

    engagements_repo = EngagementsRepository()
    monkeypatch.setattr(
        engagements_repo,
        "list_onboarding_assistant_notify_contacts",
        AsyncMock(return_value=[{"phone": "8888888888", "name": "OA"}]),
    )

    from modules.notifications.engine import NotificationEngine
    from modules.notifications.recipient_resolver import RecipientResolver

    test_engine = NotificationEngine(
        notifications_service=notifications_service,
        recipient_resolver=RecipientResolver(engagements_repository=engagements_repo),
    )
    monkeypatch.setattr(
        "modules.notifications.onboarding_notify.get_notification_engine",
        lambda: test_engine,
    )

    await notify_onboarding_assistants_on_enrollment(
        test_db_session,
        engagement=engagement,
        participant_user_id=88001,
        participant_details={"name": "Member", "participant_user_id": "88001"},
    )

    dispatch_mock.assert_awaited_once()
    assert dispatch_mock.await_args.kwargs["payload"].service_key == user_key
    contacts_mock.assert_awaited_once()
    assert contacts_mock.await_args.kwargs["service_key"] == assistant_key
    engine_module.reset_notification_engine()


def test_detail_or_hyphen_uses_placeholder_for_blank_values():
    assert detail_or_hyphen(None) == "-"
    assert detail_or_hyphen("") == "-"
    assert detail_or_hyphen("   ") == "-"
    assert detail_or_hyphen("BLR") == "BLR"
    assert detail_or_hyphen(30) == "30"


def test_participant_details_from_onboard_payload_fills_missing_with_hyphen():
    payload = PublicUserOnboardRequest(
        first_name="Pratheek",
        last_name="Bedre",
        phone="8762830757",
        email="pratheek.fitnastic@gmail.com",
        age=30,
        gender="male",
        blood_collection_date=date(2026, 8, 23),
        blood_collection_time_slot="09:00",
    )
    details = UsersService._participant_details_from_onboard_payload(
        payload,
        source="public",
        participant_user_id=5,
    )
    assert details["address"] == "-"
    assert details["pincode"] == "-"
    assert details["name"] == "Pratheek Bedre"
    assert details["collection_date"] == "2026-08-23"
    assert details["collection_time"] == "09:00"
    # Must pass DispatchRequest nested-string validation (empty strings previously failed).
    DispatchRequest(
        service_key="booking-alert-whatsapp",
        user_ids=[8],
        engagement_id=1037,
        participant_details=details,
    )


def test_participant_details_from_user_fills_missing_with_hyphen():
    class _User:
        first_name = "Pratheek"
        last_name = "Bedre"
        email = "pratheek.fitnastic@gmail.com"
        phone = "8762830757"
        age = 30
        gender = "male"
        address = None
        pin_code = None
        pincode = None

    details = participant_details_from_user(
        _User(),
        source="public",
        participant_user_id=5,
        collection_date="",
        collection_time=None,
    )
    assert details["address"] == "-"
    assert details["pincode"] == "-"
    assert details["collection_date"] == "-"
    assert details["collection_time"] == "-"
    assert details["cabin_name"] == "-"
    DispatchRequest(
        service_key="booking-alert-whatsapp",
        user_ids=[8],
        engagement_id=1037,
        participant_details=details,
    )
