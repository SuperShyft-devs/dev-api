"""Unit tests for onboarding enrollment notifications."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from modules.engagements.constants import DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY
from modules.engagements.models import Engagement
from modules.engagements.repository import EngagementsRepository
from modules.notifications.onboarding_notify import notify_onboarding_assistants_on_enrollment
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService


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
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'test-webhook', :active, false, :rpd) "
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
            "VALUES (:did, 'REF1', 'Diag', 'test_provider', 'active', 0) ON CONFLICT (diagnostic_package_id) DO NOTHING"
        ),
        {"did": diagnostic_package_id},
    )
    await test_db_session.commit()


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
        engagement_type="bio_ai",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="active",
        participant_count=0,
        notification_service_key=custom_key,
    )
    test_db_session.add(engagement)
    await test_db_session.commit()

    dispatch_mock = AsyncMock()
    notifications_service = NotificationsService(NotificationsRepository())
    monkeypatch.setattr(notifications_service, "dispatch", dispatch_mock)

    engagements_repo = EngagementsRepository()
    monkeypatch.setattr(
        engagements_repo,
        "list_onboarding_assistant_user_ids",
        AsyncMock(return_value=[101]),
    )

    await notify_onboarding_assistants_on_enrollment(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=NotificationsRepository(),
        engagements_repository=engagements_repo,
        engagement=engagement,
        participant_user_id=5001,
        participant_details={
            "name": "Test User",
            "email": "t@example.com",
            "participant_user_id": "5001",
        },
    )

    dispatch_mock.assert_awaited_once()
    payload = dispatch_mock.await_args.kwargs["payload"]
    assert payload.service_key == custom_key
    assert payload.user_ids == [101]
    assert payload.engagement_id == 99001
    assert payload.participant_details["participant_user_id"] == "5001"


@pytest.mark.asyncio
async def test_notify_skips_when_participant_details_required_but_missing(test_db_session, monkeypatch):
    service_key = "needs-participant-detail"
    await _seed_notification_service(
        test_db_session, service_key=service_key, require_participant_detail=True
    )
    await _seed_diagnostic_package(test_db_session)

    engagement = Engagement(
        engagement_id=99002,
        engagement_name="No Details",
        organization_id=None,
        engagement_code="NTFY02",
        engagement_type="bio_ai",
        assessment_package_id=None,
        diagnostic_package_id=1,
        city="BLR",
        slot_duration=20,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        status="active",
        participant_count=0,
        notification_service_key=service_key,
    )
    test_db_session.add(engagement)
    await test_db_session.commit()

    dispatch_mock = AsyncMock()
    notifications_service = NotificationsService(NotificationsRepository())
    monkeypatch.setattr(notifications_service, "dispatch", dispatch_mock)

    engagements_repo = EngagementsRepository()
    monkeypatch.setattr(
        engagements_repo,
        "list_onboarding_assistant_user_ids",
        AsyncMock(return_value=[101]),
    )

    await notify_onboarding_assistants_on_enrollment(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=NotificationsRepository(),
        engagements_repository=engagements_repo,
        engagement=engagement,
        participant_user_id=5002,
        participant_details=None,
    )

    dispatch_mock.assert_not_awaited()
