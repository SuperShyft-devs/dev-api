"""Tests for retry_failed_notifications cron helper."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import random
import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from modules.audit.models import IntegrationSyncLog
from modules.engagements.models import Engagement
from modules.notifications.models import Notification
from modules.notifications.repository import NotificationsRepository
from modules.notifications.retry_failed import (
    build_dispatch_request,
    dedupe_failed_notifications,
    is_dispatch_failure,
    list_failed_notifications,
    retry_failed_notifications,
)
from modules.notifications.schemas import SessionDetails
from modules.notifications.service import NotificationsService


async def _engagement_type_id(test_db_session, code: str = "bio_ai") -> int:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES (:code, :dn, true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        ),
        {"code": code, "dn": code},
    )
    await test_db_session.flush()
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": code},
        )
    ).one()
    return int(row[0])


def _unique_service_key(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


async def _seed_engagement(test_db_session, *, engagement_id: int | None = None) -> int:
    if engagement_id is None:
        engagement_id = random.randint(50_000_000, 99_999_999)
    engagement_type_id = await _engagement_type_id(test_db_session)
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=f"ENG-RETRY-{engagement_id}",
            engagement_type=engagement_type_id,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 1),
            status="active",
        )
    )
    await test_db_session.commit()
    return engagement_id


async def _seed_service(
    test_db_session,
    *,
    service_key: str,
    channel: str = "email",
    require_otp: bool = False,
    require_session_details: bool = False,
    require_participant_detail: bool = False,
    require_external_link: bool = False,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, "
            "require_blood_report_url, require_bio_ai_report_url, require_participant_detail, "
            "require_otp, require_session_details, require_external_link) "
            "VALUES (:sk, :dn, :ch, 'test-webhook', true, false, false, "
            ":rpd, :rotp, :rsd, :rel) "
            "ON CONFLICT (service_key) DO UPDATE SET "
            "channel = EXCLUDED.channel, require_otp = EXCLUDED.require_otp, "
            "require_session_details = EXCLUDED.require_session_details, "
            "require_participant_detail = EXCLUDED.require_participant_detail, "
            "require_external_link = EXCLUDED.require_external_link, is_active = true"
        ),
        {
            "sk": service_key,
            "dn": service_key,
            "ch": channel,
            "rpd": require_participant_detail,
            "rotp": require_otp,
            "rsd": require_session_details,
            "rel": require_external_link,
        },
    )
    await test_db_session.commit()


async def _seed_failed_notification(
    test_db_session,
    *,
    notification_id: int | None = None,
    service_key: str,
    user_id: int,
    engagement_id: int,
    dispatched_at: datetime | None = None,
    channel: str = "email",
) -> Notification:
    notification = Notification(
        service_key=service_key,
        status="failed",
        channel=channel,
        user={"user_ids": [user_id]},
        engagement_id=engagement_id,
        message="Email sending failed",
        dispatched_at=dispatched_at or datetime.now(timezone.utc) - timedelta(hours=1),
    )
    if notification_id is not None:
        notification.notification_id = notification_id
    test_db_session.add(notification)
    await test_db_session.commit()
    await test_db_session.refresh(notification)
    return notification


def test_dedupe_failed_notifications_keeps_latest():
    now = datetime.now(timezone.utc)
    older = Notification(
        notification_id=10,
        service_key="svc-a",
        status="failed",
        channel="email",
        user={"user_ids": [42]},
        engagement_id=100,
        dispatched_at=now - timedelta(hours=2),
    )
    newer = Notification(
        notification_id=20,
        service_key="svc-a",
        status="failed",
        channel="email",
        user={"user_ids": [42]},
        engagement_id=100,
        dispatched_at=now - timedelta(hours=1),
    )
    candidates = dedupe_failed_notifications([older, newer])
    assert len(candidates) == 1
    assert candidates[0].notification_id == 20
    assert candidates[0].user_id == 42


def test_is_dispatch_failure_detects_webhook_and_email_messages():
    assert is_dispatch_failure({"status": "failed", "message": "Webhook call failed: timeout"})
    assert is_dispatch_failure({"status": "pending", "message": "Email sending failed"})
    assert not is_dispatch_failure({"status": "pending", "message": "Webhook called successfully"})


@pytest.mark.asyncio
async def test_list_failed_notifications_excludes_otp_services(test_db_session):
    engagement_id = await _seed_engagement(test_db_session)
    email_sk = _unique_service_key("retry-email-svc")
    otp_sk = _unique_service_key("email-otp")
    await _seed_service(test_db_session, service_key=email_sk)
    await _seed_service(test_db_session, service_key=otp_sk, require_otp=True)
    await _seed_failed_notification(
        test_db_session,
        service_key=email_sk,
        user_id=42,
        engagement_id=engagement_id,
    )
    await _seed_failed_notification(
        test_db_session,
        service_key=otp_sk,
        user_id=43,
        engagement_id=engagement_id,
    )

    rows = await list_failed_notifications(test_db_session, hours=24, channel="email")
    keys = {row.service_key for row in rows}
    assert email_sk in keys
    assert otp_sk not in keys


@pytest.mark.asyncio
async def test_retry_dry_run_lists_candidates_without_dispatch(test_db_session):
    engagement_id = await _seed_engagement(test_db_session)
    service_key = _unique_service_key("retry-dry-svc")
    await _seed_service(test_db_session, service_key=service_key)
    await _seed_failed_notification(
        test_db_session,
        service_key=service_key,
        user_id=44,
        engagement_id=engagement_id,
    )

    service = NotificationsService(NotificationsRepository())
    service.dispatch = AsyncMock()  # type: ignore[method-assign]

    async def session_factory():
        return test_db_session

    # job_session_factory returns a callable; mimic async context manager
    class _Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return test_db_session

        async def __aexit__(self, *args):
            return None

    result = await retry_failed_notifications(
        _Factory(),
        notifications_service=service,
        hours=24,
        channel="email",
        service_key=service_key,
        limit=10,
        delay_seconds=0,
        dry_run=True,
    )

    assert result["matched"] == 1
    assert result["retried"] == 1
    service.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_retry_skips_when_already_sent(test_db_session):
    engagement_id = await _seed_engagement(test_db_session)
    service_key = _unique_service_key("retry-sent-svc")
    await _seed_service(test_db_session, service_key=service_key)
    await _seed_failed_notification(
        test_db_session,
        service_key=service_key,
        user_id=45,
        engagement_id=engagement_id,
    )
    test_db_session.add(
        Notification(
            service_key=service_key,
            status="sent",
            channel="email",
            user={"user_ids": [45]},
            engagement_id=engagement_id,
            message="Email sent successfully",
            dispatched_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    service = NotificationsService(NotificationsRepository())
    service.dispatch = AsyncMock()  # type: ignore[method-assign]

    class _Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return test_db_session

        async def __aexit__(self, *args):
            return None

    result = await retry_failed_notifications(
        _Factory(),
        notifications_service=service,
        hours=24,
        service_key=service_key,
        delay_seconds=0,
        dry_run=False,
    )
    assert result["skipped"] == 1
    assert result["retried"] == 0
    service.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_retry_dedupes_multiple_fails_for_same_user(test_db_session):
    engagement_id = await _seed_engagement(test_db_session)
    service_key = _unique_service_key("retry-dedupe-svc")
    await _seed_service(test_db_session, service_key=service_key)
    await _seed_failed_notification(
        test_db_session,
        service_key=service_key,
        user_id=46,
        engagement_id=engagement_id,
        dispatched_at=datetime.now(timezone.utc) - timedelta(hours=3),
    )
    await _seed_failed_notification(
        test_db_session,
        service_key=service_key,
        user_id=46,
        engagement_id=engagement_id,
        dispatched_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )

    dispatch_calls: list[dict] = []

    async def _dispatch(db, *, payload, triggered_by_user_id=None):
        dispatch_calls.append({"user_ids": payload.user_ids})
        return {
            "notification_id": 999,
            "status": "pending",
            "message": "Webhook called successfully",
        }

    service = NotificationsService(NotificationsRepository())
    service.dispatch = _dispatch  # type: ignore[method-assign]

    class _Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return test_db_session

        async def __aexit__(self, *args):
            return None

    result = await retry_failed_notifications(
        _Factory(),
        notifications_service=service,
        hours=24,
        service_key=service_key,
        delay_seconds=0,
        dry_run=False,
    )

    assert result["matched"] == 1
    assert result["retried"] == 1
    assert len(dispatch_calls) == 1
    assert dispatch_calls[0]["user_ids"] == [46]


@pytest.mark.asyncio
async def test_retry_circuit_breaker_stops_after_consecutive_failures(test_db_session):
    engagement_id = await _seed_engagement(test_db_session)
    service_key = _unique_service_key("retry-breaker-svc")
    await _seed_service(test_db_session, service_key=service_key)
    for user_id in (50, 51, 52):
        await _seed_failed_notification(
            test_db_session,
            service_key=service_key,
            user_id=user_id,
            engagement_id=engagement_id,
        )

    async def _dispatch(db, *, payload, triggered_by_user_id=None):
        return {
            "notification_id": 1000 + payload.user_ids[0],
            "status": "failed",
            "message": "Email sending failed",
        }

    service = NotificationsService(NotificationsRepository())
    service.dispatch = _dispatch  # type: ignore[method-assign]
    sleep_calls: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    class _Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return test_db_session

        async def __aexit__(self, *args):
            return None

    result = await retry_failed_notifications(
        _Factory(),
        notifications_service=service,
        hours=24,
        service_key=service_key,
        delay_seconds=2.5,
        max_consecutive_failures=2,
        dry_run=False,
        sleep_fn=_sleep,
    )

    assert result["retried"] == 2
    assert result["failed"] == 2
    assert result["stopped_early"] is True
    assert len(sleep_calls) == 1


@pytest.mark.asyncio
async def test_retry_invokes_delay_between_successful_dispatches(test_db_session):
    engagement_id = await _seed_engagement(test_db_session)
    service_key = _unique_service_key("retry-delay-svc")
    await _seed_service(test_db_session, service_key=service_key)
    for user_id in (60, 61):
        await _seed_failed_notification(
            test_db_session,
            service_key=service_key,
            user_id=user_id,
            engagement_id=engagement_id,
        )

    async def _dispatch(db, *, payload, triggered_by_user_id=None):
        return {
            "notification_id": 2000 + payload.user_ids[0],
            "status": "pending",
            "message": "Webhook called successfully",
        }

    service = NotificationsService(NotificationsRepository())
    service.dispatch = _dispatch  # type: ignore[method-assign]
    sleep_calls: list[float] = []

    async def _sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    class _Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return test_db_session

        async def __aexit__(self, *args):
            return None

    result = await retry_failed_notifications(
        _Factory(),
        notifications_service=service,
        hours=24,
        service_key=service_key,
        delay_seconds=4,
        dry_run=False,
        sleep_fn=_sleep,
    )

    assert result["retried"] == 2
    assert sleep_calls == [4.0]


def test_build_dispatch_request_reconstructs_session_details_from_sync_log():
    from modules.notifications.models import NotificationService

    candidate = dedupe_failed_notifications(
        [
            Notification(
                notification_id=1,
                service_key="consult-email",
                status="failed",
                channel="email",
                user={"user_ids": [7]},
                engagement_id=99,
            )
        ]
    )[0]
    service = NotificationService(
        service_key="consult-email",
        display_name="Consult",
        channel="email",
        webhook_path="/consult",
        is_active=True,
        require_session_details=True,
    )
    sync_payload = {
        "notification_id": 1,
        "members": [
            {
                "first_name": "Test",
                "email": "t@example.com",
                "session_details": {
                    "want": True,
                    "date": "2026-09-10",
                    "slot": "10:00",
                    "expert_type": "nutrition",
                },
            }
        ],
    }
    request, skip = build_dispatch_request(candidate, service=service, sync_payload=sync_payload)
    assert skip is None
    assert request is not None
    assert request.session_details_by_user_id is not None
    details = request.session_details_by_user_id[7]
    assert isinstance(details, SessionDetails)
    assert details.expert_type == "nutrition"


@pytest.mark.asyncio
async def test_load_n8n_request_payload_from_sync_log(test_db_session):
    test_db_session.add(
        IntegrationSyncLog(
            provider="n8n",
            api_endpoint_url="https://n8n.example/webhook/test",
            request_payload={
                "notification_id": 4242,
                "members": [
                    {
                        "email": "a@b.com",
                        "session_details": {
                            "want": False,
                            "date": "2026-09-01",
                            "slot": "9",
                            "expert_type": "doctor",
                        },
                    }
                ],
            },
            status="success",
        )
    )
    await test_db_session.commit()

    from modules.notifications.retry_failed import load_n8n_request_payload

    payload = await load_n8n_request_payload(test_db_session, notification_id=4242)
    assert payload is not None
    assert payload["notification_id"] == 4242
    assert payload["members"][0]["email"] == "a@b.com"
