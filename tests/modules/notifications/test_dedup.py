"""Tests for notification deduplication helpers used by cron jobs."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from modules.engagements.models import Engagement
from modules.notifications.dedup import has_notification_been_sent, should_skip_notification
from modules.notifications.models import Notification
from modules.users.models import User


async def _seed_service(test_db_session, *, service_key: str) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_record_id, require_participant_detail) "
            "VALUES (:sk, :dn, 'email', 'test-webhook', true, false, false) "
            "ON CONFLICT (service_key) DO NOTHING"
        ),
        {"sk": service_key, "dn": service_key},
    )
    await test_db_session.commit()


async def _seed_engagement(test_db_session, *, engagement_id: int) -> None:
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=f"ENG-DEDUP-{engagement_id}",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="BLR",
            slot_duration=20,
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 1),
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()


async def _seed_notification(
    test_db_session,
    *,
    service_key: str,
    user_id: int,
    engagement_id: int,
    status: str,
) -> None:
    test_db_session.add(
        Notification(
            service_key=service_key,
            status=status,
            channel="email",
            user={"user_ids": [user_id]},
            engagement_id=engagement_id,
            message="test",
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_should_skip_when_no_prior_notification(test_db_session):
    await _seed_service(test_db_session, service_key="dedup-svc-a")
    await _seed_engagement(test_db_session, engagement_id=9701)

    result = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-a",
        user_id=42,
        engagement_id=9701,
    )
    assert result is None
    assert not await has_notification_been_sent(
        test_db_session,
        service_key="dedup-svc-a",
        user_id=42,
        engagement_id=9701,
    )


@pytest.mark.asyncio
async def test_should_skip_when_prior_sent(test_db_session):
    await _seed_service(test_db_session, service_key="dedup-svc-b")
    await _seed_engagement(test_db_session, engagement_id=9702)
    await _seed_notification(
        test_db_session,
        service_key="dedup-svc-b",
        user_id=42,
        engagement_id=9702,
        status="sent",
    )

    result = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-b",
        user_id=42,
        engagement_id=9702,
    )
    assert result == "already sent"
    assert await has_notification_been_sent(
        test_db_session,
        service_key="dedup-svc-b",
        user_id=42,
        engagement_id=9702,
    )


@pytest.mark.asyncio
async def test_should_not_skip_when_prior_failed(test_db_session):
    await _seed_service(test_db_session, service_key="dedup-svc-c")
    await _seed_engagement(test_db_session, engagement_id=9703)
    await _seed_notification(
        test_db_session,
        service_key="dedup-svc-c",
        user_id=42,
        engagement_id=9703,
        status="failed",
    )

    result = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-c",
        user_id=42,
        engagement_id=9703,
    )
    assert result is None


@pytest.mark.asyncio
async def test_should_not_skip_when_prior_pending(test_db_session):
    await _seed_service(test_db_session, service_key="dedup-svc-d")
    await _seed_engagement(test_db_session, engagement_id=9704)
    await _seed_notification(
        test_db_session,
        service_key="dedup-svc-d",
        user_id=42,
        engagement_id=9704,
        status="pending",
    )

    result = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-d",
        user_id=42,
        engagement_id=9704,
    )
    assert result is None


@pytest.mark.asyncio
async def test_should_not_skip_for_different_user(test_db_session):
    await _seed_service(test_db_session, service_key="dedup-svc-e")
    await _seed_engagement(test_db_session, engagement_id=9705)
    test_db_session.add(User(user_id=99, age=30, phone="9900000000", status="active"))
    await _seed_notification(
        test_db_session,
        service_key="dedup-svc-e",
        user_id=99,
        engagement_id=9705,
        status="sent",
    )

    result = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-e",
        user_id=42,
        engagement_id=9705,
    )
    assert result is None


@pytest.mark.asyncio
async def test_multi_service_independent_dedup(test_db_session):
    await _seed_service(test_db_session, service_key="dedup-svc-email")
    await _seed_service(test_db_session, service_key="dedup-svc-whatsapp")
    await _seed_engagement(test_db_session, engagement_id=9706)
    await _seed_notification(
        test_db_session,
        service_key="dedup-svc-email",
        user_id=42,
        engagement_id=9706,
        status="sent",
    )
    await _seed_notification(
        test_db_session,
        service_key="dedup-svc-whatsapp",
        user_id=42,
        engagement_id=9706,
        status="failed",
    )

    email_skip = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-email",
        user_id=42,
        engagement_id=9706,
    )
    whatsapp_skip = await should_skip_notification(
        test_db_session,
        service_key="dedup-svc-whatsapp",
        user_id=42,
        engagement_id=9706,
    )

    assert email_skip == "already sent"
    assert whatsapp_skip is None
