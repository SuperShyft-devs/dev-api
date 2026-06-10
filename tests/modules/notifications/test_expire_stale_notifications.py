"""Tests for expiring stale pending notifications."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from modules.engagements.models import Engagement
from modules.notifications.expire_stale import expire_stale_notifications
from modules.notifications.models import Notification
from modules.notifications.repository import NotificationsRepository


async def _seed_engagement(test_db_session, *, engagement_id: int) -> None:
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=f"ENG-STALE-{engagement_id}",
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


async def _create_pending(
    test_db_session,
    *,
    service_key: str,
    dispatched_at: datetime,
) -> Notification:
    notification = Notification(
        service_key=service_key,
        status="pending",
        channel="email",
        user={"user_ids": [42]},
        engagement_id=9902,
        message="Workflow was started",
        dispatched_at=dispatched_at,
    )
    test_db_session.add(notification)
    await test_db_session.commit()
    await test_db_session.refresh(notification)
    return notification


@pytest.mark.asyncio
async def test_recent_pending_is_not_expired(test_db_session):
    await _seed_engagement(test_db_session, engagement_id=9902)
    await _seed_service(test_db_session, service_key="stale-recent-svc")
    notification = await _create_pending(
        test_db_session,
        service_key="stale-recent-svc",
        dispatched_at=datetime.now(timezone.utc) - timedelta(minutes=30),
    )

    result = await expire_stale_notifications(
        test_db_session,
        repository=NotificationsRepository(),
        timeout_hours=2,
        dry_run=False,
    )
    await test_db_session.commit()
    await test_db_session.refresh(notification)

    assert result["expired"] == 0
    assert notification.status == "pending"


@pytest.mark.asyncio
async def test_old_pending_is_expired(test_db_session):
    await _seed_engagement(test_db_session, engagement_id=9902)
    await _seed_service(test_db_session, service_key="stale-old-svc")
    notification = await _create_pending(
        test_db_session,
        service_key="stale-old-svc",
        dispatched_at=datetime.now(timezone.utc) - timedelta(hours=5),
    )

    result = await expire_stale_notifications(
        test_db_session,
        repository=NotificationsRepository(),
        timeout_hours=2,
        dry_run=False,
    )
    await test_db_session.commit()
    await test_db_session.refresh(notification)

    assert result["expired"] == 1
    assert notification.status == "failed"
    assert "Callback timeout" in (notification.message or "")
