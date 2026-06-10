"""Tests for POST /notifications/callback."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from modules.engagements.models import Engagement
from modules.notifications.models import Notification


async def _seed_engagement(test_db_session, *, engagement_id: int) -> None:
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Camp",
            engagement_code=f"ENG-CB-{engagement_id}",
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


async def _create_notification(
    test_db_session,
    *,
    service_key: str,
    notification_id: int | None = None,
    status: str = "pending",
) -> Notification:
    notification = Notification(
        notification_id=notification_id,
        service_key=service_key,
        status=status,
        channel="email",
        user={"user_ids": [42]},
        engagement_id=9901,
        message="Workflow was started",
        dispatched_at=datetime.now(timezone.utc),
    )
    test_db_session.add(notification)
    await test_db_session.commit()
    await test_db_session.refresh(notification)
    return notification


@pytest.mark.asyncio
async def test_callback_sets_sent_and_completed_at(async_client, test_db_session):
    await _seed_engagement(test_db_session, engagement_id=9901)
    await _seed_service(test_db_session, service_key="callback-sent-svc")
    notification = await _create_notification(test_db_session, service_key="callback-sent-svc")

    response = await async_client.post(
        "/notifications/callback",
        json={
            "notification_id": notification.notification_id,
            "status": "sent",
            "message": "Email sent successfully",
        },
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["notification_id"] == notification.notification_id
    assert data["status"] == "sent"

    await test_db_session.refresh(notification)
    assert notification.status == "sent"
    assert notification.message == "Email sent successfully"
    assert notification.completed_at is not None


@pytest.mark.asyncio
async def test_callback_sets_failed_without_completed_at(async_client, test_db_session):
    await _seed_engagement(test_db_session, engagement_id=9901)
    await _seed_service(test_db_session, service_key="callback-failed-svc")
    notification = await _create_notification(test_db_session, service_key="callback-failed-svc")

    response = await async_client.post(
        "/notifications/callback",
        json={
            "notification_id": notification.notification_id,
            "status": "failed",
            "message": "Report fetch failed",
        },
    )
    assert response.status_code == 200

    await test_db_session.refresh(notification)
    assert notification.status == "failed"
    assert notification.message == "Report fetch failed"
    assert notification.completed_at is None


@pytest.mark.asyncio
async def test_callback_returns_404_for_unknown_notification(async_client, test_db_session):
    response = await async_client.post(
        "/notifications/callback",
        json={
            "notification_id": 99999999,
            "status": "sent",
            "message": "Email sent successfully",
        },
    )
    assert response.status_code == 404
