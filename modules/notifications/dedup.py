"""Notification deduplication helpers for cron jobs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import cast, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.notifications.models import Notification
from modules.notifications.repository import NotificationsRepository


async def should_skip_notification(
    db: AsyncSession,
    *,
    service_key: str,
    user_id: int,
    engagement_id: int,
    repository: NotificationsRepository | None = None,
    pending_timeout_hours: int | None = None,
) -> str | None:
    """Return a skip reason if dispatch should be skipped, else None.

    Skips when a prior notification for this service_key + user + engagement
    has status ``sent``, or when a non-stale ``pending`` row is still in flight.
    Retries when prior rows are ``failed`` or stale ``pending``.
    """
    repo = repository or NotificationsRepository()
    hours = (
        pending_timeout_hours
        if pending_timeout_hours is not None
        else settings.NOTIFICATION_PENDING_TIMEOUT_HOURS
    )
    in_flight_cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    if await repo.has_in_flight_pending_notification(
        db,
        service_key=service_key,
        user_id=user_id,
        engagement_id=engagement_id,
        dispatched_after=in_flight_cutoff,
    ):
        return "already in flight"

    query = (
        select(Notification.status)
        .where(Notification.service_key == service_key)
        .where(Notification.engagement_id == engagement_id)
        .where(
            cast(Notification.user, JSONB)["user_ids"].contains(
                type_coerce([user_id], JSONB)
            )
        )
    )
    result = await db.execute(query)
    statuses = [row[0] for row in result.all()]

    if not statuses:
        return None

    if any(status == "sent" for status in statuses):
        return "already sent"

    return None


async def has_notification_been_sent(
    db: AsyncSession,
    *,
    service_key: str,
    user_id: int,
    engagement_id: int,
) -> bool:
    """Return True if a notification was already sent for this user+engagement+service."""
    return (
        await should_skip_notification(
            db,
            service_key=service_key,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        is not None
    )
