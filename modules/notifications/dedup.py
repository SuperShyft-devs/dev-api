"""Notification deduplication helpers for cron jobs."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import cast, func, or_, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.expire_stale import DEFAULT_PENDING_TIMEOUT_HOURS
from modules.notifications.models import Notification
from modules.notifications.repository import NotificationsRepository

_IST = ZoneInfo("Asia/Kolkata")


def _ist_day_bounds_utc(reference_date: date) -> tuple[datetime, datetime]:
    """Return UTC bounds for reference_date in Asia/Kolkata."""
    start_ist = datetime.combine(reference_date, time.min, tzinfo=_IST)
    end_ist = datetime.combine(reference_date, time.max, tzinfo=_IST)
    return start_ist.astimezone(timezone.utc), end_ist.astimezone(timezone.utc)


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
        else DEFAULT_PENDING_TIMEOUT_HOURS
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


async def should_skip_notification_on_date(
    db: AsyncSession,
    *,
    service_key: str,
    user_id: int,
    engagement_id: int,
    reference_date: date,
    repository: NotificationsRepository | None = None,
    pending_timeout_hours: int | None = None,
) -> str | None:
    """Return a skip reason if dispatch should be skipped for reference_date (IST), else None.

    Skips when a prior notification for this service_key + user + engagement was already
    ``sent`` on reference_date, or when a non-stale ``pending`` row was dispatched on
    reference_date and is still in flight.
    """
    repo = repository or NotificationsRepository()
    hours = (
        pending_timeout_hours
        if pending_timeout_hours is not None
        else DEFAULT_PENDING_TIMEOUT_HOURS
    )
    in_flight_cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    day_start_utc, day_end_utc = _ist_day_bounds_utc(reference_date)
    user_filter = cast(Notification.user, JSONB)["user_ids"].contains(
        type_coerce([user_id], JSONB)
    )
    dispatch_ts = func.coalesce(Notification.completed_at, Notification.dispatched_at)

    in_flight_query = (
        select(Notification.notification_id)
        .where(Notification.service_key == service_key)
        .where(Notification.engagement_id == engagement_id)
        .where(Notification.status == "pending")
        .where(Notification.dispatched_at.isnot(None))
        .where(Notification.dispatched_at >= in_flight_cutoff)
        .where(dispatch_ts >= day_start_utc)
        .where(dispatch_ts <= day_end_utc)
        .where(user_filter)
        .limit(1)
    )
    in_flight = (await db.execute(in_flight_query)).scalar_one_or_none()
    if in_flight is not None:
        return "already in flight"

    sent_query = (
        select(Notification.notification_id)
        .where(Notification.service_key == service_key)
        .where(Notification.engagement_id == engagement_id)
        .where(Notification.status == "sent")
        .where(
            or_(
                Notification.dispatched_at.isnot(None),
                Notification.completed_at.isnot(None),
            )
        )
        .where(dispatch_ts >= day_start_utc)
        .where(dispatch_ts <= day_end_utc)
        .where(user_filter)
        .limit(1)
    )
    sent = (await db.execute(sent_query)).scalar_one_or_none()
    if sent is not None:
        return "already sent"

    return None
