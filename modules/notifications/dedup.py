"""Notification deduplication helpers for cron jobs."""

from __future__ import annotations

from sqlalchemy import cast, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.models import Notification


async def should_skip_notification(
    db: AsyncSession,
    *,
    service_key: str,
    user_id: int,
    engagement_id: int,
) -> str | None:
    """Return a skip reason if dispatch should be skipped, else None.

    Skips only when a prior notification for this service_key + user + engagement
    has status ``sent``. Retries when prior rows are ``failed`` or ``pending`` only.
    """
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
