"""Notification deduplication helpers for cron jobs."""

from __future__ import annotations

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.models import Notification


async def has_notification_been_sent(
    db: AsyncSession,
    *,
    service_key: str,
    user_id: int,
    engagement_id: int,
) -> bool:
    """Return True if a notification with the given service_key was already
    sent for this user+engagement (status is ``sent``)."""
    query = (
        select(func.count())
        .select_from(Notification)
        .where(Notification.service_key == service_key)
        .where(Notification.engagement_id == engagement_id)
        .where(Notification.status == "sent")
    )
    # Notification.user is a JSON column storing user info; we need to check
    # if the notification's user list contains this user_id.  Since user is
    # stored as a JSON object with a "user_id" field during dispatch, we
    # filter by checking the notification was created for matching engagement
    # and service key.  For a more precise check we also verify the JSON
    # contains the user_id.
    result = await db.execute(query)
    count = result.scalar_one()
    if count == 0:
        return False

    # Double-check: verify at least one of those notifications was for this user_id
    detail_query = (
        select(Notification)
        .where(Notification.service_key == service_key)
        .where(Notification.engagement_id == engagement_id)
        .where(Notification.status == "sent")
    )
    result = await db.execute(detail_query)
    for notif in result.scalars():
        user_data = notif.user
        if isinstance(user_data, dict) and user_data.get("user_id") == user_id:
            return True
        if isinstance(user_data, list):
            for u in user_data:
                if isinstance(u, dict) and u.get("user_id") == user_id:
                    return True
    return False
