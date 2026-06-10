"""Expire notifications stuck in pending without a callback."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.notifications.repository import NotificationsRepository

_STALE_MESSAGE = "Callback timeout — workflow did not report completion"


async def expire_stale_notifications(
    db: AsyncSession,
    *,
    repository: NotificationsRepository | None = None,
    timeout_hours: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Mark pending notifications older than the timeout as failed."""
    repo = repository or NotificationsRepository()
    hours = timeout_hours if timeout_hours is not None else settings.NOTIFICATION_PENDING_TIMEOUT_HOURS
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    stale_ids = await repo.list_stale_pending_notification_ids(db, dispatched_before=cutoff)
    if dry_run:
        return {
            "timeout_hours": hours,
            "cutoff": cutoff.isoformat(),
            "expired": 0,
            "would_expire": len(stale_ids),
            "notification_ids": stale_ids,
            "dry_run": True,
        }

    expired = await repo.expire_stale_pending_notifications(
        db,
        dispatched_before=cutoff,
        message=_STALE_MESSAGE,
    )
    return {
        "timeout_hours": hours,
        "cutoff": cutoff.isoformat(),
        "expired": expired,
        "would_expire": len(stale_ids),
        "notification_ids": stale_ids,
        "dry_run": False,
    }
