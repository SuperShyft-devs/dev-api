"""Dispatch notifications when a new user account is created."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.engine import get_notification_engine
from modules.notifications.onboarding_notify import participant_details_from_user

logger = logging.getLogger(__name__)


async def notify_user_registered(
    db: AsyncSession,
    *,
    user,
) -> None:
    """Emit users.registered for the new user (WhatsApp/email per admin bindings)."""
    user_id = int(user.user_id)
    details = participant_details_from_user(
        user,
        source="registration",
        participant_user_id=user_id,
    )
    try:
        await get_notification_engine().emit(
            db,
            event_code="users.registered",
            context={
                "user_id": user_id,
                "participant_details": details,
            },
        )
    except Exception as exc:
        logger.warning(
            "User registration notification failed for user_id=%s: %s",
            user_id,
            str(exc),
        )
