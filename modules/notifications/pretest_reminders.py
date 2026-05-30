"""Dispatch pretest blood-collection reminders for participants with collection tomorrow."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.engagements.repository import EngagementsRepository
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

PRETEST_WHATSAPP_KEY = "pretest-whatsapp"
PRETEST_EMAIL_KEY = "pretest-email"
_IST = ZoneInfo("Asia/Kolkata")
PretestWindow = Literal["early", "late"]


def tomorrow_in_ist(*, as_of: date | None = None) -> date:
    """Return the calendar day after as_of (or today in IST when as_of is omitted)."""
    if as_of is not None:
        return as_of + timedelta(days=1)
    today_ist = datetime.now(_IST).date()
    return today_ist + timedelta(days=1)


async def _ensure_pretest_services_active(
    db: AsyncSession,
    *,
    notifications_repository: NotificationsRepository,
) -> None:
    for service_key in (PRETEST_WHATSAPP_KEY, PRETEST_EMAIL_KEY):
        svc = await notifications_repository.get_service_by_key(db, service_key=service_key)
        if svc is None or not svc.is_active:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Notification service '{service_key}' not found or inactive",
            )


async def dispatch_pretest_reminders(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    notifications_repository: NotificationsRepository,
    engagements_repository: EngagementsRepository,
    window: PretestWindow,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, int | str | bool]:
    """Find participants with collection tomorrow and dispatch WhatsApp + email pretest reminders."""
    await _ensure_pretest_services_active(
        db,
        notifications_repository=notifications_repository,
    )

    collection_date = tomorrow_in_ist(as_of=as_of)
    participants = await engagements_repository.list_participants_for_pretest_reminder(
        db,
        collection_date=collection_date,
        window=window,
    )

    matched = len(participants)
    whatsapp_sent = 0
    email_sent = 0
    failed = 0

    if dry_run:
        return {
            "window": window,
            "as_of": (as_of or datetime.now(_IST).date()).isoformat(),
            "collection_date": collection_date.isoformat(),
            "matched": matched,
            "whatsapp_sent": 0,
            "email_sent": 0,
            "failed": 0,
            "dry_run": True,
        }

    for user_id, engagement_id in participants:
        for service_key in (PRETEST_WHATSAPP_KEY, PRETEST_EMAIL_KEY):
            try:
                await notifications_service.dispatch(
                    db,
                    payload=DispatchRequest(
                        service_key=service_key,
                        user_ids=[user_id],
                        engagement_id=engagement_id,
                    ),
                    triggered_by_user_id=None,
                )
                if service_key == PRETEST_WHATSAPP_KEY:
                    whatsapp_sent += 1
                else:
                    email_sent += 1
            except Exception as exc:
                failed += 1
                logger.warning(
                    "Pretest reminder dispatch failed: service_key=%s user_id=%s engagement_id=%s: %s",
                    service_key,
                    user_id,
                    engagement_id,
                    str(exc),
                )

    return {
        "window": window,
        "as_of": (as_of or datetime.now(_IST).date()).isoformat(),
        "collection_date": collection_date.isoformat(),
        "matched": matched,
        "whatsapp_sent": whatsapp_sent,
        "email_sent": email_sent,
        "failed": failed,
        "dry_run": False,
    }
