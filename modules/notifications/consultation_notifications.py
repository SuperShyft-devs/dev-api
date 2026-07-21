"""Dispatch consultation readiness notifications for home-collection consultation engagements."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.repository import EngagementsRepository
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")


async def dispatch_consultation_notifications(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    engagements_repository: EngagementsRepository,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, int | str | bool | list[dict[str, Any]]]:
    """Notify participants whose BioAI or blood report is ready for consultation booking."""
    participants = await engagements_repository.list_participants_for_consultation_notification(db)

    matched = len(participants)
    sent = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []
    reference_date = (as_of or datetime.now(_IST).date()).isoformat()

    if dry_run:
        for user_id, engagement_id, raw_keys in participants:
            keys = [k.strip() for k in (raw_keys or "").split(",") if k.strip()]
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "service_key": ",".join(keys) if keys else None,
                "action": "dry_run",
                "reason": "no notification keys configured" if not keys else "would dispatch",
            })
        return {
            "as_of": reference_date,
            "matched": matched,
            "sent": 0,
            "skipped": matched,
            "failed": 0,
            "dry_run": True,
            "details": details,
        }

    for user_id, engagement_id, raw_keys in participants:
        service_keys = [k.strip() for k in (raw_keys or "").split(",") if k.strip()]
        if not service_keys:
            skipped += 1
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "action": "skipped",
                "reason": "no notification keys configured",
            })
            continue

        try:
            dispatched_any = False
            skipped_all = True
            for sk in service_keys:
                skip_reason = await should_skip_notification(
                    db,
                    service_key=sk,
                    user_id=user_id,
                    engagement_id=engagement_id,
                )
                if skip_reason:
                    details.append({
                        "user_id": user_id,
                        "engagement_id": engagement_id,
                        "service_key": sk,
                        "action": "skipped",
                        "reason": f"notification '{sk}' {skip_reason}",
                    })
                    continue

                skipped_all = False
                await notifications_service.dispatch(
                    db,
                    payload=DispatchRequest(
                        service_key=sk,
                        user_ids=[user_id],
                        engagement_id=engagement_id,
                    ),
                    triggered_by_user_id=None,
                )
                dispatched_any = True
                details.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "service_key": sk,
                    "action": "sent",
                    "reason": f"dispatched '{sk}'",
                })

            if dispatched_any:
                sent += 1
            elif skipped_all:
                skipped += 1
        except Exception as exc:
            failed += 1
            details.append({
                "user_id": user_id,
                "engagement_id": engagement_id,
                "service_key": ",".join(service_keys),
                "action": "failed",
                "reason": str(exc),
            })
            logger.warning(
                "Consultation notification dispatch failed: service_keys=%s user_id=%s engagement_id=%s: %s",
                ",".join(service_keys),
                user_id,
                engagement_id,
                str(exc),
            )

    return {
        "as_of": reference_date,
        "matched": matched,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "dry_run": False,
        "details": details,
    }
