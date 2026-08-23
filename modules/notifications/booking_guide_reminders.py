"""Dispatch booking guide notifications to onboarding assistants day before blood collection."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.models import EmployeeRole
from modules.engagements.repository import EngagementsRepository
from modules.notifications.dedup import should_skip_notification
from modules.notifications.pretest_reminders import tomorrow_in_ist
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
_BOOKING_GUIDE_OA_ROLES = frozenset({EmployeeRole.admin, EmployeeRole.onboarding_assistant})


async def dispatch_booking_guide_reminders(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    engagements_repository: EngagementsRepository,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, int | str | bool | list[dict[str, Any]]]:
    """Find engagements with blood collection tomorrow and notify assigned onboarding assistants."""
    collection_date = tomorrow_in_ist(as_of=as_of)
    engagements = await engagements_repository.list_engagements_for_booking_guide_reminder(
        db,
        collection_date=collection_date,
    )

    matched = 0
    sent = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    if dry_run:
        for engagement_id, service_configs in engagements:
            assistant_user_ids = await engagements_repository.list_onboarding_assistant_user_ids(
                db,
                engagement_id=engagement_id,
                roles=_BOOKING_GUIDE_OA_ROLES,
            )
            service_keys = [cfg.service_key for cfg in service_configs]
            for user_id in assistant_user_ids:
                matched += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "service_key": ",".join(service_keys) if service_keys else None,
                    "action": "dry_run",
                    "reason": "no notification keys configured" if not service_keys else "would dispatch",
                })
        return {
            "as_of": (as_of or datetime.now(_IST).date()).isoformat(),
            "collection_date": collection_date.isoformat(),
            "matched": matched,
            "sent": 0,
            "skipped": matched,
            "failed": 0,
            "dry_run": True,
            "details": details,
        }

    for engagement_id, service_configs in engagements:
        assistant_user_ids = await engagements_repository.list_onboarding_assistant_user_ids(
            db,
            engagement_id=engagement_id,
            roles=_BOOKING_GUIDE_OA_ROLES,
        )
        if not assistant_user_ids:
            continue

        service_keys = [cfg.service_key for cfg in service_configs]
        if not service_configs:
            for user_id in assistant_user_ids:
                matched += 1
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": engagement_id,
                    "action": "skipped",
                    "reason": "no notification keys configured",
                })
            continue

        for user_id in assistant_user_ids:
            matched += 1
            try:
                dispatched_any = False
                skipped_all = True
                for cfg in service_configs:
                    sk = cfg.service_key
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
                            external_link=cfg.external_link,
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
                    "Booking guide reminder dispatch failed: service_keys=%s user_id=%s engagement_id=%s: %s",
                    ",".join(service_keys),
                    user_id,
                    engagement_id,
                    str(exc),
                )

    return {
        "as_of": (as_of or datetime.now(_IST).date()).isoformat(),
        "collection_date": collection_date.isoformat(),
        "matched": matched,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "dry_run": False,
        "details": details,
    }
