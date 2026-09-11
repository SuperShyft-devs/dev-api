"""Dispatch booking guide notifications to assigned phlebo partners day before blood collection."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.repository import EngagementsRepository
from modules.notifications.pretest_reminders import tomorrow_in_ist
from modules.notifications.service import NotificationsService
from modules.partners.models import PartnerRole

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")
_BOOKING_GUIDE_PARTNER_ROLES = frozenset({PartnerRole.phlebo.value})


async def dispatch_booking_guide_reminders(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    engagements_repository: EngagementsRepository,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, int | str | bool | list[dict[str, Any]]]:
    """Find engagements with blood collection tomorrow and notify assigned phlebo partners."""
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
            contacts = await engagements_repository.list_onboarding_assistant_partner_contacts(
                db,
                engagement_id=engagement_id,
                roles=_BOOKING_GUIDE_PARTNER_ROLES,
            )
            service_keys = [cfg.service_key for cfg in service_configs]
            for contact in contacts:
                matched += 1
                details.append({
                    "partner_id": contact.get("partner_id"),
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
        contacts = await engagements_repository.list_onboarding_assistant_partner_contacts(
            db,
            engagement_id=engagement_id,
            roles=_BOOKING_GUIDE_PARTNER_ROLES,
        )
        if not contacts:
            continue

        service_keys = [cfg.service_key for cfg in service_configs]
        if not service_configs:
            for contact in contacts:
                matched += 1
                skipped += 1
                details.append({
                    "partner_id": contact.get("partner_id"),
                    "engagement_id": engagement_id,
                    "action": "skipped",
                    "reason": "no notification keys configured",
                })
            continue

        for contact in contacts:
            matched += 1
            partner_id = contact.get("partner_id")
            try:
                dispatched_any = False
                skipped_all = True
                for cfg in service_configs:
                    sk = cfg.service_key
                    skipped_all = False
                    await notifications_service.dispatch_to_contacts(
                        db,
                        service_key=sk,
                        contacts=[contact],
                        engagement_id=engagement_id,
                    )
                    dispatched_any = True
                    details.append({
                        "partner_id": partner_id,
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
                    "partner_id": partner_id,
                    "engagement_id": engagement_id,
                    "service_key": ",".join(service_keys),
                    "action": "failed",
                    "reason": str(exc),
                })
                logger.warning(
                    "Booking guide reminder dispatch failed: service_keys=%s partner_id=%s engagement_id=%s: %s",
                    ",".join(service_keys),
                    partner_id,
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
