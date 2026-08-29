"""Dispatch pretest blood-collection reminders for participants with collection tomorrow."""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.repository import EngagementsRepository, PretestReminderParticipant
from modules.notifications.dedup import should_skip_notification
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")


def tomorrow_in_ist(*, as_of: date | None = None) -> date:
    """Return the calendar day after as_of (or today in IST when as_of is omitted)."""
    if as_of is not None:
        return as_of + timedelta(days=1)
    today_ist = datetime.now(_IST).date()
    return today_ist + timedelta(days=1)


def format_blood_collection_slot(slot_start_time: time | None) -> str:
    """Format participant slot_start_time for session_details / message templates."""
    if slot_start_time is None:
        return ""
    return (
        datetime.combine(date(2000, 1, 1), slot_start_time)
        .strftime("%I:%M %p")
        .lstrip("0")
    )


def session_details_from_pretest_participant(
    participant: PretestReminderParticipant,
) -> SessionDetails | None:
    slot = format_blood_collection_slot(participant.slot_start_time)
    if not slot:
        return None
    cabin = (participant.blood_collection_cabin or "").strip() or None
    return SessionDetails(
        want=True,
        date=participant.engagement_date,
        slot=slot,
        expert_type="blood_collection",
        cabin=cabin,
    )


async def dispatch_pretest_reminders(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    engagements_repository: EngagementsRepository,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, int | str | bool | list[dict[str, Any]]]:
    """Find participants with collection tomorrow and dispatch pretest guideline notifications."""
    collection_date = tomorrow_in_ist(as_of=as_of)
    participants = await engagements_repository.list_participants_for_pretest_reminder(
        db,
        collection_date=collection_date,
    )

    matched = len(participants)
    sent = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    if dry_run:
        for participant in participants:
            service_keys = participant.service_keys
            details.append({
                "user_id": participant.user_id,
                "engagement_id": participant.engagement_id,
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

    for participant in participants:
        service_keys = participant.service_keys
        if not participant.service_configs:
            skipped += 1
            details.append({
                "user_id": participant.user_id,
                "engagement_id": participant.engagement_id,
                "action": "skipped",
                "reason": "no notification keys configured",
            })
            continue

        session_details = session_details_from_pretest_participant(participant)
        if session_details is None:
            skipped += 1
            details.append({
                "user_id": participant.user_id,
                "engagement_id": participant.engagement_id,
                "action": "skipped",
                "reason": "missing slot_start_time for session_details",
            })
            continue

        try:
            dispatched_any = False
            skipped_all = True
            for cfg in participant.service_configs:
                sk = cfg.service_key
                skip_reason = await should_skip_notification(
                    db,
                    service_key=sk,
                    user_id=participant.user_id,
                    engagement_id=participant.engagement_id,
                )
                if skip_reason:
                    details.append({
                        "user_id": participant.user_id,
                        "engagement_id": participant.engagement_id,
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
                        user_ids=[participant.user_id],
                        engagement_id=participant.engagement_id,
                        session_details=session_details,
                        external_link=cfg.external_link,
                    ),
                    triggered_by_user_id=None,
                )
                dispatched_any = True
                details.append({
                    "user_id": participant.user_id,
                    "engagement_id": participant.engagement_id,
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
                "user_id": participant.user_id,
                "engagement_id": participant.engagement_id,
                "service_key": ",".join(service_keys),
                "action": "failed",
                "reason": str(exc),
            })
            logger.warning(
                "Pretest reminder dispatch failed: service_keys=%s user_id=%s engagement_id=%s: %s",
                ",".join(service_keys),
                participant.user_id,
                participant.engagement_id,
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
