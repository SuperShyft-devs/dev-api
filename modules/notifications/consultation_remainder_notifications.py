"""Dispatch consultation remainder notifications on the day of a scheduled consultation."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.repository import ConsultationRemainderParticipant, EngagementsRepository
from modules.notifications.dedup import should_skip_notification_on_date
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")


def today_in_ist(*, as_of: date | None = None) -> date:
    """Return as_of or today in IST."""
    if as_of is not None:
        return as_of
    return datetime.now(_IST).date()


def _session_details_from_participant(
    participant: ConsultationRemainderParticipant,
) -> SessionDetails:
    return SessionDetails(
        want=participant.want,
        date=participant.consultation_date,
        slot=participant.consultation_slot or "",
        expert_type=participant.expert_type,
    )


async def dispatch_consultation_remainder_notifications(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    engagements_repository: EngagementsRepository,
    as_of: date | None = None,
    dry_run: bool = False,
) -> dict[str, int | str | bool | list[dict[str, Any]]]:
    """Notify participants with a consultation scheduled for today (IST)."""
    consultation_date = today_in_ist(as_of=as_of)
    participants = await engagements_repository.list_participants_for_consultation_remainder(
        db,
        consultation_date=consultation_date,
    )

    matched = len(participants)
    sent = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []
    reference_date = consultation_date.isoformat()

    if dry_run:
        for participant in participants:
            details.append({
                "user_id": participant.user_id,
                "engagement_id": participant.engagement_id,
                "expert_type": participant.expert_type,
                "service_key": ",".join(participant.service_keys) if participant.service_keys else None,
                "action": "dry_run",
                "reason": "no notification keys configured" if not participant.service_keys else "would dispatch",
            })
        return {
            "as_of": reference_date,
            "consultation_date": reference_date,
            "matched": matched,
            "sent": 0,
            "skipped": matched,
            "failed": 0,
            "dry_run": True,
            "details": details,
        }

    for participant in participants:
        if not participant.service_keys:
            skipped += 1
            details.append({
                "user_id": participant.user_id,
                "engagement_id": participant.engagement_id,
                "expert_type": participant.expert_type,
                "action": "skipped",
                "reason": "no notification keys configured",
            })
            continue

        try:
            dispatched_any = False
            skipped_all = True
            session_details = _session_details_from_participant(participant)
            for sk in participant.service_keys:
                skip_reason = await should_skip_notification_on_date(
                    db,
                    service_key=sk,
                    user_id=participant.user_id,
                    engagement_id=participant.engagement_id,
                    reference_date=consultation_date,
                )
                if skip_reason:
                    details.append({
                        "user_id": participant.user_id,
                        "engagement_id": participant.engagement_id,
                        "expert_type": participant.expert_type,
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
                    ),
                    triggered_by_user_id=None,
                )
                dispatched_any = True
                details.append({
                    "user_id": participant.user_id,
                    "engagement_id": participant.engagement_id,
                    "expert_type": participant.expert_type,
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
                "expert_type": participant.expert_type,
                "service_key": ",".join(participant.service_keys),
                "action": "failed",
                "reason": str(exc),
            })
            logger.warning(
                "Consultation remainder dispatch failed: service_keys=%s user_id=%s engagement_id=%s expert_type=%s: %s",
                ",".join(participant.service_keys),
                participant.user_id,
                participant.engagement_id,
                participant.expert_type,
                str(exc),
            )

    return {
        "as_of": reference_date,
        "consultation_date": reference_date,
        "matched": matched,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "dry_run": False,
        "details": details,
    }
