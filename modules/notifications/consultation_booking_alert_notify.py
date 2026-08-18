"""Dispatch consultation booking alerts to admin- and expert-role onboarding assistants."""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.models import EmployeeRole
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.repository import EngagementsRepository
from modules.notifications.onboarding_notify import (
    participant_details_from_user,
    _with_participant_user_id,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_CONSULTATION_BOOKING_ALERT_OA_ROLES = frozenset({EmployeeRole.admin, EmployeeRole.expert})


async def notify_onboarding_assistants_on_consultation_booking(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    notifications_repository: NotificationsRepository,
    engagements_repository: EngagementsRepository,
    engagement,
    participant_user,
    participant_user_id: int,
    expert_type: str,
    consultation_date: date,
    consultation_slot: str,
) -> None:
    """Dispatch each configured consultation_booking_alert service to admin/expert OAs."""
    if not consultation_date or not (consultation_slot or "").strip():
        return

    en_repo = EngagementNotificationsRepository()
    service_keys = await en_repo.get_services_for_engagement_event(
        db,
        engagement_id=int(engagement.engagement_id),
        event_code="consultation_booking_alert",
    )
    if not service_keys:
        return

    assistant_user_ids = await engagements_repository.list_onboarding_assistant_user_ids(
        db,
        engagement_id=int(engagement.engagement_id),
        roles=_CONSULTATION_BOOKING_ALERT_OA_ROLES,
    )
    if not assistant_user_ids:
        return

    participant_details = participant_details_from_user(
        participant_user,
        source="consultation_booking",
        participant_user_id=participant_user_id,
    )
    details = _with_participant_user_id(participant_details, participant_user_id)
    session_details = SessionDetails(
        want=True,
        date=consultation_date,
        slot=consultation_slot,
        expert_type=expert_type,
    )
    engagement_id = int(engagement.engagement_id)

    for service_key in service_keys:
        try:
            svc = await notifications_repository.get_service_by_key(db, service_key=service_key)
            if svc is None:
                logger.warning(
                    "Consultation booking alert skipped: service_key=%s not found (engagement_id=%s)",
                    service_key,
                    engagement_id,
                )
                continue
            if not svc.is_active:
                logger.warning(
                    "Consultation booking alert skipped: service_key=%s inactive (engagement_id=%s)",
                    service_key,
                    engagement_id,
                )
                continue
            if svc.require_participant_detail and not details:
                logger.warning(
                    "Consultation booking alert skipped: service_key=%s requires participant_details "
                    "(engagement_id=%s)",
                    service_key,
                    engagement_id,
                )
                continue

            dispatch_payload = DispatchRequest(
                service_key=service_key,
                user_ids=assistant_user_ids,
                engagement_id=engagement_id,
                participant_details=details,
                session_details=session_details,
            )
            await notifications_service.dispatch(
                db,
                payload=dispatch_payload,
                triggered_by_user_id=None,
            )
        except Exception as exc:
            logger.warning(
                "Consultation booking alert failed for engagement_id=%s service_key=%s: %s",
                engagement_id,
                service_key,
                str(exc),
            )
