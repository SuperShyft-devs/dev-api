"""Fire-and-forget notifications to onboarding assistants when a user enrolls."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.constants import DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY
from modules.engagements.repository import EngagementsRepository
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)


def participant_details_from_user(
    user,
    *,
    source: str,
    participant_user_id: int,
    collection_date: str | None = None,
    collection_time: str | None = None,
) -> dict[str, str]:
    first_name = getattr(user, "first_name", None) or ""
    last_name = getattr(user, "last_name", None) or ""
    name = f"{first_name} {last_name}".strip()
    details: dict[str, str] = {
        "name": name,
        "email": str(getattr(user, "email", None) or ""),
        "phone": str(getattr(user, "phone", None) or ""),
        "engagement": source,
        "participant_user_id": str(participant_user_id),
    }
    if collection_date is not None:
        details["collection_date"] = collection_date
    if collection_time is not None:
        details["collection_time"] = collection_time
    age = getattr(user, "age", None)
    if age is not None:
        details["age"] = str(age)
    gender = getattr(user, "gender", None)
    if gender:
        details["gender"] = str(gender)
    address = getattr(user, "address", None)
    if address:
        details["address"] = str(address)
    pincode = getattr(user, "pin_code", None) or getattr(user, "pincode", None)
    if pincode:
        details["pincode"] = str(pincode)
    return details


def _with_participant_user_id(
    participant_details: dict[str, str] | None,
    participant_user_id: int,
) -> dict[str, str] | None:
    if participant_details is None:
        return None
    return {**participant_details, "participant_user_id": str(participant_user_id)}


async def notify_onboarding_assistants_on_enrollment(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    notifications_repository: NotificationsRepository,
    engagements_repository: EngagementsRepository,
    engagement,
    participant_user_id: int,
    participant_details: dict[str, str] | None,
) -> None:
    """Dispatch the engagement's notification service to each onboarding assistant."""
    service_key = (getattr(engagement, "notification_service_key", None) or "").strip()
    if not service_key:
        service_key = DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY

    try:
        svc = await notifications_repository.get_service_by_key(db, service_key=service_key)
        if svc is None:
            logger.warning(
                "Onboarding notification skipped: service_key=%s not found (engagement_id=%s)",
                service_key,
                getattr(engagement, "engagement_id", None),
            )
            return
        if not svc.is_active:
            logger.warning(
                "Onboarding notification skipped: service_key=%s inactive (engagement_id=%s)",
                service_key,
                getattr(engagement, "engagement_id", None),
            )
            return

        if svc.require_participant_detail and not participant_details:
            logger.warning(
                "Onboarding notification skipped: service_key=%s requires participant_details "
                "(engagement_id=%s)",
                service_key,
                getattr(engagement, "engagement_id", None),
            )
            return

        assistant_user_ids = await engagements_repository.list_onboarding_assistant_user_ids(
            db, engagement_id=int(engagement.engagement_id)
        )
        if not assistant_user_ids:
            return

        details = _with_participant_user_id(participant_details, participant_user_id)

        engagement_id = int(engagement.engagement_id)
        record_id: str | None = None
        participant_instance = await notifications_repository.get_metsights_instance_for_user_engagement(
            db,
            user_id=participant_user_id,
            engagement_id=engagement_id,
        )
        if participant_instance is not None:
            record_id = (participant_instance.metsights_record_id or "").strip() or None

        for uid in assistant_user_ids:
            try:
                dispatch_payload = DispatchRequest(
                    service_key=service_key,
                    user_id=uid,
                    engagement_id=engagement_id,
                    record_id=record_id,
                    participant_details=details,
                )
                await notifications_service.dispatch(
                    db,
                    payload=dispatch_payload,
                    triggered_by_user_id=uid,
                )
            except Exception as exc:
                logger.warning(
                    "Onboarding assistant notification failed for user_id=%s engagement_id=%s: %s",
                    uid,
                    engagement.engagement_id,
                    str(exc),
                )
    except Exception as exc:
        logger.warning(
            "Onboarding assistant notification batch failed for engagement_id=%s: %s",
            getattr(engagement, "engagement_id", None),
            str(exc),
        )
