"""Dispatch onboarding notifications to admin-role assistants when a user enrolls."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

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


def _parse_service_keys(raw: str | None) -> list[str]:
    return [k.strip() for k in (raw or "").split(",") if k.strip()]


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
    """Dispatch each configured onboarding notification service to admin-role assistants."""
    service_keys = _parse_service_keys(getattr(engagement, "onboarding_notification", None))
    if not service_keys:
        return

    assistant_user_ids = await engagements_repository.list_onboarding_assistant_user_ids(
        db, engagement_id=int(engagement.engagement_id)
    )
    if not assistant_user_ids:
        return

    details = _with_participant_user_id(participant_details, participant_user_id)
    engagement_id = int(engagement.engagement_id)

    for service_key in service_keys:
        try:
            svc = await notifications_repository.get_service_by_key(db, service_key=service_key)
            if svc is None:
                logger.warning(
                    "Onboarding notification skipped: service_key=%s not found (engagement_id=%s)",
                    service_key,
                    engagement_id,
                )
                continue
            if not svc.is_active:
                logger.warning(
                    "Onboarding notification skipped: service_key=%s inactive (engagement_id=%s)",
                    service_key,
                    engagement_id,
                )
                continue

            if svc.require_participant_detail and not participant_details:
                logger.warning(
                    "Onboarding notification skipped: service_key=%s requires participant_details "
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
            )
            await notifications_service.dispatch(
                db,
                payload=dispatch_payload,
                triggered_by_user_id=None,
            )
        except Exception as exc:
            logger.warning(
                "Onboarding assistant notification failed for engagement_id=%s service_key=%s: %s",
                engagement_id,
                service_key,
                str(exc),
            )
