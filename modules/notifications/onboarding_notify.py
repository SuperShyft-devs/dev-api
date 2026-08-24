"""Dispatch onboarding notifications to admin-role assistants when a user enrolls."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.repository import EngagementsRepository
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

# DispatchRequest.validate_nested_strings rejects empty strings; use a hyphen placeholder.
_MISSING_DETAIL = "-"


def detail_or_hyphen(value: object | None) -> str:
    """Return a non-empty string for notification participant_details fields."""
    if value is None:
        return _MISSING_DETAIL
    text = str(value).strip()
    return text if text else _MISSING_DETAIL


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
        "name": detail_or_hyphen(name),
        "email": detail_or_hyphen(getattr(user, "email", None)),
        "phone": detail_or_hyphen(getattr(user, "phone", None)),
        "engagement": detail_or_hyphen(source),
        "participant_user_id": str(participant_user_id),
        "age": detail_or_hyphen(getattr(user, "age", None)),
        "gender": detail_or_hyphen(getattr(user, "gender", None)),
        "address": detail_or_hyphen(getattr(user, "address", None)),
        "pincode": detail_or_hyphen(
            getattr(user, "pin_code", None) or getattr(user, "pincode", None)
        ),
        "collection_date": detail_or_hyphen(collection_date),
        "collection_time": detail_or_hyphen(collection_time),
    }
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
    """Dispatch each configured onboarding notification service to admin-role assistants."""
    en_repo = EngagementNotificationsRepository()
    service_keys = await en_repo.get_services_for_engagement_event(
        db, engagement_id=int(engagement.engagement_id), event_code="onboarding"
    )
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
