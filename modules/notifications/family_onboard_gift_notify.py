"""Diwali family-onboard gift notifications (booker + receiver, WhatsApp + email)."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.bioai_report.client import BioAiReportsClient, extract_gift_card_url
from modules.bioai_report.pdf_registration import bioreport_gift_card_endpoint
from modules.notifications.dedup import should_skip_notification
from modules.notifications.onboarding_notify import detail_or_hyphen
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService

if TYPE_CHECKING:
    from modules.users.models import User

logger = logging.getLogger(__name__)

GIFT_URL = "https://app.supershyft.com"
DEFAULT_GIFT_CARD_MESSAGE = "Wishing you joy and light."
GIFT_CARD_OCCASION = "diwali"

SENDER_WHATSAPP_KEY = "family-onboard-sender-whatsapp"
SENDER_EMAIL_KEY = "family-onboard-sender-email"
RECEIVER_WHATSAPP_KEY = "family-onboard-receiver-whatsapp"
RECEIVER_EMAIL_KEY = "family-onboard-receiver-email"

_ALL_KEYS: tuple[str, ...] = (
    SENDER_WHATSAPP_KEY,
    SENDER_EMAIL_KEY,
    RECEIVER_WHATSAPP_KEY,
    RECEIVER_EMAIL_KEY,
)


def _display_name(user: User) -> str:
    first = (getattr(user, "first_name", None) or "").strip()
    last = (getattr(user, "last_name", None) or "").strip()
    name = f"{first} {last}".strip()
    return name if name else "there"


def _format_relationship(relation: str | None) -> str:
    raw = (relation or "").strip().replace("_", " ")
    if not raw:
        return "-"
    return raw.title()


def _is_booker_plus_alias_email(recipient_email: str | None, booker_email: str | None) -> bool:
    """True when recipient email is a `{booker_local}+NNNN@domain` alias."""
    if not recipient_email or not booker_email or "@" not in recipient_email or "@" not in booker_email:
        return False
    r_local, r_domain = recipient_email.split("@", 1)
    b_local, b_domain = booker_email.split("@", 1)
    if r_domain.lower() != b_domain.lower():
        return False
    return bool(re.fullmatch(rf"{re.escape(b_local)}\+\d{{4}}", r_local, flags=re.IGNORECASE))


def _participant_details(
    *,
    booker: User,
    recipient: User,
    relationship: str | None,
    gift_url: str = GIFT_URL,
) -> dict[str, str]:
    return {
        "booker_name": detail_or_hyphen(_display_name(booker)),
        "booker_phone": detail_or_hyphen((getattr(booker, "phone", None) or "").strip()),
        "recipient_name": detail_or_hyphen(_display_name(recipient)),
        "relationship": detail_or_hyphen(_format_relationship(relationship)),
        "gift_url": gift_url,
    }


def _gift_card_message(booker_message: str | None) -> str:
    text = (booker_message or "").strip()
    return text if text else DEFAULT_GIFT_CARD_MESSAGE


async def _resolve_receiver_gift_card_url(
    db: AsyncSession,
    *,
    booker: User,
    recipient: User,
    engagement_id: int,
    relationship: str | None,
    booker_message: str | None,
    bio_ai_reports_client: BioAiReportsClient | None = None,
) -> str:
    client = bio_ai_reports_client or BioAiReportsClient()
    payload = {
        "sender_name": _display_name(booker),
        "receiver_name": _display_name(recipient),
        "relation": _format_relationship(relationship),
        "message": _gift_card_message(booker_message),
        "occasion": GIFT_CARD_OCCASION,
    }
    try:
        response = await tracked_integration_call(
            db,
            provider="bio_ai_reports",
            api_url=bioreport_gift_card_endpoint(),
            engagement_id=int(engagement_id),
            user_id=int(recipient.user_id),
            request_payload=payload,
            operation=lambda: client.create_gift_card(payload),
            reraise=True,
        )
        if not isinstance(response, dict):
            raise AppError(
                status_code=502,
                error_code="BIO_AI_REPORTS_ERROR",
                message="bio-ai-reports returned an invalid response",
            )
        return extract_gift_card_url(response)
    except AppError as exc:
        logger.warning(
            "Family onboard gift card URL fallback: booker=%s recipient=%s: %s",
            getattr(booker, "user_id", None),
            getattr(recipient, "user_id", None),
            exc,
        )
        return GIFT_URL
    except Exception as exc:
        logger.warning(
            "Family onboard gift card URL fallback: booker=%s recipient=%s: %s",
            getattr(booker, "user_id", None),
            getattr(recipient, "user_id", None),
            exc,
        )
        return GIFT_URL


def _should_skip_receiver_email(recipient: User, booker: User) -> bool:
    email = (getattr(recipient, "email", None) or "").strip()
    if not email:
        return True
    return _is_booker_plus_alias_email(email, getattr(booker, "email", None))


async def _dispatch_one(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    notifications_repository: NotificationsRepository,
    service_key: str,
    user_id: int,
    engagement_id: int,
    participant_details: dict[str, str],
) -> None:
    try:
        svc = await notifications_repository.get_service_by_key(db, service_key=service_key)
        if svc is None:
            logger.warning(
                "Family onboard gift skipped: service_key=%s not found (engagement_id=%s)",
                service_key,
                engagement_id,
            )
            return
        if not svc.is_active:
            logger.warning(
                "Family onboard gift skipped: service_key=%s inactive (engagement_id=%s)",
                service_key,
                engagement_id,
            )
            return

        skip_reason = await should_skip_notification(
            db,
            service_key=service_key,
            user_id=user_id,
            engagement_id=engagement_id,
            repository=notifications_repository,
        )
        if skip_reason:
            logger.info(
                "Family onboard gift skipped: service_key=%s user=%s engagement=%s reason=%s",
                service_key,
                user_id,
                engagement_id,
                skip_reason,
            )
            return

        await notifications_service.dispatch(
            db,
            payload=DispatchRequest(
                service_key=service_key,
                user_ids=[user_id],
                engagement_id=engagement_id,
                participant_details=participant_details,
            ),
            triggered_by_user_id=None,
        )
    except Exception as exc:
        logger.warning(
            "Family onboard gift failed: service_key=%s user=%s engagement=%s: %s",
            service_key,
            user_id,
            engagement_id,
            exc,
        )


async def notify_family_onboard_gift(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService | None,
    booker: User,
    recipient: User,
    engagement_id: int,
    relationship: str | None,
    booker_message: str | None = None,
    bio_ai_reports_client: BioAiReportsClient | None = None,
) -> None:
    """Best-effort gift notifications after family-onboard payment confirm."""
    if notifications_service is None:
        return

    repo = notifications_service._repo
    eid = int(engagement_id)
    base_details = _participant_details(booker=booker, recipient=recipient, relationship=relationship)
    receiver_gift_url = await _resolve_receiver_gift_card_url(
        db,
        booker=booker,
        recipient=recipient,
        engagement_id=eid,
        relationship=relationship,
        booker_message=booker_message,
        bio_ai_reports_client=bio_ai_reports_client,
    )
    receiver_wa_details = {**base_details, "gift_url": receiver_gift_url}
    booker_id = int(booker.user_id)
    recipient_id = int(recipient.user_id)

    from modules.notifications.engine import get_notification_engine

    engine = get_notification_engine()
    await engine.emit(
        db,
        event_code="family.gift_sent",
        engagement_id=eid,
        recipient_types=["booker"],
        context={
            "booker_user_id": booker_id,
            "user_id": booker_id,
            "participant_details": base_details,
            "fallback_service_keys": [SENDER_WHATSAPP_KEY, SENDER_EMAIL_KEY],
        },
    )
    receiver_keys = [RECEIVER_WHATSAPP_KEY]
    if not _should_skip_receiver_email(recipient, booker):
        receiver_keys.append(RECEIVER_EMAIL_KEY)
    await engine.emit(
        db,
        event_code="family.gift_received",
        engagement_id=eid,
        recipient_types=["members"],
        context={
            "user_id": recipient_id,
            "user_ids": [recipient_id],
            "participant_details": receiver_wa_details,
            "fallback_service_keys": receiver_keys,
        },
    )


__all__ = [
    "DEFAULT_GIFT_CARD_MESSAGE",
    "GIFT_CARD_OCCASION",
    "GIFT_URL",
    "SENDER_WHATSAPP_KEY",
    "SENDER_EMAIL_KEY",
    "RECEIVER_WHATSAPP_KEY",
    "RECEIVER_EMAIL_KEY",
    "_ALL_KEYS",
    "_is_booker_plus_alias_email",
    "_should_skip_receiver_email",
    "_resolve_receiver_gift_card_url",
    "notify_family_onboard_gift",
]
