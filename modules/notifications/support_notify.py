"""Dispatch support-query notifications to default onboarding assistants (staff employees)."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.engine import get_notification_engine
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.support.models import SupportTicket

logger = logging.getLogger(__name__)


def _parse_service_keys(raw: str | None) -> list[str]:
    return [k.strip() for k in (raw or "").split(",") if k.strip()]


def participant_details_from_support_ticket(
    user,
    *,
    ticket: SupportTicket,
) -> dict[str, str]:
    first_name = getattr(user, "first_name", None) or ""
    last_name = getattr(user, "last_name", None) or ""
    name = f"{first_name} {last_name}".strip()
    return {
        "name": name,
        "email": str(getattr(user, "email", None) or ""),
        "phone": str(getattr(user, "phone", None) or ""),
        "contact_input": str(ticket.contact_input or ""),
        "query_text": str(ticket.query_text or ""),
        "ticket_id": str(ticket.ticket_id),
        "participant_user_id": str(ticket.user_id or getattr(user, "user_id", "") or ""),
    }


async def notify_default_onboarding_assistants_on_support_query(
    db: AsyncSession,
    *,
    notifications_service=None,
    notifications_repository=None,
    platform_settings_repository: PlatformSettingsRepository | None = None,
    ticket: SupportTicket,
    user,
) -> None:
    """Emit support.ticket_created for default staff assistants."""
    _ = notifications_service
    _ = notifications_repository
    platform_settings_repository = platform_settings_repository or PlatformSettingsRepository()
    settings_row = await platform_settings_repository.get_by_id(db)
    fallback_keys = _parse_service_keys(
        getattr(settings_row, "default_support_query_notification", None) if settings_row else None
    )
    if not fallback_keys:
        return

    details = participant_details_from_support_ticket(user, ticket=ticket)
    try:
        await get_notification_engine().emit(
            db,
            event_code="support.ticket_created",
            context={
                "participant_details": details,
                "user_id": ticket.user_id,
                "fallback_service_keys": fallback_keys,
            },
        )
    except Exception as exc:
        logger.warning(
            "Support notification failed for ticket_id=%s: %s",
            ticket.ticket_id,
            str(exc),
        )
