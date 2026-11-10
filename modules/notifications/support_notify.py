"""Dispatch support-query notifications to default onboarding assistants (staff employees)."""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.engine import get_notification_engine
from modules.support.models import SupportTicket

logger = logging.getLogger(__name__)


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
    platform_settings_repository=None,
    ticket: SupportTicket,
    user,
) -> None:
    """Emit support.ticket_created for default staff assistants."""
    _ = notifications_service
    _ = notifications_repository
    _ = platform_settings_repository
    details = participant_details_from_support_ticket(user, ticket=ticket)
    try:
        await get_notification_engine().emit(
            db,
            event_code="support.ticket_created",
            context={
                "participant_details": details,
                "user_id": ticket.user_id,
            },
        )
    except Exception as exc:
        logger.warning(
            "Support notification failed for ticket_id=%s: %s",
            ticket.ticket_id,
            str(exc),
        )
