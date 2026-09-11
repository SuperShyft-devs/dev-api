"""Dispatch support-query notifications to default onboarding assistants (phlebo partners)."""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.partners.models import Partner
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


async def _resolve_default_assistant_contacts(
    db: AsyncSession,
    *,
    platform_settings_repository: PlatformSettingsRepository,
) -> list[dict]:
    """Default assistant IDs are partner_ids after migration 0133."""
    partner_ids = await platform_settings_repository.resolve_default_onboarding_assistant_employee_ids(db)
    if not partner_ids:
        return []

    result = await db.execute(
        select(Partner)
        .where(Partner.partner_id.in_(partner_ids))
        .where(Partner.status == "active")
    )
    contacts: list[dict] = []
    for partner in result.scalars().all():
        contacts.append(
            {
                "first_name": partner.name or "",
                "last_name": "",
                "phone": partner.phone or "",
                "email": partner.email or "",
            }
        )
    return contacts


async def notify_default_onboarding_assistants_on_support_query(
    db: AsyncSession,
    *,
    notifications_service: NotificationsService,
    notifications_repository: NotificationsRepository,
    platform_settings_repository: PlatformSettingsRepository,
    ticket: SupportTicket,
    user,
) -> None:
    """Dispatch each configured support notification service to default phlebo partners."""
    settings_row = await platform_settings_repository.get_by_id(db)
    service_keys = _parse_service_keys(
        getattr(settings_row, "default_support_query_notification", None) if settings_row else None
    )
    if not service_keys:
        return

    contacts = await _resolve_default_assistant_contacts(
        db, platform_settings_repository=platform_settings_repository
    )
    if not contacts:
        return

    details = participant_details_from_support_ticket(user, ticket=ticket)
    ticket_id = int(ticket.ticket_id)

    for service_key in service_keys:
        try:
            svc = await notifications_repository.get_service_by_key(db, service_key=service_key)
            if svc is None:
                logger.warning(
                    "Support notification skipped: service_key=%s not found (ticket_id=%s)",
                    service_key,
                    ticket_id,
                )
                continue
            if not svc.is_active:
                logger.warning(
                    "Support notification skipped: service_key=%s inactive (ticket_id=%s)",
                    service_key,
                    ticket_id,
                )
                continue

            if svc.require_participant_detail and not details:
                logger.warning(
                    "Support notification skipped: service_key=%s requires participant_details "
                    "(ticket_id=%s)",
                    service_key,
                    ticket_id,
                )
                continue

            await notifications_service.dispatch_to_contacts(
                db,
                service_key=service_key,
                contacts=contacts,
                participant_details=details,
            )
        except Exception as exc:
            logger.warning(
                "Support notification failed for ticket_id=%s service_key=%s: %s",
                ticket_id,
                service_key,
                str(exc),
            )
