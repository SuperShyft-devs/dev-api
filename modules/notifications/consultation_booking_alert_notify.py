"""Dispatch consultation booking alerts to mode-aware partner recipients."""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.consultation_booking_validation import effective_consultation_mode
from modules.engagements.enums import ConsultationMode
from modules.engagements.models import OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.engagements.slot_availability import resolve_consultation_cabin_display_name
from modules.experts.repository import ExpertsRepository
from modules.notifications.onboarding_notify import (
    participant_details_from_user,
    _with_participant_user_id,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import SessionDetails
from modules.notifications.service import NotificationsService
from modules.partners.models import PartnerRole
from modules.partners.repository import PartnersRepository

logger = logging.getLogger(__name__)

_PHLEBO_ROLES = frozenset({PartnerRole.phlebo.value})
_EXPERT_ROLES = frozenset({PartnerRole.expert.value})


def _dedupe_contacts(contacts: list[dict]) -> list[dict]:
    seen: set[int] = set()
    ordered: list[dict] = []
    for contact in contacts:
        partner_id = contact.get("partner_id")
        if not isinstance(partner_id, int) or partner_id in seen:
            continue
        seen.add(partner_id)
        ordered.append(contact)
    return ordered


def _partner_to_contact(partner) -> dict:
    name = (partner.name or "").strip()
    parts = name.split(None, 1)
    return {
        "partner_id": int(partner.partner_id),
        "name": name,
        "first_name": parts[0] if parts else "",
        "last_name": parts[1] if len(parts) > 1 else "",
        "phone": partner.phone,
        "email": partner.email,
        "role": partner.role,
        "status": partner.status,
    }


async def _list_phlebo_contacts(
    db: AsyncSession,
    *,
    engagements_repository: EngagementsRepository,
    engagement_id: int,
) -> list[dict]:
    return await engagements_repository.list_onboarding_assistant_partner_contacts(
        db,
        engagement_id=engagement_id,
        roles=_PHLEBO_ROLES,
    )


async def _ensure_expert_partner_assigned(
    db: AsyncSession,
    *,
    engagement_id: int,
    partner_id: int,
    engagements_repository: EngagementsRepository,
) -> None:
    existing = await engagements_repository.get_onboarding_assistant_assignment(
        db,
        engagement_id=engagement_id,
        partner_id=partner_id,
    )
    if existing is not None:
        return
    assignment = OnboardingAssistantAssignment(
        engagement_id=engagement_id,
        partner_id=partner_id,
    )
    await engagements_repository.create_onboarding_assistant_assignment(db, assignment)


async def resolve_consultation_booking_alert_contacts(
    db: AsyncSession,
    *,
    engagement,
    expert_type: str,
    expert_id: int | None,
    engagements_repository: EngagementsRepository,
    experts_repository: ExpertsRepository,
    partners_repository: PartnersRepository | None = None,
) -> list[dict]:
    engagement_id = int(engagement.engagement_id)
    partners_repo = partners_repository or PartnersRepository()
    mode = effective_consultation_mode(engagement)

    phlebo_contacts = await _list_phlebo_contacts(
        db,
        engagements_repository=engagements_repository,
        engagement_id=engagement_id,
    )

    if mode == ConsultationMode.offline:
        expert_contacts = await engagements_repository.list_onboarding_assistant_partner_contacts(
            db,
            engagement_id=engagement_id,
            roles=_EXPERT_ROLES,
        )
        return _dedupe_contacts(phlebo_contacts + expert_contacts)

    if expert_id is not None:
        expert = await experts_repository.get_by_id(db, expert_id)
        if expert is None:
            logger.warning(
                "Consultation booking alert: expert_id=%s not found (engagement_id=%s)",
                expert_id,
                engagement_id,
            )
            return _dedupe_contacts(phlebo_contacts)
        if (expert.status or "").lower() != "active":
            logger.warning(
                "Consultation booking alert: expert_id=%s inactive (engagement_id=%s)",
                expert_id,
                engagement_id,
            )
            return _dedupe_contacts(phlebo_contacts)
        if expert.expert_type != expert_type:
            logger.warning(
                "Consultation booking alert: expert_id=%s type mismatch expected=%s got=%s",
                expert_id,
                expert_type,
                expert.expert_type,
            )
            return _dedupe_contacts(phlebo_contacts)
        if expert.partner_id is None:
            logger.warning(
                "Consultation booking alert: expert_id=%s has no partner_id (engagement_id=%s)",
                expert_id,
                engagement_id,
            )
            return _dedupe_contacts(phlebo_contacts)

        partner = await partners_repo.get_by_id(db, int(expert.partner_id))
        if partner is None or (partner.status or "").lower() != "active":
            return _dedupe_contacts(phlebo_contacts)

        await _ensure_expert_partner_assigned(
            db,
            engagement_id=engagement_id,
            partner_id=int(partner.partner_id),
            engagements_repository=engagements_repository,
        )
        return _dedupe_contacts(phlebo_contacts + [_partner_to_contact(partner)])

    partner_ids = await experts_repository.list_active_partner_ids_by_type(db, expert_type=expert_type)
    type_contacts: list[dict] = []
    for partner_id in partner_ids:
        partner = await partners_repo.get_by_id(db, partner_id)
        if partner is None or (partner.status or "").lower() != "active":
            continue
        type_contacts.append(_partner_to_contact(partner))
    return _dedupe_contacts(phlebo_contacts + type_contacts)


async def resolve_consultation_booking_alert_user_ids(
    db: AsyncSession,
    *,
    engagement,
    expert_type: str,
    expert_id: int | None,
    engagements_repository: EngagementsRepository,
    experts_repository: ExpertsRepository,
    employee_repository=None,
) -> list[int]:
    """Deprecated: alerts dispatch to partner contacts. Returns empty list."""
    _ = db, engagement, expert_type, expert_id, engagements_repository, experts_repository, employee_repository
    return []


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
    consultation_cabin: str | None = None,
    expert_id: int | None = None,
    experts_repository: ExpertsRepository | None = None,
    employee_repository=None,
) -> None:
    """Dispatch each configured consultation_booking_alert service to mode-aware partners."""
    _ = employee_repository
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

    experts_repo = experts_repository or ExpertsRepository()
    contacts = await resolve_consultation_booking_alert_contacts(
        db,
        engagement=engagement,
        expert_type=expert_type,
        expert_id=expert_id,
        engagements_repository=engagements_repository,
        experts_repository=experts_repo,
    )
    if not contacts:
        return

    participant_details = participant_details_from_user(
        participant_user,
        source="consultation_booking",
        participant_user_id=participant_user_id,
    )
    details = _with_participant_user_id(participant_details, participant_user_id)
    slot_detail = None
    if getattr(engagement, "slot_detail_id", None) is not None:
        from modules.engagements.slot_info_repository import EngagementSlotInfoRepository

        slot_detail = await EngagementSlotInfoRepository().get_by_id(db, int(engagement.slot_detail_id))
    cabin = resolve_consultation_cabin_display_name(
        slot_detail,
        consultation_date=consultation_date,
        cabin_key=consultation_cabin,
    )
    session_details = SessionDetails(
        want=True,
        date=consultation_date,
        slot=consultation_slot,
        expert_type=expert_type,
        cabin=cabin,
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

            await notifications_service.dispatch_to_contacts(
                db,
                service_key=service_key,
                contacts=contacts,
                engagement_id=engagement_id,
                participant_details=details,
                session_details=session_details,
            )
        except Exception as exc:
            logger.warning(
                "Consultation booking alert failed for engagement_id=%s service_key=%s: %s",
                engagement_id,
                service_key,
                str(exc),
            )
