"""Dispatch consultation booking alerts to mode-aware onboarding assistant recipients."""

from __future__ import annotations

import logging
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.models import Employee, EmployeeRole
from modules.employee.repository import EmployeeRepository
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.consultation_booking_validation import effective_consultation_mode
from modules.engagements.enums import ConsultationMode
from modules.engagements.models import OnboardingAssistantAssignment
from modules.engagements.repository import EngagementsRepository
from modules.experts.repository import ExpertsRepository
from modules.notifications.onboarding_notify import (
    participant_details_from_user,
    _with_participant_user_id,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

_ADMIN_OA_ROLES = frozenset({EmployeeRole.admin})
_EXPERT_OA_ROLES = frozenset({EmployeeRole.expert})


def _dedupe_user_ids(user_ids: list[int]) -> list[int]:
    seen: set[int] = set()
    ordered: list[int] = []
    for uid in user_ids:
        if uid in seen:
            continue
        seen.add(uid)
        ordered.append(uid)
    return ordered


async def _list_admin_oa_user_ids(
    db: AsyncSession,
    *,
    engagements_repository: EngagementsRepository,
    engagement_id: int,
) -> list[int]:
    return await engagements_repository.list_onboarding_assistant_user_ids(
        db,
        engagement_id=engagement_id,
        roles=_ADMIN_OA_ROLES,
    )


async def _ensure_expert_employee(db: AsyncSession, *, user_id: int, employee_repository: EmployeeRepository) -> Employee:
    existing = await employee_repository.get_by_user_id(db, user_id)
    if existing is not None:
        return existing
    row = Employee(
        user_id=user_id,
        role=EmployeeRole.expert,
        status="active",
    )
    return await employee_repository.create(db, row)


async def _ensure_expert_assigned_to_engagement(
    db: AsyncSession,
    *,
    engagement_id: int,
    employee_id: int,
    engagements_repository: EngagementsRepository,
) -> None:
    existing = await engagements_repository.get_onboarding_assistant_assignment(
        db,
        engagement_id=engagement_id,
        employee_id=employee_id,
    )
    if existing is not None:
        return
    assignment = OnboardingAssistantAssignment(
        engagement_id=engagement_id,
        employee_id=employee_id,
    )
    await engagements_repository.create_onboarding_assistant_assignment(db, assignment)


async def resolve_consultation_booking_alert_user_ids(
    db: AsyncSession,
    *,
    engagement,
    expert_type: str,
    expert_id: int | None,
    engagements_repository: EngagementsRepository,
    experts_repository: ExpertsRepository,
    employee_repository: EmployeeRepository,
) -> list[int]:
    engagement_id = int(engagement.engagement_id)
    mode = effective_consultation_mode(engagement)

    if mode == ConsultationMode.offline:
        admin_ids = await _list_admin_oa_user_ids(
            db,
            engagements_repository=engagements_repository,
            engagement_id=engagement_id,
        )
        expert_oa_ids = await engagements_repository.list_onboarding_assistant_user_ids(
            db,
            engagement_id=engagement_id,
            roles=_EXPERT_OA_ROLES,
        )
        return _dedupe_user_ids(admin_ids + expert_oa_ids)

    admin_ids = await _list_admin_oa_user_ids(
        db,
        engagements_repository=engagements_repository,
        engagement_id=engagement_id,
    )

    if expert_id is not None:
        expert = await experts_repository.get_by_id(db, expert_id)
        if expert is None:
            logger.warning(
                "Consultation booking alert: expert_id=%s not found (engagement_id=%s)",
                expert_id,
                engagement_id,
            )
            return _dedupe_user_ids(admin_ids)
        if (expert.status or "").lower() != "active":
            logger.warning(
                "Consultation booking alert: expert_id=%s inactive (engagement_id=%s)",
                expert_id,
                engagement_id,
            )
            return _dedupe_user_ids(admin_ids)
        if expert.expert_type != expert_type:
            logger.warning(
                "Consultation booking alert: expert_id=%s type mismatch expected=%s got=%s",
                expert_id,
                expert_type,
                expert.expert_type,
            )
            return _dedupe_user_ids(admin_ids)
        if expert.user_id is None:
            logger.warning(
                "Consultation booking alert: expert_id=%s has no user_id (engagement_id=%s)",
                expert_id,
                engagement_id,
            )
            return _dedupe_user_ids(admin_ids)

        employee = await _ensure_expert_employee(
            db,
            user_id=int(expert.user_id),
            employee_repository=employee_repository,
        )
        await _ensure_expert_assigned_to_engagement(
            db,
            engagement_id=engagement_id,
            employee_id=int(employee.employee_id),
            engagements_repository=engagements_repository,
        )
        return _dedupe_user_ids(admin_ids + [int(expert.user_id)])

    type_expert_user_ids = await experts_repository.list_active_user_ids_by_type(db, expert_type=expert_type)
    return _dedupe_user_ids(admin_ids + type_expert_user_ids)


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
    expert_id: int | None = None,
    experts_repository: ExpertsRepository | None = None,
    employee_repository: EmployeeRepository | None = None,
) -> None:
    """Dispatch each configured consultation_booking_alert service to mode-aware recipients."""
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
    employee_repo = employee_repository or EmployeeRepository()
    assistant_user_ids = await resolve_consultation_booking_alert_user_ids(
        db,
        engagement=engagement,
        expert_type=expert_type,
        expert_id=expert_id,
        engagements_repository=engagements_repository,
        experts_repository=experts_repo,
        employee_repository=employee_repo,
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
