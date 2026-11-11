"""Engagement notifications HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagement_notifications.notification_setup import (
    build_notification_setup_for_profile,
    list_addable_event_codes,
)
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.schemas import (
    EngagementNotificationResponse,
    EngagementNotificationsUpsertRequest,
    NotificationDefaultResponse,
    NotificationDefaultsUpsertRequest,
    configs_to_api,
)
from modules.engagements.models import Engagement
from modules.notifications.event_applicability import profile_from_engagement, profile_from_mapping
from modules.notifications.events_catalog import get_event
from modules.notifications.binding_schemas import EngagementNotificationWizardPayload
from modules.notifications.binding_wizard_service import NotificationBindingWizardService

router = APIRouter(tags=["engagement-notifications"])


def _get_repository() -> EngagementNotificationsRepository:
    return EngagementNotificationsRepository()


@router.get("/engagements/notification-setup")
async def get_engagement_notification_setup(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    engagement_id: int | None = Query(None),
    engagement_kind: str | None = Query(None),
    payment_type: str | None = Query(None),
    blood_collection_type: str | None = Query(None),
    consultation_mode: str | None = Query(None),
    auto_booking: bool | None = Query(None),
):
    _ = employee
    if engagement_id is not None:
        engagement = await db.get(Engagement, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Engagement not found")
        profile = await profile_from_engagement(db, engagement)
        data = await build_notification_setup_for_profile(
            db, profile, engagement_id=engagement_id
        )
    elif engagement_kind:
        profile = profile_from_mapping(
            {
                "engagement_kind": engagement_kind,
                "payment_type": payment_type,
                "blood_collection_type": blood_collection_type,
                "consultation_mode": consultation_mode,
                "auto_booking": auto_booking,
            }
        )
        data = await build_notification_setup_for_profile(db, profile, engagement_id=None)
    else:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Provide engagement_id or engagement_kind",
        )
    data["addable_events"] = list_addable_event_codes(profile)
    return success_response(data)


@router.post("/engagements/{engagement_id}/notifications/seed-from-defaults")
async def seed_engagement_notifications_from_defaults(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    engagement = await db.get(Engagement, engagement_id)
    if engagement is None:
        raise AppError(status_code=404, error_code="NOT_FOUND", message="Engagement not found")
    profile = await profile_from_engagement(db, engagement)
    rows = await repo.seed_from_global_defaults(db, engagement_id=engagement_id, profile=profile)
    await db.commit()
    setup = await build_notification_setup_for_profile(db, profile, engagement_id=engagement_id)
    return success_response({"seeded_slots": len(rows), **setup})


@router.delete("/engagements/{engagement_id}/notifications/bindings/{binding_id}")
async def delete_engagement_notification_binding(
    engagement_id: int,
    binding_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    slot = await repo.get_slot_by_id(db, binding_id)
    if slot is None or slot.engagement_id != engagement_id:
        raise AppError(status_code=404, error_code="NOT_FOUND", message="Binding not found")
    await repo.delete_slot(db, binding_id)
    await db.commit()
    return success_response({"deleted": binding_id})


@router.get("/engagements/{engagement_id}/notifications")
async def get_engagement_notifications(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    rows = await repo.list_for_engagement(db, engagement_id)
    data = []
    for row in rows:
        catalog = get_event(row.event_code)
        data.append(
            EngagementNotificationResponse(
                id=row.id,
                engagement_id=row.engagement_id,
                event_code=row.event_code,
                notification_services=configs_to_api(row.notification_services),
                event_display_name=catalog.name if catalog else row.event_code,
                recipient_type=row.recipient_type,
                delivery_channel=row.delivery_channel,
                recipient_config=(
                    row.recipient_config if isinstance(row.recipient_config, dict) else None
                ),
                condition_key=row.condition_key,
                emit_condition=row.emit_condition if isinstance(row.emit_condition, dict) else None,
            ).model_dump()
        )
    return success_response(data)


@router.post("/engagements/{engagement_id}/notifications/wizard")
async def save_engagement_notification_wizard(
    engagement_id: int,
    payload: EngagementNotificationWizardPayload,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    _ = employee
    wizard = NotificationBindingWizardService()
    result = await wizard.save_engagement_wizard(db, engagement_id, payload)
    await db.commit()
    return success_response(result)


@router.put("/engagements/{engagement_id}/notifications")
async def upsert_engagement_notifications(
    engagement_id: int,
    payload: EngagementNotificationsUpsertRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    items = [item.model_dump() for item in payload.notifications]
    await repo.upsert_for_engagement(db, engagement_id, items)
    await db.commit()
    rows = await repo.list_for_engagement(db, engagement_id)
    data = [
        EngagementNotificationResponse(
            id=row.id,
            engagement_id=row.engagement_id,
            event_code=row.event_code,
            notification_services=configs_to_api(row.notification_services),
        ).model_dump()
        for row in rows
    ]
    return success_response(data)


@router.get("/platform-settings/engagement-notification-defaults")
async def get_notification_defaults(
    engagement_type_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    rows = await repo.list_defaults_for_type(db, engagement_type_id)
    data = []
    for row in rows:
        catalog = get_event(row.event_code)
        data.append(
            NotificationDefaultResponse(
                id=row.id,
                engagement_type_id=row.engagement_type_id,
                event_code=row.event_code,
                recipient_type=row.recipient_type,
                notification_services=configs_to_api(row.notification_services),
                event_display_name=catalog.name if catalog else row.event_name,
            ).model_dump()
        )
    return success_response(data)


@router.put("/platform-settings/engagement-notification-defaults")
async def upsert_notification_defaults(
    payload: NotificationDefaultsUpsertRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    items = [item.model_dump() for item in payload.defaults]
    await repo.upsert_defaults(db, payload.engagement_type_id, items)
    await db.commit()
    rows = await repo.list_defaults_for_type(db, payload.engagement_type_id)
    data = [
        NotificationDefaultResponse(
            id=row.id,
            engagement_type_id=row.engagement_type_id,
            event_code=row.event_code,
            recipient_type=row.recipient_type,
            notification_services=configs_to_api(row.notification_services),
        ).model_dump()
        for row in rows
    ]
    return success_response(data)
