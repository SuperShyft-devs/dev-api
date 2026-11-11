"""Engagement notifications HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.notification_setup import (
    build_notification_setup_for_engagement,
    build_notification_setup_for_profile,
)
from modules.engagement_notifications.schemas import (
    EngagementNotificationResponse,
    EngagementNotificationsUpsertRequest,
    NotificationSetupPreviewRequest,
    configs_to_api,
)
from modules.notifications.event_applicability import profile_from_mapping
from modules.notifications.events_catalog import get_event
from modules.notifications.binding_schemas import EngagementNotificationWizardPayload
from modules.notifications.binding_wizard_service import NotificationBindingWizardService

router = APIRouter(tags=["engagement-notifications"])


def _get_repository() -> EngagementNotificationsRepository:
    return EngagementNotificationsRepository()


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


@router.get("/engagements/{engagement_id}/notification-setup")
async def get_engagement_notification_setup(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    _ = employee
    data = await build_notification_setup_for_engagement(db, engagement_id)
    return success_response(data)


@router.post("/engagements/notification-setup/preview")
async def preview_engagement_notification_setup(
    payload: NotificationSetupPreviewRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
):
    _ = employee
    profile = profile_from_mapping(
        {
            "engagement_kind": payload.engagement_kind,
            "blood_collection_type": payload.blood_collection_type,
            "auto_booking": payload.auto_booking,
            "consultation_mode": payload.consultation_mode,
        }
    )
    data = await build_notification_setup_for_profile(db, profile, engagement_id=None)
    return success_response(data)


@router.get("/platform-settings/engagement-notification-defaults")
async def get_notification_defaults_removed(
    engagement_type_id: int = Query(...),
    employee: EmployeeContext = Depends(get_current_employee),
):
    _ = employee, engagement_type_id
    raise HTTPException(
        status_code=410,
        detail="Engagement notification defaults moved to Notifications → Notifications (global bindings).",
    )


@router.put("/platform-settings/engagement-notification-defaults")
async def upsert_notification_defaults_removed(
    employee: EmployeeContext = Depends(get_current_employee),
):
    _ = employee
    raise HTTPException(
        status_code=410,
        detail="Engagement notification defaults moved to Notifications → Notifications (global bindings).",
    )
