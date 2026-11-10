"""Engagement notifications HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.schemas import (
    EngagementNotificationResponse,
    EngagementNotificationsUpsertRequest,
    NotificationDefaultResponse,
    NotificationDefaultsUpsertRequest,
    configs_to_api,
)
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
