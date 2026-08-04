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
)
from modules.engagements.models import AutoNotificationEvent
from modules.notification_events.repository import NotificationEventsRepository

router = APIRouter(tags=["engagement-notifications"])


def _get_repository() -> EngagementNotificationsRepository:
    return EngagementNotificationsRepository()


def _get_events_repository() -> NotificationEventsRepository:
    return NotificationEventsRepository()


@router.get("/engagements/{engagement_id}/notifications")
async def get_engagement_notifications(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
    events_repo: NotificationEventsRepository = Depends(_get_events_repository),
):
    _ = employee
    rows = await repo.list_for_engagement(db, engagement_id)
    events_by_id: dict[int, AutoNotificationEvent] = {}
    for row in rows:
        if row.notification_event_id not in events_by_id:
            event = await events_repo.get_by_id(db, row.notification_event_id)
            if event:
                events_by_id[row.notification_event_id] = event

    data = []
    for row in rows:
        event = events_by_id.get(row.notification_event_id)
        data.append(EngagementNotificationResponse(
            id=row.id,
            engagement_id=row.engagement_id,
            notification_event_id=row.notification_event_id,
            notification_services=list(row.notification_services or []),
            event_code=event.event_code if event else None,
            event_display_name=event.display_name if event else None,
        ).model_dump())
    return success_response(data)


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
            notification_event_id=row.notification_event_id,
            notification_services=list(row.notification_services or []),
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
    events_repo: NotificationEventsRepository = Depends(_get_events_repository),
):
    _ = employee
    rows = await repo.list_defaults_for_type(db, engagement_type_id)
    events_by_id: dict[int, AutoNotificationEvent] = {}
    for row in rows:
        if row.notification_event_id not in events_by_id:
            event = await events_repo.get_by_id(db, row.notification_event_id)
            if event:
                events_by_id[row.notification_event_id] = event

    data = []
    for row in rows:
        event = events_by_id.get(row.notification_event_id)
        data.append(NotificationDefaultResponse(
            id=row.id,
            engagement_type_id=row.engagement_type_id,
            notification_event_id=row.notification_event_id,
            notification_services=list(row.notification_services or []),
            event_code=event.event_code if event else None,
            event_display_name=event.display_name if event else None,
        ).model_dump())
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
            notification_event_id=row.notification_event_id,
            notification_services=list(row.notification_services or []),
        ).model_dump()
        for row in rows
    ]
    return success_response(data)
