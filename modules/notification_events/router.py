"""Auto notification events HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.notification_events.schemas import (
    NotificationEventCreateRequest,
    NotificationEventUpdateRequest,
)
from modules.notification_events.service import NotificationEventsService

router = APIRouter(prefix="/notification-events", tags=["notification-events"])


def _get_service() -> NotificationEventsService:
    return NotificationEventsService()


@router.get("")
async def list_notification_events(
    engagement_type_id: int | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: NotificationEventsService = Depends(_get_service),
):
    _ = employee
    data = await service.list_all(db, engagement_type_id=engagement_type_id)
    return success_response([item.model_dump() for item in data])


@router.get("/{event_id}")
async def get_notification_event(
    event_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: NotificationEventsService = Depends(_get_service),
):
    _ = employee
    data = await service.get_by_id(db, event_id)
    return success_response(data.model_dump())


@router.post("")
async def create_notification_event(
    payload: NotificationEventCreateRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: NotificationEventsService = Depends(_get_service),
):
    _ = employee
    data = await service.create(db, payload)
    await db.commit()
    return success_response(data.model_dump())


@router.put("/{event_id}")
async def update_notification_event(
    event_id: int,
    payload: NotificationEventUpdateRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: NotificationEventsService = Depends(_get_service),
):
    _ = employee
    data = await service.update(db, event_id, payload)
    await db.commit()
    return success_response(data.model_dump())


@router.delete("/{event_id}")
async def delete_notification_event(
    event_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: NotificationEventsService = Depends(_get_service),
):
    _ = employee
    await service.delete(db, event_id)
    await db.commit()
    return success_response({"deleted": True})
