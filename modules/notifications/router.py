"""Notifications HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.notifications.dependencies import get_notifications_service
from modules.notifications.schemas import (
    CallbackRequest,
    DispatchRequest,
    NotificationServiceCreate,
    NotificationServiceUpdate,
)
from modules.notifications.service import NotificationsService


router = APIRouter(prefix="/notifications", tags=["notifications"])


def _notification_dict(n) -> dict:
    return {
        "notification_id": n.notification_id,
        "service_key": n.service_key,
        "status": n.status,
        "channel": n.channel,
        "user_id": n.user_id,
        "engagement_id": n.engagement_id,
        "assessment_instance_id": n.assessment_instance_id,
        "message": n.message,
        "triggered_by_user_id": n.triggered_by_user_id,
        "dispatched_at": n.dispatched_at.isoformat() if n.dispatched_at else None,
        "completed_at": n.completed_at.isoformat() if n.completed_at else None,
    }


def _service_dict(s) -> dict:
    return {
        "notification_service_id": s.notification_service_id,
        "service_key": s.service_key,
        "display_name": s.display_name,
        "channel": s.channel,
        "webhook_path": s.webhook_path,
        "is_active": s.is_active,
        "require_record_id": s.require_record_id,
        "created_at": s.created_at.isoformat() if s.created_at else None,
    }


# ── Public callback (called by external service) ───────────────────────

@router.post("/callback")
async def notification_callback(
    payload: CallbackRequest,
    db: AsyncSession = Depends(get_db),
    svc: NotificationsService = Depends(get_notifications_service),
):
    result = await svc.callback(db, payload=payload)
    await db.commit()
    return success_response(result)


# ── Employee-only dispatch ──────────────────────────────────────────────

@router.post("/dispatch", status_code=201)
async def dispatch_notification(
    payload: DispatchRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    result = await svc.dispatch(db, payload=payload, triggered_by_user_id=employee.user_id)
    await db.commit()
    return success_response(result)


# ── Admin: list notifications ───────────────────────────────────────────

@router.get("")
async def list_notifications(
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    service_key: str | None = None,
    user_id: int | None = None,
    engagement_id: int | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    items, total = await svc.list_notifications(
        db,
        page=page,
        limit=limit,
        status=status,
        service_key=service_key,
        user_id=user_id,
        engagement_id=engagement_id,
    )
    return success_response(
        [_notification_dict(n) for n in items],
        meta={"page": page, "limit": limit, "total": total},
    )


# ── Admin: notification services CRUD ───────────────────────────────────

@router.get("/services")
async def list_services(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    services = await svc.list_services(db)
    return success_response([_service_dict(s) for s in services])


@router.post("/services", status_code=201)
async def create_service(
    payload: NotificationServiceCreate,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    service = await svc.create_service(db, payload=payload)
    await db.commit()
    return success_response(_service_dict(service))


@router.put("/services/{notification_service_id}")
async def update_service(
    notification_service_id: int,
    payload: NotificationServiceUpdate,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    service = await svc.update_service(
        db, notification_service_id=notification_service_id, payload=payload
    )
    await db.commit()
    return success_response(_service_dict(service))
