"""Notifications HTTP routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagements.repository import EngagementsRepository
from modules.assessments.dependencies import get_assessment_package_categories_service
from modules.metsights.dependencies import get_metsights_service
from modules.notifications.dependencies import (
    authenticate_notification_endpoint,
    get_notifications_service,
)
from modules.notifications.questionnaire_reminders import dispatch_questionnaire_reminders
from modules.notifications.schemas import (
    CallbackRequest,
    DispatchRequest,
    NotificationServiceCreate,
    NotificationServiceUpdate,
    PrepareReportsRequest,
)
from modules.notifications.service import NotificationsService
from modules.users.dependencies import get_participant_journey_service
from modules.users.participant_journey_service import ParticipantJourneyService
from modules.employee.access_control import ensure_internal_employee


router = APIRouter(prefix="/notifications", tags=["notifications"])


def _service_dict(s) -> dict:
    return {
        "notification_service_id": s.notification_service_id,
        "service_key": s.service_key,
        "display_name": s.display_name,
        "channel": s.channel,
        "webhook_path": s.webhook_path,
        "is_active": s.is_active,
        "require_blood_report_url": s.require_blood_report_url,
        "require_bio_ai_report_url": s.require_bio_ai_report_url,
        "require_participant_detail": s.require_participant_detail,
        "require_otp": s.require_otp,
        "created_at": s.created_at.isoformat() if s.created_at else None,
    }


# ── Authenticated callback (called by n8n / external service) ───────────

@router.post("/callback")
async def notification_callback(
    payload: CallbackRequest,
    db: AsyncSession = Depends(get_db),
    svc: NotificationsService = Depends(get_notifications_service),
    auth=Depends(authenticate_notification_endpoint),
):
    result = await svc.callback(db, payload=payload)
    await db.commit()
    return success_response(result)


# ── Authenticated dispatch ────────────────────────────────────────────────

@router.post("/dispatch", status_code=201)
async def dispatch_notification(
    payload: DispatchRequest,
    db: AsyncSession = Depends(get_db),
    svc: NotificationsService = Depends(get_notifications_service),
    auth=Depends(authenticate_notification_endpoint),
):
    triggered_by = auth.user_id if auth is not None else None
    result = await svc.dispatch(db, payload=payload, triggered_by_user_id=triggered_by)
    await db.commit()
    return success_response(result)


@router.post("/prepare-reports")
async def prepare_reports(
    payload: PrepareReportsRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
    journey_service: ParticipantJourneyService = Depends(get_participant_journey_service),
):
    ensure_internal_employee(employee)
    if not payload.require_blood_report_url and not payload.require_bio_ai_report_url:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="At least one of require_blood_report_url or require_bio_ai_report_url must be true",
        )
    prepare_details = await svc.prepare_reports_for_user(
        db,
        user_id=payload.user_id,
        require_blood_report_url=payload.require_blood_report_url,
        require_bio_ai_report_url=payload.require_bio_ai_report_url,
    )
    await db.commit()
    summary, meta = await journey_service.get_summary(
        db,
        employee=employee,
        user_id=payload.user_id,
        page=1,
        limit=100,
    )
    return success_response(
        {"instances": summary["instances"], "prepare_details": prepare_details},
        meta=meta,
    )


# ── Questionnaire reminders (triggered by external scheduler) ────────────

@router.post("/questionnaire-reminders/dispatch")
async def dispatch_questionnaire_reminders_endpoint(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    result = await dispatch_questionnaire_reminders(
        db,
        notifications_service=svc,
        engagements_repository=EngagementsRepository(),
        metsights_service=get_metsights_service(),
        categories_service=get_assessment_package_categories_service(),
    )
    await db.commit()
    return success_response(result)


# ── Admin: list notifications ───────────────────────────────────────────

@router.get("")
async def list_notifications(
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    service_key: str | None = None,
    channel: str | None = None,
    user_id: int | None = None,
    engagement_id: int | None = None,
    dispatched_from: datetime | None = None,
    dispatched_to: datetime | None = None,
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
        channel=channel,
        user_id=user_id,
        engagement_id=engagement_id,
        dispatched_from=dispatched_from,
        dispatched_to=dispatched_to,
    )
    return success_response(
        items,
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


@router.delete("/services/{notification_service_id}")
async def delete_service(
    notification_service_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    result = await svc.delete_service(
        db, notification_service_id=notification_service_id
    )
    await db.commit()
    return success_response(result)


@router.delete("/{notification_id}")
async def delete_notification(
    notification_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    svc: NotificationsService = Depends(get_notifications_service),
):
    result = await svc.delete_notification(db, notification_id=notification_id)
    await db.commit()
    return success_response(result)
