"""Engagement notifications HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagement_notifications.engagement_bindings_service import engagement_binding_to_response
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.notification_setup import (
    build_notification_setup_for_engagement,
    build_notification_setup_for_profile,
)
from modules.notifications.event_applicability import profile_from_engagement
from modules.engagements.models import Engagement
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


def _row_response(row) -> dict:
    catalog = get_event(row.event_code)
    base = engagement_binding_to_response(row)
    base["event_display_name"] = catalog.name if catalog else row.event_code
    base["notification_services"] = configs_to_api(row.notification_services)
    return base


@router.get("/engagements/{engagement_id}/notification-bindings")
async def list_engagement_notification_bindings(
    engagement_id: int,
    event_code: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    rows = await repo.list_for_engagement(db, engagement_id, event_code=event_code)
    return success_response([_row_response(r) for r in rows])


@router.delete("/engagements/{engagement_id}/notification-bindings/{binding_id}")
async def delete_engagement_notification_binding(
    engagement_id: int,
    binding_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    row = await repo.get_by_id(db, binding_id)
    if row is None or row.engagement_id != engagement_id:
        raise HTTPException(status_code=404, detail="Binding not found")
    await repo.delete_binding(db, binding_id)
    await db.commit()
    return success_response({"deleted": True})


@router.post("/engagements/{engagement_id}/notification-bindings/copy-from-global")
async def copy_engagement_bindings_from_global(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    engagement = await db.get(Engagement, engagement_id)
    if engagement is None:
        raise HTTPException(status_code=404, detail="Engagement not found")
    profile = await profile_from_engagement(db, engagement)
    rows = await repo.copy_global_bindings_to_engagement(
        db, engagement_id, profile, replace_existing=True
    )
    await db.commit()
    return success_response({"copied": len(rows)})


@router.get("/engagements/{engagement_id}/notifications/events/{event_code}")
async def get_engagement_event_notification_bindings(
    engagement_id: int,
    event_code: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    from modules.notifications.bindings_repository import NotificationBindingsRepository
    from modules.notifications.bindings_service import NotificationBindingsService

    catalog = get_event(event_code)
    engagement_rows = await repo.list_for_event(
        db, engagement_id=engagement_id, event_code=event_code
    )
    global_repo = NotificationBindingsRepository()
    global_svc = NotificationBindingsService(global_repo)
    global_rows = await global_repo.list_for_event(db, event_code=event_code)
    return success_response(
        {
            "event_code": event_code,
            "name": catalog.name if catalog else event_code,
            "engagement_bindings": [_row_response(r) for r in engagement_rows],
            "global_bindings": [global_svc._to_response(r).model_dump() for r in global_rows],
        }
    )


@router.get("/engagements/{engagement_id}/notifications")
async def get_engagement_notifications(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    repo: EngagementNotificationsRepository = Depends(_get_repository),
):
    _ = employee
    rows = await repo.list_for_engagement(db, engagement_id)
    return success_response([_row_response(r) for r in rows])


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
    return success_response([_row_response(r) for r in rows])


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
