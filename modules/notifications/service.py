"""Notifications service.

Business logic for dispatching and tracking notifications.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.notifications.models import Notification, NotificationService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import (
    CallbackRequest,
    DispatchRequest,
    NotificationServiceCreate,
    NotificationServiceUpdate,
)

logger = logging.getLogger(__name__)


class NotificationsService:
    """Notification service layer."""

    def __init__(self, repository: NotificationsRepository) -> None:
        self._repo = repository

    # ── Dispatch ────────────────────────────────────────────────────────

    async def dispatch(
        self,
        db: AsyncSession,
        *,
        payload: DispatchRequest,
        triggered_by_user_id: int,
    ) -> dict:
        svc = await self._repo.get_service_by_key(db, service_key=payload.service_key)
        if svc is None or not svc.is_active:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Notification service '{payload.service_key}' not found or inactive",
            )

        user = await self._repo.get_user_by_id(db, user_id=payload.user_id)
        if user is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="User not found")

        assessment_instance_id: int | None = None
        record_id: str | None = payload.record_id

        if record_id:
            instance = await self._repo.get_assessment_instance_by_record_id(
                db, metsights_record_id=record_id
            )
            if instance:
                assessment_instance_id = instance.assessment_instance_id
        elif payload.engagement_id is not None:
            instance = await self._repo.get_metsights_instance_for_user_engagement(
                db,
                user_id=payload.user_id,
                engagement_id=payload.engagement_id,
            )
            if instance:
                assessment_instance_id = instance.assessment_instance_id
                record_id = instance.metsights_record_id
        else:
            instance = await self._repo.get_latest_metsights_instance_for_user(
                db, user_id=payload.user_id
            )
            if instance:
                assessment_instance_id = instance.assessment_instance_id
                record_id = instance.metsights_record_id

        if svc.require_record_id and not record_id:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="This service requires a record_id but none was found",
            )

        if svc.require_participant_detail and not payload.participant_details:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="This service requires participant_details but none were provided",
            )

        notification = Notification(
            service_key=svc.service_key,
            status="pending",
            channel=svc.channel,
            user_id=payload.user_id,
            engagement_id=payload.engagement_id,
            assessment_instance_id=assessment_instance_id,
            message="Notification dispatch initiated",
            triggered_by_user_id=triggered_by_user_id,
        )
        notification = await self._repo.create_notification(db, notification)

        webhook_url = settings.NOTIFICATION_SERVICE_BASE_URL.rstrip("/") + "/" + svc.webhook_path.lstrip("/")
        webhook_payload = {
            "notification_id": notification.notification_id,
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "phone": user.phone or "",
            "email": user.email or "",
            "record_id": record_id,
        }
        if payload.engagement_id is not None:
            webhook_payload["engagement_id"] = payload.engagement_id
        if payload.participant_details:
            webhook_payload["participant_details"] = payload.participant_details

        try:
            async with httpx.AsyncClient(timeout=settings.NOTIFICATION_SERVICE_TIMEOUT_SECONDS) as client:
                resp = await client.post(webhook_url, json=webhook_payload)
                resp.raise_for_status()
                resp_data = resp.json()
                webhook_message = resp_data.get("message", "Webhook called successfully")
        except Exception as exc:
            logger.error("Notification webhook call failed: %s", exc)
            webhook_message = f"Webhook call failed: {exc}"

        await self._repo.update_notification(
            db,
            notification_id=notification.notification_id,
            values={
                "message": webhook_message,
                "dispatched_at": datetime.now(timezone.utc),
            },
        )

        return {
            "notification_id": notification.notification_id,
            "status": notification.status,
            "message": webhook_message,
        }

    # ── Callback ────────────────────────────────────────────────────────

    async def callback(self, db: AsyncSession, *, payload: CallbackRequest) -> dict:
        notification = await self._repo.get_notification_by_id(
            db, notification_id=payload.notification_id
        )
        if notification is None:
            raise AppError(
                status_code=404, error_code="NOT_FOUND", message="Notification not found"
            )

        values: dict = {
            "status": payload.status,
            "message": payload.message or notification.message,
        }
        if payload.status == "sent":
            values["completed_at"] = datetime.now(timezone.utc)

        await self._repo.update_notification(
            db, notification_id=payload.notification_id, values=values
        )

        return {"notification_id": payload.notification_id, "status": payload.status}

    # ── Admin list ──────────────────────────────────────────────────────

    async def list_notifications(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        service_key: str | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
    ) -> tuple[list[Notification], int]:
        items = await self._repo.list_notifications(
            db,
            page=page,
            limit=limit,
            status=status,
            service_key=service_key,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        total = await self._repo.count_notifications(
            db,
            status=status,
            service_key=service_key,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        return items, total

    # ── Service CRUD ────────────────────────────────────────────────────

    async def list_services(self, db: AsyncSession) -> list[NotificationService]:
        return await self._repo.list_services(db)

    async def create_service(
        self, db: AsyncSession, *, payload: NotificationServiceCreate
    ) -> NotificationService:
        existing = await self._repo.get_service_by_key(db, service_key=payload.service_key)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CONFLICT",
                message=f"Service key '{payload.service_key}' already exists",
            )
        svc = NotificationService(
            service_key=payload.service_key,
            display_name=payload.display_name,
            channel=payload.channel,
            webhook_path=payload.webhook_path,
            is_active=payload.is_active,
            require_record_id=payload.require_record_id,
            require_participant_detail=payload.require_participant_detail,
        )
        return await self._repo.create_service(db, svc)

    async def update_service(
        self,
        db: AsyncSession,
        *,
        notification_service_id: int,
        payload: NotificationServiceUpdate,
    ) -> NotificationService:
        svc = await self._repo.get_service_by_id(
            db, notification_service_id=notification_service_id
        )
        if svc is None:
            raise AppError(
                status_code=404, error_code="NOT_FOUND", message="Service not found"
            )
        if payload.display_name is not None:
            svc.display_name = payload.display_name
        if payload.channel is not None:
            svc.channel = payload.channel
        if payload.webhook_path is not None:
            svc.webhook_path = payload.webhook_path
        if payload.is_active is not None:
            svc.is_active = payload.is_active
        if payload.require_record_id is not None:
            svc.require_record_id = payload.require_record_id
        if payload.require_participant_detail is not None:
            svc.require_participant_detail = payload.require_participant_detail
        return await self._repo.update_service(db, svc)

    async def delete_notification(
        self, db: AsyncSession, *, notification_id: int
    ) -> dict:
        notification = await self._repo.get_notification_by_id(
            db, notification_id=notification_id
        )
        if notification is None:
            raise AppError(
                status_code=404, error_code="NOT_FOUND", message="Notification not found"
            )
        deleted = await self._repo.delete_notification(db, notification_id=notification_id)
        if deleted < 1:
            raise AppError(
                status_code=404, error_code="NOT_FOUND", message="Notification not found"
            )
        return {"notification_id": notification_id, "deleted": True}

    async def delete_service(
        self, db: AsyncSession, *, notification_service_id: int
    ) -> dict:
        svc = await self._repo.get_service_by_id(
            db, notification_service_id=notification_service_id
        )
        if svc is None:
            raise AppError(
                status_code=404, error_code="NOT_FOUND", message="Service not found"
            )
        in_use = await self._repo.count_notifications_for_service_key(
            db, service_key=svc.service_key
        )
        if in_use > 0:
            raise AppError(
                status_code=409,
                error_code="CONFLICT",
                message="Cannot delete a service that has sent notifications. Delete those notifications first.",
            )
        deleted = await self._repo.delete_service(
            db, notification_service_id=notification_service_id
        )
        if deleted < 1:
            raise AppError(
                status_code=404, error_code="NOT_FOUND", message="Service not found"
            )
        return {"notification_service_id": notification_service_id, "deleted": True}
