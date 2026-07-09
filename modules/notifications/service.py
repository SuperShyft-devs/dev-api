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
from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository
from modules.notifications.models import Notification, NotificationService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import (
    CallbackRequest,
    DispatchRequest,
    NotificationServiceCreate,
    NotificationServiceUpdate,
)

logger = logging.getLogger(__name__)

_VALID_NOTIFICATION_STATUSES = frozenset({"pending", "sent", "failed"})
_VALID_NOTIFICATION_CHANNELS = frozenset({"email", "whatsapp"})


def _parse_status_filter(status: str | None) -> list[str] | None:
    if status is None or not status.strip():
        return None
    parts = [s.strip().lower() for s in status.split(",") if s.strip()]
    if not parts:
        return None
    invalid = [s for s in parts if s not in _VALID_NOTIFICATION_STATUSES]
    if invalid:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=f"Invalid status filter: {', '.join(invalid)}",
        )
    return parts


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
        triggered_by_user_id: int | None = None,
    ) -> dict:
        svc = await self._repo.get_service_by_key(db, service_key=payload.service_key)
        if svc is None or not svc.is_active:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Notification service '{payload.service_key}' not found or inactive",
            )

        users = await self._repo.get_users_by_ids(db, user_ids=payload.user_ids)
        found_ids = {u.user_id for u in users}
        missing_ids = [uid for uid in payload.user_ids if uid not in found_ids]
        if missing_ids:
            raise AppError(
                status_code=404,
                error_code="NOT_FOUND",
                message=f"Users not found: {missing_ids}",
            )

        if svc.require_participant_detail and not payload.participant_details:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="This service requires participant_details but none were provided",
            )

        otp_value = (payload.otp or "").strip()
        if svc.require_otp and not otp_value:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="This service requires otp but none was provided",
            )

        needs_report = svc.require_blood_report_url or svc.require_bio_ai_report_url

        if payload.assessment_instance_id is not None and len(payload.user_ids) > 1:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="assessment_instance_id is only supported for single-user dispatch",
            )

        members: list[dict] = []
        resolved_assessment_instance_id: int | None = None
        resolved_engagement_id: int | None = payload.engagement_id

        for user in users:
            instance = None
            if payload.assessment_instance_id is not None:
                instance = await self._repo.get_instance_for_user(
                    db,
                    user_id=user.user_id,
                    assessment_instance_id=payload.assessment_instance_id,
                )
                if instance is None:
                    raise AppError(
                        status_code=400,
                        error_code="INVALID_INPUT",
                        message=(
                            f"Assessment instance {payload.assessment_instance_id} "
                            f"not found for user_id={user.user_id}"
                        ),
                    )
            elif payload.engagement_id is not None:
                instance = await self._repo.get_metsights_instance_for_user_engagement(
                    db,
                    user_id=user.user_id,
                    engagement_id=payload.engagement_id,
                )
            elif needs_report:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=(
                        "This service requires report URLs; provide assessment_instance_id "
                        "or engagement_id"
                    ),
                )

            if instance:
                resolved_assessment_instance_id = (
                    resolved_assessment_instance_id or instance.assessment_instance_id
                )
                if resolved_engagement_id is None and instance.engagement_id is not None:
                    resolved_engagement_id = instance.engagement_id

            member: dict = {
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "phone": user.phone or "",
                "email": user.email or "",
            }

            if needs_report:
                if not instance:
                    raise AppError(
                        status_code=400,
                        error_code="INVALID_INPUT",
                        message=f"No assessment instance found for user_id={user.user_id}",
                    )
                ihr = await self._repo.get_health_report_for_instance(
                    db, assessment_instance_id=instance.assessment_instance_id
                )
                if svc.require_blood_report_url:
                    url = ihr.diagnostic_report_url if ihr else None
                    if not url:
                        raise AppError(
                            status_code=400,
                            error_code="INVALID_INPUT",
                            message=f"Blood report URL not available for user_id={user.user_id}",
                        )
                    member["blood_report_url"] = url
                if svc.require_bio_ai_report_url:
                    type_code = await self._repo.get_assessment_type_code_for_instance(
                        db, assessment_instance_id=instance.assessment_instance_id
                    )
                    if (type_code or "") not in ("1", "2"):
                        raise AppError(
                            status_code=400,
                            error_code="INVALID_INPUT",
                            message=(
                                f"BioAI reports are only available for MetSights Basic/Pro assessments; "
                                f"assessment_instance_id={instance.assessment_instance_id} is type {type_code!r}"
                            ),
                        )
                    url = ihr.report_url if ihr else None
                    if not url:
                        raise AppError(
                            status_code=400,
                            error_code="INVALID_INPUT",
                            message=f"BioAI report URL not available for user_id={user.user_id}",
                        )
                    member["bio_ai_report_url"] = url

            if svc.require_otp:
                member["otp"] = otp_value
            members.append(member)

        notification = Notification(
            service_key=svc.service_key,
            status="pending",
            channel=svc.channel,
            user={"user_ids": payload.user_ids},
            engagement_id=resolved_engagement_id,
            assessment_instance_id=resolved_assessment_instance_id,
            message="Notification dispatch initiated",
            triggered_by_user_id=triggered_by_user_id,
        )
        notification = await self._repo.create_notification(db, notification)

        webhook_url = settings.NOTIFICATION_SERVICE_BASE_URL.rstrip("/") + "/" + svc.webhook_path.lstrip("/")
        webhook_payload: dict = {
            "notification_id": notification.notification_id,
            "members": members,
        }
        if resolved_engagement_id is not None:
            webhook_payload["engagement_id"] = resolved_engagement_id
        if payload.participant_details:
            webhook_payload["participant_details"] = payload.participant_details

        audit_repo = AuditRepository()
        sync_log = await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=None,
                user_id=None,
                provider="n8n",
                api_endpoint_url=webhook_url,
                request_payload=webhook_payload,
                status="pending",
            ),
        )

        try:
            async with httpx.AsyncClient(timeout=settings.NOTIFICATION_SERVICE_TIMEOUT_SECONDS) as client:
                resp = await client.post(webhook_url, json=webhook_payload)
                resp.raise_for_status()
                try:
                    resp_data = resp.json()
                except Exception:
                    resp_data = {"status_code": resp.status_code, "body": resp.text}
                webhook_message = (
                    resp_data.get("message", "Webhook called successfully")
                    if isinstance(resp_data, dict)
                    else "Webhook called successfully"
                )
                await audit_repo.update_sync_log_status(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="success",
                    response_payload=resp_data if isinstance(resp_data, dict) else {"body": resp_data},
                )
        except Exception as exc:
            logger.error("Notification webhook call failed: %s", exc)
            webhook_message = f"Webhook call failed: {exc}"
            await audit_repo.update_sync_log_status(
                db,
                sync_log_id=sync_log.sync_log_id,
                status="failed",
                error_message=str(exc),
            )

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

    @staticmethod
    def _notification_base_dict(n: Notification) -> dict:
        return {
            "notification_id": n.notification_id,
            "service_key": n.service_key,
            "status": n.status,
            "channel": n.channel,
            "user": n.user,
            "engagement_id": n.engagement_id,
            "assessment_instance_id": n.assessment_instance_id,
            "message": n.message,
            "triggered_by_user_id": n.triggered_by_user_id,
            "dispatched_at": n.dispatched_at.isoformat() if n.dispatched_at else None,
            "completed_at": n.completed_at.isoformat() if n.completed_at else None,
        }

    async def _enrich_notification_list_items(
        self, db: AsyncSession, items: list[Notification]
    ) -> list[dict]:
        if not items:
            return []

        service_keys: set[str] = set()
        engagement_ids: set[int] = set()
        user_ids: set[int] = set()

        for n in items:
            service_keys.add(n.service_key)
            if n.engagement_id is not None:
                engagement_ids.add(n.engagement_id)
            if n.triggered_by_user_id is not None:
                user_ids.add(n.triggered_by_user_id)
            raw_user = n.user if isinstance(n.user, dict) else {}
            for uid in raw_user.get("user_ids") or []:
                if isinstance(uid, int):
                    user_ids.add(uid)

        services = await self._repo.get_services_by_keys(db, service_keys=list(service_keys))
        service_by_key = {s.service_key: s for s in services}

        engagements = await self._repo.get_engagements_by_ids(
            db, engagement_ids=list(engagement_ids)
        )
        engagement_by_id = {e.engagement_id: e for e in engagements}

        users = await self._repo.get_users_by_ids(db, user_ids=list(user_ids))
        user_by_id = {u.user_id: u for u in users}

        enriched: list[dict] = []
        for n in items:
            row = self._notification_base_dict(n)
            svc = service_by_key.get(n.service_key)
            row["service_display_name"] = svc.display_name if svc else n.service_key

            raw_user = n.user if isinstance(n.user, dict) else {}
            recipient_ids = [
                uid for uid in (raw_user.get("user_ids") or []) if isinstance(uid, int)
            ]
            recipients: list[dict] = []
            for uid in recipient_ids:
                u = user_by_id.get(uid)
                recipients.append(
                    {
                        "user_id": uid,
                        "first_name": u.first_name if u else None,
                        "last_name": u.last_name if u else None,
                    }
                )
            row["recipients"] = recipients

            engagement = (
                engagement_by_id.get(n.engagement_id) if n.engagement_id is not None else None
            )
            row["engagement_name"] = engagement.engagement_name if engagement else None
            row["engagement_code"] = engagement.engagement_code if engagement else None

            enriched.append(row)
        return enriched

    async def list_notifications(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        service_key: str | None = None,
        channel: str | None = None,
        user_id: int | None = None,
        engagement_id: int | None = None,
        dispatched_from: datetime | None = None,
        dispatched_to: datetime | None = None,
    ) -> tuple[list[dict], int]:
        statuses = _parse_status_filter(status)
        if channel is not None and channel.strip():
            channel = channel.strip().lower()
            if channel not in _VALID_NOTIFICATION_CHANNELS:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=f"Invalid channel filter: {channel}",
                )
        else:
            channel = None

        filter_kwargs = {
            "statuses": statuses,
            "service_key": service_key,
            "channel": channel,
            "user_id": user_id,
            "engagement_id": engagement_id,
            "dispatched_from": dispatched_from,
            "dispatched_to": dispatched_to,
        }
        items = await self._repo.list_notifications(db, page=page, limit=limit, **filter_kwargs)
        total = await self._repo.count_notifications(db, **filter_kwargs)
        return await self._enrich_notification_list_items(db, items), total

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
            require_blood_report_url=payload.require_blood_report_url,
            require_bio_ai_report_url=payload.require_bio_ai_report_url,
            require_participant_detail=payload.require_participant_detail,
            require_otp=payload.require_otp,
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
        if payload.require_blood_report_url is not None:
            svc.require_blood_report_url = payload.require_blood_report_url
        if payload.require_bio_ai_report_url is not None:
            svc.require_bio_ai_report_url = payload.require_bio_ai_report_url
        if payload.require_participant_detail is not None:
            svc.require_participant_detail = payload.require_participant_detail
        if payload.require_otp is not None:
            svc.require_otp = payload.require_otp
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
