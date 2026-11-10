"""Transactional save for admin notification binding wizard."""

from __future__ import annotations

import re

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    serialize_for_db,
)
from modules.notifications.binding_schemas import (
    EngagementNotificationWizardPayload,
    NotificationBindingResponse,
    WizardBindingPayload,
    WizardTemplateSlot,
)
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.bindings_service import NotificationBindingsService
from modules.notifications.events_catalog import get_event
from modules.notifications.models import NotificationService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.variable_catalog import KNOWN_RECIPIENT_TYPES


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def derive_service_key(event_code: str, channel: str, *, suffix: int = 0) -> str:
    base = f"{_slug(event_code)}-{channel}"
    return base if suffix == 0 else f"{base}-{suffix}"


def build_template_variables(
    *,
    template_variables: dict | list | None,
    template_slots: list[WizardTemplateSlot] | None,
) -> dict | list | None:
    if template_variables is not None:
        return template_variables
    if not template_slots:
        return None
    return {
        "slots": [
            {
                "index": slot.index,
                "source": slot.source,
                **({"value": slot.value} if slot.value is not None else {}),
            }
            for slot in template_slots
        ]
    }


class NotificationBindingWizardService:
    def __init__(
        self,
        *,
        bindings_repo: NotificationBindingsRepository | None = None,
        notifications_repo: NotificationsRepository | None = None,
        bindings_service: NotificationBindingsService | None = None,
        en_repo: EngagementNotificationsRepository | None = None,
    ) -> None:
        self._bindings_repo = bindings_repo or NotificationBindingsRepository()
        self._notifications_repo = notifications_repo or NotificationsRepository()
        self._bindings_service = bindings_service or NotificationBindingsService(self._bindings_repo)
        self._en_repo = en_repo or EngagementNotificationsRepository()

    def _validate_recipient(self, payload: WizardBindingPayload, ev) -> dict | None:
        rt = payload.recipient_type.strip()
        if rt not in KNOWN_RECIPIENT_TYPES:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unknown recipient_type '{rt}'",
            )
        if rt == "custom":
            cfg = payload.recipient_config
            if cfg is None or (not cfg.phones and not cfg.emails):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Custom recipient requires at least one phone or email",
                )
            return {"phones": list(cfg.phones), "emails": list(cfg.emails)}
        if rt not in ev.default_recipients and rt not in (
            "assistants",
            "departments",
            "departments.ops",
            "departments.tech",
            "departments.design",
        ):
            pass
        return None

    async def _ensure_service(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        spec,
    ) -> NotificationService:
        if spec.notification_service_id is not None:
            svc = await self._notifications_repo.get_service_by_id(
                db, notification_service_id=spec.notification_service_id
            )
            if svc is None:
                raise AppError(status_code=404, error_code="NOT_FOUND", message="Service not found")
            svc.display_name = (spec.display_name or svc.display_name).strip()
            svc.channel = spec.channel
            svc.is_active = spec.is_active
            svc.template_name = spec.template_name
            svc.template_variables = build_template_variables(
                template_variables=spec.template_variables,
                template_slots=spec.template_slots,
            )
            svc.email_subject = spec.email_subject
            svc.email_body = spec.email_body
            svc.email_html = spec.email_html
            await self._notifications_repo.update_service(db, svc)
            return svc

        key = (spec.service_key or "").strip()
        if not key:
            key = derive_service_key(event_code, spec.channel)
        attempt = 0
        while await self._notifications_repo.get_service_by_key(db, service_key=key):
            attempt += 1
            if attempt > 50:
                raise AppError(
                    status_code=409,
                    error_code="CONFLICT",
                    message="Could not allocate unique service_key",
                )
            key = derive_service_key(event_code, spec.channel, suffix=attempt)

        display_name = (spec.display_name or key).strip()
        svc = NotificationService(
            service_key=key,
            display_name=display_name,
            channel=spec.channel,
            webhook_path=key,
            is_active=spec.is_active,
            template_name=spec.template_name,
            template_variables=build_template_variables(
                template_variables=spec.template_variables,
                template_slots=spec.template_slots,
            ),
            email_subject=spec.email_subject,
            email_body=spec.email_body,
            email_html=spec.email_html,
        )
        return await self._notifications_repo.create_service(db, svc)

    async def create_binding_wizard(
        self, db: AsyncSession, payload: WizardBindingPayload
    ) -> NotificationBindingResponse:
        ev = get_event(payload.event_code)
        if ev is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unknown event_code '{payload.event_code}'",
            )
        recipient_config = self._validate_recipient(payload, ev)
        svc = await self._ensure_service(db, event_code=payload.event_code, spec=payload.service)
        row = await self._bindings_repo.create(
            db,
            event_code=payload.event_code,
            event_name=ev.name,
            recipient_type=payload.recipient_type.strip(),
            notification_services=serialize_for_db(
                [NotificationServiceConfigItem(service_key=svc.service_key)]
            ),
            engagement_type_id=payload.engagement_type_id,
            recipient_config=recipient_config,
        )
        return self._bindings_service._to_response(row)

    async def update_binding_wizard(
        self, db: AsyncSession, binding_id: int, payload: WizardBindingPayload
    ) -> NotificationBindingResponse:
        row = await self._bindings_repo.get_by_id(db, binding_id)
        if row is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Binding not found")
        ev = get_event(payload.event_code)
        if ev is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unknown event_code '{payload.event_code}'",
            )
        recipient_config = self._validate_recipient(payload, ev)
        svc = await self._ensure_service(db, event_code=payload.event_code, spec=payload.service)
        updated = await self._bindings_repo.update(
            db,
            row,
            event_name=ev.name,
            recipient_type=payload.recipient_type.strip(),
            notification_services=serialize_for_db(
                [NotificationServiceConfigItem(service_key=svc.service_key)]
            ),
            recipient_config=recipient_config,
        )
        if payload.engagement_type_id is not None:
            updated.engagement_type_id = payload.engagement_type_id
            await db.flush()
        return self._bindings_service._to_response(updated)

    async def save_engagement_wizard(
        self,
        db: AsyncSession,
        engagement_id: int,
        payload: EngagementNotificationWizardPayload,
    ) -> dict:
        ev = get_event(payload.event_code)
        if ev is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unknown event_code '{payload.event_code}'",
            )
        svc = await self._ensure_service(db, event_code=payload.event_code, spec=payload.service)
        configs = serialize_for_db([NotificationServiceConfigItem(service_key=svc.service_key)])
        existing = await self._en_repo.get_for_engagement_and_event_code(
            db, engagement_id=engagement_id, event_code=payload.event_code
        )
        if existing is None:
            await self._en_repo.upsert_for_engagement(
                db,
                engagement_id,
                [{"event_code": payload.event_code, "notification_services": configs}],
            )
        else:
            all_rows = await self._en_repo.list_for_engagement(db, engagement_id)
            items = []
            for row in all_rows:
                if row.event_code == payload.event_code:
                    items.append(
                        {"event_code": row.event_code, "notification_services": configs}
                    )
                else:
                    items.append(
                        {
                            "event_code": row.event_code,
                            "notification_services": row.notification_services,
                        }
                    )
            await self._en_repo.upsert_for_engagement(db, engagement_id, items)
        rows = await self._en_repo.list_for_engagement(db, engagement_id)
        return {"engagement_id": engagement_id, "notifications": len(rows)}
