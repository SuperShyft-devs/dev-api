"""Business logic for notification event bindings."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.engagement_notifications.service_config import (
    normalize_notification_services,
    serialize_for_db,
    validate_notification_service_configs,
)
from modules.notifications.binding_schemas import (
    NotificationBindingCreate,
    NotificationBindingResponse,
    NotificationBindingUpdate,
)
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.repository import NotificationsRepository
from modules.notifications.events_catalog import get_event, list_events
from modules.notifications.emit_contact_catalog import payload_contact_fields_for_event
from modules.notifications.event_trigger_source import allows_payload_recipient, trigger_source_for_event
from modules.notifications.variable_catalog import RECIPIENT_PAYLOAD_HINTS, fields_for_groups
from modules.notifications.binding_schemas import CatalogEventResponse, PayloadContactFieldResponse


class NotificationBindingsService:
    def __init__(self, repository: NotificationBindingsRepository | None = None) -> None:
        self._repo = repository or NotificationBindingsRepository()

    def list_catalog_events(self) -> list[CatalogEventResponse]:
        out: list[CatalogEventResponse] = []
        for e in list_events():
            ts = trigger_source_for_event(e)
            allow_payload = allows_payload_recipient(e)
            payload_fields = (
                [
                    PayloadContactFieldResponse(**row)
                    for row in payload_contact_fields_for_event(e.code)
                ]
                if allow_payload
                else []
            )
            out.append(
                CatalogEventResponse(
                    code=e.code,
                    name=e.name,
                    status=e.status,
                    variable_groups=list(e.variable_groups),
                    variable_fields=fields_for_groups(e.variable_groups),
                    default_recipients=list(e.default_recipients),
                    trigger_notes=e.trigger_notes,
                    legacy_event_code=e.legacy_event_code,
                    recipient_payload_hints=dict(RECIPIENT_PAYLOAD_HINTS),
                    trigger_source=ts,
                    allows_payload_recipient=allow_payload,
                    payload_contact_fields=payload_fields,
                )
            )
        return out

    async def list_bindings(
        self,
        db: AsyncSession,
        *,
        event_code: str | None = None,
    ) -> list[NotificationBindingResponse]:
        rows = await self._repo.list_all(db, event_code=event_code)
        return [self._to_response(r) for r in rows]

    async def get_binding(self, db: AsyncSession, binding_id: int) -> NotificationBindingResponse:
        row = await self._repo.get_by_id(db, binding_id)
        if row is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Binding not found")
        return self._to_response(row)

    async def create_binding(
        self,
        db: AsyncSession,
        payload: NotificationBindingCreate,
    ) -> NotificationBindingResponse:
        if get_event(payload.event_code) is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unknown event_code '{payload.event_code}'",
            )
        configs = await validate_notification_service_configs(
            db,
            notification_services=[c.model_dump() for c in payload.notification_services],
            event_label=payload.event_code,
        )
        delivery_channel = "unknown"
        if configs:
            svc_row = await NotificationsRepository().get_service_by_key(
                db, service_key=configs[0].service_key
            )
            if svc_row is not None and svc_row.channel:
                delivery_channel = str(svc_row.channel).strip().lower()
        row = await self._repo.create(
            db,
            event_code=payload.event_code,
            event_name=payload.event_name,
            recipient_type=payload.recipient_type,
            delivery_channel=delivery_channel,
            notification_services=serialize_for_db(configs),
            engagement_type_id=payload.engagement_type_id,
        )
        return self._to_response(row)

    async def update_binding(
        self,
        db: AsyncSession,
        binding_id: int,
        payload: NotificationBindingUpdate,
    ) -> NotificationBindingResponse:
        row = await self._repo.get_by_id(db, binding_id)
        if row is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Binding not found")
        services = None
        if payload.notification_services is not None:
            configs = await validate_notification_service_configs(
                db,
                notification_services=[c.model_dump() for c in payload.notification_services],
                event_label=row.event_code,
            )
            services = serialize_for_db(configs)
        updated = await self._repo.update(
            db,
            row,
            event_name=payload.event_name,
            recipient_type=payload.recipient_type,
            notification_services=services,
            engagement_type_id=payload.engagement_type_id,
        )
        return self._to_response(updated)

    async def delete_binding(self, db: AsyncSession, binding_id: int) -> None:
        row = await self._repo.get_by_id(db, binding_id)
        if row is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Binding not found")
        await self._repo.delete(db, binding_id)

    @staticmethod
    def _to_response(row) -> NotificationBindingResponse:
        normalized = normalize_notification_services(row.notification_services)
        services = serialize_for_db(normalized)
        service_key = normalized[0].service_key if normalized else None
        delivery_channel = (
            str(getattr(row, "delivery_channel", "") or "").strip().lower() or "unknown"
        )
        return NotificationBindingResponse(
            id=row.id,
            event_code=row.event_code,
            event_name=row.event_name,
            recipient_type=row.recipient_type,
            delivery_channel=delivery_channel,
            notification_services=services,
            engagement_type_id=row.engagement_type_id,
            recipient_config=row.recipient_config if hasattr(row, "recipient_config") else None,
            service_key=service_key,
        )
