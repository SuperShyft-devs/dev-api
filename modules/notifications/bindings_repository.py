"""Repository for notification_events bindings table."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.models import NotificationEventBinding

_EMIT_CONDITION_UNSET = object()


class NotificationBindingsRepository:
    async def list_all(
        self,
        db: AsyncSession,
        *,
        event_code: str | None = None,
        engagement_type_id: int | None = None,
    ) -> list[NotificationEventBinding]:
        q = select(NotificationEventBinding).order_by(
            NotificationEventBinding.event_code.asc(),
            NotificationEventBinding.recipient_type.asc(),
            NotificationEventBinding.delivery_channel.asc(),
            NotificationEventBinding.id.asc(),
        )
        if event_code is not None:
            q = q.where(NotificationEventBinding.event_code == event_code)
        if engagement_type_id is not None:
            q = q.where(NotificationEventBinding.engagement_type_id == engagement_type_id)
        result = await db.execute(q)
        return list(result.scalars().all())

    async def get_by_id(self, db: AsyncSession, binding_id: int) -> NotificationEventBinding | None:
        result = await db.execute(
            select(NotificationEventBinding).where(NotificationEventBinding.id == binding_id)
        )
        return result.scalar_one_or_none()

    async def find_binding_by_slot(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        recipient_type: str,
        engagement_type_id: int | None,
        delivery_channel: str,
        condition_key: str = "default",
    ) -> NotificationEventBinding | None:
        q = select(NotificationEventBinding).where(
            NotificationEventBinding.event_code == event_code,
            NotificationEventBinding.recipient_type == recipient_type,
            NotificationEventBinding.delivery_channel == delivery_channel,
            NotificationEventBinding.condition_key == condition_key,
        )
        if engagement_type_id is None:
            q = q.where(NotificationEventBinding.engagement_type_id.is_(None))
        else:
            q = q.where(NotificationEventBinding.engagement_type_id == engagement_type_id)
        result = await db.execute(q)
        return result.scalar_one_or_none()

    async def create(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        event_name: str,
        recipient_type: str,
        notification_services: list,
        delivery_channel: str,
        engagement_type_id: int | None = None,
        recipient_config: dict | None = None,
        condition_key: str = "default",
        emit_condition: dict | None = None,
    ) -> NotificationEventBinding:
        obj = NotificationEventBinding(
            event_code=event_code,
            event_name=event_name,
            recipient_type=recipient_type,
            delivery_channel=delivery_channel,
            notification_services=notification_services,
            engagement_type_id=engagement_type_id,
            recipient_config=recipient_config,
            condition_key=condition_key,
            emit_condition=emit_condition,
        )
        db.add(obj)
        await db.flush()
        return obj

    async def update(
        self,
        db: AsyncSession,
        binding: NotificationEventBinding,
        *,
        event_name: str | None = None,
        recipient_type: str | None = None,
        notification_services: list | None = None,
        engagement_type_id: int | None = None,
        recipient_config: dict | None = None,
        delivery_channel: str | None = None,
        condition_key: str | None = None,
        emit_condition: dict | None | object = _EMIT_CONDITION_UNSET,
    ) -> NotificationEventBinding:
        if event_name is not None:
            binding.event_name = event_name
        if recipient_type is not None:
            binding.recipient_type = recipient_type
        if delivery_channel is not None:
            binding.delivery_channel = delivery_channel
        if notification_services is not None:
            binding.notification_services = notification_services
        if recipient_config is not None:
            binding.recipient_config = recipient_config
        if condition_key is not None:
            binding.condition_key = condition_key
        if emit_condition is not _EMIT_CONDITION_UNSET:
            binding.emit_condition = emit_condition  # type: ignore[assignment]
        await db.flush()
        return binding

    async def delete(self, db: AsyncSession, binding_id: int) -> None:
        await db.execute(delete(NotificationEventBinding).where(NotificationEventBinding.id == binding_id))

    async def list_for_event(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        engagement_type_id: int | None = None,
    ) -> list[NotificationEventBinding]:
        q = select(NotificationEventBinding).where(NotificationEventBinding.event_code == event_code)
        if engagement_type_id is not None:
            q = q.where(
                (NotificationEventBinding.engagement_type_id.is_(None))
                | (NotificationEventBinding.engagement_type_id == engagement_type_id)
            )
        else:
            q = q.where(NotificationEventBinding.engagement_type_id.is_(None))
        q = q.order_by(
            NotificationEventBinding.engagement_type_id.desc().nullslast(),
            NotificationEventBinding.delivery_channel.asc(),
            NotificationEventBinding.id.asc(),
        )
        result = await db.execute(q)
        return list(result.scalars().all())
