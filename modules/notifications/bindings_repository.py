"""Repository for notification_events bindings table."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.models import NotificationEventBinding


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

    async def create(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        event_name: str,
        recipient_type: str,
        notification_services: list,
        engagement_type_id: int | None = None,
    ) -> NotificationEventBinding:
        obj = NotificationEventBinding(
            event_code=event_code,
            event_name=event_name,
            recipient_type=recipient_type,
            notification_services=notification_services,
            engagement_type_id=engagement_type_id,
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
    ) -> NotificationEventBinding:
        if event_name is not None:
            binding.event_name = event_name
        if recipient_type is not None:
            binding.recipient_type = recipient_type
        if notification_services is not None:
            binding.notification_services = notification_services
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
        q = q.order_by(NotificationEventBinding.engagement_type_id.desc().nullslast())
        result = await db.execute(q)
        return list(result.scalars().all())
