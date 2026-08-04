"""Auto notification events repository."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import AutoNotificationEvent


class NotificationEventsRepository:
    """Database queries for auto_notification_events."""

    async def list_all(
        self,
        db: AsyncSession,
        *,
        engagement_type_id: int | None = None,
    ) -> list[AutoNotificationEvent]:
        query = select(AutoNotificationEvent).order_by(AutoNotificationEvent.id.asc())
        if engagement_type_id is not None:
            query = query.where(
                AutoNotificationEvent.engagement_type_ids.contains([engagement_type_id])
            )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_by_id(self, db: AsyncSession, event_id: int) -> AutoNotificationEvent | None:
        query = select(AutoNotificationEvent).where(AutoNotificationEvent.id == event_id)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_by_event_code(self, db: AsyncSession, event_code: str) -> AutoNotificationEvent | None:
        query = select(AutoNotificationEvent).where(AutoNotificationEvent.event_code == event_code)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def create(
        self,
        db: AsyncSession,
        *,
        engagement_type_ids: list[int],
        event_code: str,
        display_name: str,
        description: str | None = None,
    ) -> AutoNotificationEvent:
        obj = AutoNotificationEvent(
            engagement_type_ids=engagement_type_ids,
            event_code=event_code,
            display_name=display_name,
            description=description,
        )
        db.add(obj)
        await db.flush()
        return obj

    async def update(
        self,
        db: AsyncSession,
        event_id: int,
        *,
        engagement_type_ids: list[int] | None = None,
        display_name: str | None = None,
        description: str | None = ...,
    ) -> AutoNotificationEvent | None:
        obj = await self.get_by_id(db, event_id)
        if obj is None:
            return None
        if engagement_type_ids is not None:
            obj.engagement_type_ids = engagement_type_ids
        if display_name is not None:
            obj.display_name = display_name
        if description is not ...:
            obj.description = description
        await db.flush()
        return obj

    async def delete(self, db: AsyncSession, event_id: int) -> bool:
        stmt = delete(AutoNotificationEvent).where(AutoNotificationEvent.id == event_id)
        result = await db.execute(stmt)
        return result.rowcount > 0
