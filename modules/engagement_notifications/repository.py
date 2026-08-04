"""Engagement notifications repository."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import (
    AutoNotificationEvent,
    EngagementNotification,
    EngagementNotificationDefault,
)


class EngagementNotificationsRepository:
    """Database queries for engagement_notifications and engagement_notification_defaults."""

    # --- engagement_notifications ---

    async def list_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
    ) -> list[EngagementNotification]:
        query = (
            select(EngagementNotification)
            .where(EngagementNotification.engagement_id == engagement_id)
            .order_by(EngagementNotification.notification_event_id.asc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_for_engagement_and_event_code(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
    ) -> EngagementNotification | None:
        query = (
            select(EngagementNotification)
            .join(AutoNotificationEvent, AutoNotificationEvent.id == EngagementNotification.notification_event_id)
            .where(EngagementNotification.engagement_id == engagement_id)
            .where(AutoNotificationEvent.event_code == event_code)
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_services_for_engagement_event(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
    ) -> list[str]:
        """Return the notification_services array for an engagement+event, or empty list."""
        en = await self.get_for_engagement_and_event_code(
            db, engagement_id=engagement_id, event_code=event_code
        )
        if en is None:
            return []
        return list(en.notification_services or [])

    async def upsert_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
        notifications: list[dict],
    ) -> list[EngagementNotification]:
        """Bulk upsert notification configs for an engagement.

        Each dict: {notification_event_id: int, notification_services: list[str]}
        """
        await db.execute(
            delete(EngagementNotification).where(
                EngagementNotification.engagement_id == engagement_id
            )
        )
        result = []
        for item in notifications:
            services = item.get("notification_services", [])
            if not services:
                continue
            obj = EngagementNotification(
                engagement_id=engagement_id,
                notification_event_id=item["notification_event_id"],
                notification_services=services,
            )
            db.add(obj)
            result.append(obj)
        await db.flush()
        return result

    async def populate_from_defaults(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        engagement_type_id: int,
    ) -> list[EngagementNotification]:
        """Auto-populate engagement_notifications from defaults for the given type."""
        defaults = await self.list_defaults_for_type(db, engagement_type_id)
        items = []
        for d in defaults:
            if d.notification_services:
                items.append({
                    "notification_event_id": d.notification_event_id,
                    "notification_services": list(d.notification_services),
                })
        if not items:
            return []
        return await self.upsert_for_engagement(db, engagement_id, items)

    # --- engagement_notification_defaults ---

    async def list_defaults_for_type(
        self,
        db: AsyncSession,
        engagement_type_id: int,
    ) -> list[EngagementNotificationDefault]:
        query = (
            select(EngagementNotificationDefault)
            .where(EngagementNotificationDefault.engagement_type_id == engagement_type_id)
            .order_by(EngagementNotificationDefault.notification_event_id.asc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def upsert_defaults(
        self,
        db: AsyncSession,
        engagement_type_id: int,
        defaults: list[dict],
    ) -> list[EngagementNotificationDefault]:
        """Bulk upsert notification defaults for an engagement type.

        Each dict: {notification_event_id: int, notification_services: list[str]}
        """
        await db.execute(
            delete(EngagementNotificationDefault).where(
                EngagementNotificationDefault.engagement_type_id == engagement_type_id
            )
        )
        result = []
        for item in defaults:
            services = item.get("notification_services", [])
            if not services:
                continue
            obj = EngagementNotificationDefault(
                engagement_type_id=engagement_type_id,
                notification_event_id=item["notification_event_id"],
                notification_services=services,
            )
            db.add(obj)
            result.append(obj)
        await db.flush()
        return result
