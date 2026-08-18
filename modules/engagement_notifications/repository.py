"""Engagement notifications repository."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.service_config import (
    extract_service_keys,
    normalize_notification_services,
    prepare_notification_items_for_storage,
    serialize_for_db,
)
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
        """Return service keys for an engagement+event, or empty list."""
        en = await self.get_for_engagement_and_event_code(
            db, engagement_id=engagement_id, event_code=event_code
        )
        if en is None:
            return []
        return extract_service_keys(en.notification_services)

    async def upsert_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
        notifications: list[dict],
    ) -> list[EngagementNotification]:
        """Bulk upsert notification configs for an engagement."""
        prepared = await prepare_notification_items_for_storage(db, notifications)

        await db.execute(
            delete(EngagementNotification).where(
                EngagementNotification.engagement_id == engagement_id
            )
        )
        result = []
        for item in prepared:
            obj = EngagementNotification(
                engagement_id=engagement_id,
                notification_event_id=item["notification_event_id"],
                notification_services=item["notification_services"],
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
            configs = normalize_notification_services(d.notification_services)
            if configs:
                items.append({
                    "notification_event_id": d.notification_event_id,
                    "notification_services": serialize_for_db(configs),
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
        """Bulk upsert notification defaults for an engagement type."""
        prepared = await prepare_notification_items_for_storage(db, defaults)

        await db.execute(
            delete(EngagementNotificationDefault).where(
                EngagementNotificationDefault.engagement_type_id == engagement_type_id
            )
        )
        result = []
        for item in prepared:
            obj = EngagementNotificationDefault(
                engagement_type_id=engagement_type_id,
                notification_event_id=item["notification_event_id"],
                notification_services=item["notification_services"],
            )
            db.add(obj)
            result.append(obj)
        await db.flush()
        return result
