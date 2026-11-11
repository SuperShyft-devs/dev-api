"""Engagement notifications repository."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.notifications.models import NotificationService

from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    extract_service_keys,
    normalize_notification_services,
    prepare_notification_items_for_storage,
    serialize_for_db,
)
from modules.engagements.models import EngagementNotification
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.events_catalog import get_event, map_legacy_event_code
from modules.notifications.models import NotificationEventBinding

# Events crons and report loaders read from engagement_notifications (not emit engine).
_ENGAGEMENT_SCOPED_EVENT_CODES = frozenset(
    {
        "reminders.pretest",
        "reminders.questionnaire_before",
        "reminders.questionnaire_after",
        "reminders.booking_guide",
        "reminders.consultation_ready_to_book",
        "reminders.consultation_reminder",
        "reports.blood_ready",
        "reports.bioai_ready",
        "reports.face_scan_ready",
        "users.onboarded",
    }
)


class EngagementNotificationsRepository:
    """Database queries for engagement_notifications and type-level defaults."""

    def __init__(self) -> None:
        self._bindings_repo = NotificationBindingsRepository()

    async def list_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
    ) -> list[EngagementNotification]:
        query = (
            select(EngagementNotification)
            .where(EngagementNotification.engagement_id == engagement_id)
            .order_by(EngagementNotification.event_code.asc())
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
        normalized = map_legacy_event_code(event_code)
        query = (
            select(EngagementNotification)
            .where(EngagementNotification.engagement_id == engagement_id)
            .where(EngagementNotification.event_code == normalized)
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
            ev = get_event(item["event_code"])
            event_name = ev.name if ev else item["event_code"]
            recipient_type = (
                ev.default_recipients[0] if ev and ev.default_recipients else "user"
            )
            obj = EngagementNotification(
                engagement_id=engagement_id,
                event_code=item["event_code"],
                event_name=event_name,
                recipient_type=recipient_type,
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
        """Auto-populate engagement_notifications from global bindings + applicability."""
        del engagement_type_id
        from modules.engagements.models import Engagement
        from modules.engagement_notifications.notification_setup import (
            collect_global_default_services_for_applicable,
        )
        from modules.notifications.event_applicability import profile_from_engagement

        engagement = await db.get(Engagement, engagement_id)
        if engagement is None:
            return []
        profile = await profile_from_engagement(db, engagement)
        items = await collect_global_default_services_for_applicable(db, profile)
        scoped = [i for i in items if i["event_code"] in _ENGAGEMENT_SCOPED_EVENT_CODES]
        if not scoped:
            return []
        return await self.upsert_for_engagement(db, engagement_id, scoped)

    async def list_defaults_for_type(
        self,
        db: AsyncSession,
        engagement_type_id: int,
    ) -> list[NotificationEventBinding]:
        return await self._bindings_repo.list_all(db, engagement_type_id=engagement_type_id)

    async def upsert_defaults(
        self,
        db: AsyncSession,
        engagement_type_id: int,
        defaults: list[dict],
    ) -> list[NotificationEventBinding]:
        """Bulk upsert notification defaults for an engagement type."""
        prepared = await prepare_notification_items_for_storage(db, defaults)

        existing = await self._bindings_repo.list_all(db, engagement_type_id=engagement_type_id)
        for row in existing:
            await self._bindings_repo.delete(db, row.id)

        result: list[NotificationEventBinding] = []
        for item in prepared:
            ev = get_event(item["event_code"])
            event_name = ev.name if ev else item["event_code"]
            recipient = item.get("recipient_type") or (
                ev.default_recipients[0] if ev and ev.default_recipients else "user"
            )
            obj = await self._bindings_repo.create(
                db,
                event_code=item["event_code"],
                event_name=event_name,
                recipient_type=str(recipient),
                notification_services=item["notification_services"],
                engagement_type_id=engagement_type_id,
            )
            result.append(obj)
        return result
