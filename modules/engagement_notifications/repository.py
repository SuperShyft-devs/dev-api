"""Engagement notifications repository."""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    extract_service_keys,
    normalize_notification_services,
    prepare_notification_items_for_storage,
    serialize_for_db,
)
from modules.engagements.models import Engagement, EngagementNotification
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.event_applicability import (
    EngagementNotificationProfile,
    list_applicable_event_codes,
    profile_from_engagement,
)
from modules.notifications.events_catalog import get_event, map_legacy_event_code
from modules.notifications.models import NotificationEventBinding, NotificationService

# Events crons and report loaders read service keys from engagement notification slots.
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
    """Database queries for engagement notification binding slots."""

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
            .order_by(
                EngagementNotification.event_code.asc(),
                EngagementNotification.recipient_type.asc(),
                EngagementNotification.delivery_channel.asc(),
            )
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_slots_for_event(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
    ) -> list[EngagementNotification]:
        normalized = map_legacy_event_code(event_code)
        query = (
            select(EngagementNotification)
            .where(EngagementNotification.engagement_id == engagement_id)
            .where(EngagementNotification.event_code == normalized)
            .order_by(EngagementNotification.id.asc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_slot_by_id(
        self, db: AsyncSession, slot_id: int
    ) -> EngagementNotification | None:
        return await db.get(EngagementNotification, slot_id)

    async def has_any_slots(self, db: AsyncSession, engagement_id: int) -> bool:
        result = await db.execute(
            select(EngagementNotification.id)
            .where(EngagementNotification.engagement_id == engagement_id)
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def get_for_engagement_and_event_code(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
    ) -> EngagementNotification | None:
        slots = await self.list_slots_for_event(
            db, engagement_id=engagement_id, event_code=event_code
        )
        return slots[0] if slots else None

    async def get_services_for_engagement_event(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
    ) -> list[str]:
        slots = await self.list_slots_for_event(
            db, engagement_id=engagement_id, event_code=event_code
        )
        keys: list[str] = []
        seen: set[str] = set()
        for slot in slots:
            for key in extract_service_keys(slot.notification_services):
                if key and key not in seen:
                    seen.add(key)
                    keys.append(key)
        return keys

    async def delete_slot(self, db: AsyncSession, slot_id: int) -> None:
        await db.execute(delete(EngagementNotification).where(EngagementNotification.id == slot_id))

    async def delete_slots_for_event(
        self, db: AsyncSession, *, engagement_id: int, event_code: str
    ) -> None:
        normalized = map_legacy_event_code(event_code)
        await db.execute(
            delete(EngagementNotification).where(
                EngagementNotification.engagement_id == engagement_id,
                EngagementNotification.event_code == normalized,
            )
        )

    async def upsert_binding_slot(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
        recipient_type: str,
        delivery_channel: str,
        notification_services: list,
        event_name: str | None = None,
        recipient_config: dict | None = None,
        condition_key: str = "default",
        emit_condition: dict | None = None,
        source_binding_id: int | None = None,
        slot_id: int | None = None,
    ) -> EngagementNotification:
        normalized = map_legacy_event_code(event_code)
        ev = get_event(normalized)
        name = event_name or (ev.name if ev else normalized)
        channel = delivery_channel.strip().lower()
        ck = (condition_key or "default").strip() or "default"
        rt = recipient_type.strip()

        existing: EngagementNotification | None = None
        if slot_id is not None:
            existing = await self.get_slot_by_id(db, slot_id)
        if existing is None:
            result = await db.execute(
                select(EngagementNotification).where(
                    EngagementNotification.engagement_id == engagement_id,
                    EngagementNotification.event_code == normalized,
                    EngagementNotification.recipient_type == rt,
                    EngagementNotification.delivery_channel == channel,
                    EngagementNotification.condition_key == ck,
                )
            )
            existing = result.scalar_one_or_none()

        if existing is not None:
            existing.event_name = name
            existing.notification_services = notification_services
            existing.recipient_config = recipient_config
            existing.emit_condition = emit_condition
            if source_binding_id is not None:
                existing.source_binding_id = source_binding_id
            await db.flush()
            return existing

        obj = EngagementNotification(
            engagement_id=engagement_id,
            event_code=normalized,
            event_name=name,
            recipient_type=rt,
            delivery_channel=channel,
            notification_services=notification_services,
            recipient_config=recipient_config,
            condition_key=ck,
            emit_condition=emit_condition,
            source_binding_id=source_binding_id,
        )
        db.add(obj)
        await db.flush()
        return obj

    async def upsert_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
        notifications: list[dict],
    ) -> list[EngagementNotification]:
        """Legacy bulk replace: one default slot per event (admin PUT)."""
        prepared = await prepare_notification_items_for_storage(db, notifications)
        await db.execute(
            delete(EngagementNotification).where(
                EngagementNotification.engagement_id == engagement_id
            )
        )
        result: list[EngagementNotification] = []
        for item in prepared:
            ev = get_event(item["event_code"])
            event_name = ev.name if ev else item["event_code"]
            recipient_type = (
                ev.default_recipients[0] if ev and ev.default_recipients else "user"
            )
            channel = "whatsapp"
            keys = extract_service_keys(item["notification_services"])
            if keys:
                svc_row = await db.execute(
                    select(NotificationService).where(NotificationService.service_key == keys[0])
                )
                svc = svc_row.scalar_one_or_none()
                if svc and svc.channel:
                    channel = str(svc.channel).lower()
            obj = EngagementNotification(
                engagement_id=engagement_id,
                event_code=item["event_code"],
                event_name=event_name,
                recipient_type=recipient_type,
                delivery_channel=channel,
                notification_services=item["notification_services"],
                condition_key="default",
            )
            db.add(obj)
            result.append(obj)
        await db.flush()
        return result

    async def _clone_global_row(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        row: NotificationEventBinding,
    ) -> EngagementNotification | None:
        configs = normalize_notification_services(row.notification_services)
        if not configs:
            return None
        keys = [c.service_key for c in configs if c.service_key]
        if not keys:
            return None
        svc_rows = await db.execute(
            select(NotificationService).where(NotificationService.service_key.in_(keys))
        )
        services_by_key = {s.service_key: s for s in svc_rows.scalars().all()}
        filtered: list[NotificationServiceConfigItem] = []
        for cfg in configs:
            sk = cfg.service_key
            svc = services_by_key.get(sk or "")
            if svc is None:
                continue
            link = cfg.external_link
            if svc.require_external_link and not (link and str(link).strip()):
                link = "https://app.supershyft.com"
            filtered.append(
                NotificationServiceConfigItem(
                    service_key=sk,
                    external_link=str(link).strip() if link else None,
                )
            )
        if not filtered:
            return None
        emit_cond = row.emit_condition if isinstance(row.emit_condition, dict) else None
        recipient_cfg = row.recipient_config if isinstance(row.recipient_config, dict) else None
        return await self.upsert_binding_slot(
            db,
            engagement_id=engagement_id,
            event_code=row.event_code,
            recipient_type=row.recipient_type,
            delivery_channel=row.delivery_channel,
            notification_services=serialize_for_db(filtered),
            event_name=row.event_name,
            recipient_config=recipient_cfg,
            condition_key=row.condition_key or "default",
            emit_condition=emit_cond,
            source_binding_id=row.id,
        )

    async def seed_from_global_defaults(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        profile: EngagementNotificationProfile,
        extra_event_codes: frozenset[str] | None = None,
    ) -> list[EngagementNotification]:
        """Clone global default bindings for each applicable event (idempotent per slot)."""
        codes = set(list_applicable_event_codes(profile))
        if extra_event_codes:
            codes |= set(extra_event_codes)

        existing_slots = await self.list_for_engagement(db, engagement_id)
        existing_keys = {
            (s.event_code, s.recipient_type, s.delivery_channel, s.condition_key)
            for s in existing_slots
        }

        created: list[EngagementNotification] = []
        for event_code in sorted(codes):
            global_rows = await self._bindings_repo.list_for_event(
                db, event_code=event_code, engagement_type_id=None
            )
            for row in global_rows:
                if row.engagement_type_id is not None:
                    continue
                slot_key = (
                    map_legacy_event_code(row.event_code),
                    row.recipient_type,
                    row.delivery_channel,
                    row.condition_key or "default",
                )
                if slot_key in existing_keys:
                    continue
                obj = await self._clone_global_row(db, engagement_id=engagement_id, row=row)
                if obj is not None:
                    existing_keys.add(slot_key)
                    created.append(obj)
        return created

    async def populate_from_defaults(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        engagement_type_id: int | None = None,
    ) -> list[EngagementNotification]:
        _ = engagement_type_id
        engagement = await db.get(Engagement, engagement_id)
        if engagement is None:
            return []
        profile = await profile_from_engagement(db, engagement)
        return await self.seed_from_global_defaults(db, engagement_id=engagement_id, profile=profile)

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
