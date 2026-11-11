"""Engagement notifications repository."""

from __future__ import annotations

import copy
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.service_config import (
    extract_service_keys,
    normalize_notification_services,
    prepare_notification_items_for_storage,
    serialize_for_db,
    validate_notification_service_configs,
)
from modules.engagements.models import Engagement, EngagementNotification
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.events_catalog import get_event, map_legacy_event_code


class EngagementNotificationsRepository:
    """Database queries for engagement_notifications (per-engagement binding slots)."""

    def __init__(self) -> None:
        self._bindings_repo = NotificationBindingsRepository()

    async def list_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
        *,
        event_code: str | None = None,
    ) -> list[EngagementNotification]:
        q = (
            select(EngagementNotification)
            .where(EngagementNotification.engagement_id == engagement_id)
            .order_by(
                EngagementNotification.event_code.asc(),
                EngagementNotification.recipient_type.asc(),
                EngagementNotification.delivery_channel.asc(),
                EngagementNotification.id.asc(),
            )
        )
        if event_code is not None:
            q = q.where(EngagementNotification.event_code == map_legacy_event_code(event_code))
        result = await db.execute(q)
        return list(result.scalars().all())

    async def get_by_id(
        self, db: AsyncSession, binding_id: int
    ) -> EngagementNotification | None:
        result = await db.execute(
            select(EngagementNotification).where(EngagementNotification.id == binding_id)
        )
        return result.scalar_one_or_none()

    async def find_binding_by_slot(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
        recipient_type: str,
        delivery_channel: str,
        condition_key: str = "default",
    ) -> EngagementNotification | None:
        normalized = map_legacy_event_code(event_code)
        result = await db.execute(
            select(EngagementNotification).where(
                EngagementNotification.engagement_id == engagement_id,
                EngagementNotification.event_code == normalized,
                EngagementNotification.recipient_type == recipient_type,
                EngagementNotification.delivery_channel == delivery_channel,
                EngagementNotification.condition_key == condition_key,
            )
        )
        return result.scalar_one_or_none()

    async def list_for_event(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
    ) -> list[EngagementNotification]:
        return await self.list_for_engagement(db, engagement_id, event_code=event_code)

    async def get_for_engagement_and_event_code(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
        recipient_type: str = "user",
    ) -> EngagementNotification | None:
        """Primary participant-facing binding when multiple rows exist."""
        rows = await self.list_for_event(db, engagement_id=engagement_id, event_code=event_code)
        for row in rows:
            if row.recipient_type == recipient_type and extract_service_keys(row.notification_services):
                return row
        for row in rows:
            if row.recipient_type == recipient_type:
                return row
        return rows[0] if rows else None

    async def get_services_for_engagement_event(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
        recipient_type: str = "user",
    ) -> list[str]:
        en = await self.get_for_engagement_and_event_code(
            db,
            engagement_id=engagement_id,
            event_code=event_code,
            recipient_type=recipient_type,
        )
        if en is None:
            return []
        return extract_service_keys(en.notification_services)

    async def create_binding(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        event_code: str,
        event_name: str,
        recipient_type: str,
        delivery_channel: str,
        notification_services: list,
        recipient_config: dict | None = None,
        condition_key: str = "default",
        emit_condition: dict | None = None,
    ) -> EngagementNotification:
        obj = EngagementNotification(
            engagement_id=engagement_id,
            event_code=map_legacy_event_code(event_code),
            event_name=event_name,
            recipient_type=recipient_type,
            delivery_channel=delivery_channel,
            notification_services=notification_services,
            recipient_config=recipient_config,
            condition_key=condition_key or "default",
            emit_condition=emit_condition,
        )
        db.add(obj)
        await db.flush()
        return obj

    async def update_binding(
        self,
        db: AsyncSession,
        row: EngagementNotification,
        *,
        event_name: str | None = None,
        recipient_type: str | None = None,
        delivery_channel: str | None = None,
        notification_services: list | None = None,
        recipient_config: dict | None = None,
        condition_key: str | None = None,
        emit_condition: dict | None = None,
    ) -> EngagementNotification:
        if event_name is not None:
            row.event_name = event_name
        if recipient_type is not None:
            row.recipient_type = recipient_type
        if delivery_channel is not None:
            row.delivery_channel = delivery_channel
        if notification_services is not None:
            row.notification_services = notification_services
        if recipient_config is not None:
            row.recipient_config = recipient_config
        if condition_key is not None:
            row.condition_key = condition_key
        if emit_condition is not None:
            row.emit_condition = emit_condition
        await db.flush()
        return row

    async def delete_binding(self, db: AsyncSession, binding_id: int) -> bool:
        row = await self.get_by_id(db, binding_id)
        if row is None:
            return False
        await db.delete(row)
        await db.flush()
        return True

    async def replace_all_bindings(
        self,
        db: AsyncSession,
        engagement_id: int,
        items: list[dict],
    ) -> list[EngagementNotification]:
        """Replace full engagement binding set (admin save)."""
        prepared = await self._prepare_binding_items(db, items)
        await db.execute(
            delete(EngagementNotification).where(
                EngagementNotification.engagement_id == engagement_id
            )
        )
        result: list[EngagementNotification] = []
        for item in prepared:
            obj = await self.create_binding(db, engagement_id=engagement_id, **item)
            result.append(obj)
        return result

    async def upsert_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
        notifications: list[dict],
    ) -> list[EngagementNotification]:
        """Legacy bulk replace; accepts binding-shaped dicts."""
        return await self.replace_all_bindings(db, engagement_id, notifications)

    async def copy_global_bindings_to_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
        profile,
        *,
        replace_existing: bool = True,
    ) -> list[EngagementNotification]:
        from modules.notifications.event_applicability import list_applicable_event_codes

        if replace_existing:
            await db.execute(
                delete(EngagementNotification).where(
                    EngagementNotification.engagement_id == engagement_id
                )
            )

        created: list[EngagementNotification] = []
        for event_code in list_applicable_event_codes(profile):
            global_rows = await self._bindings_repo.list_for_event(db, event_code=event_code)
            if not global_rows:
                continue
            ev = get_event(event_code)
            event_name = ev.name if ev else event_code
            for grow in global_rows:
                services = grow.notification_services or []
                if not extract_service_keys(services):
                    continue
                row = await self.create_binding(
                    db,
                    engagement_id=engagement_id,
                    event_code=event_code,
                    event_name=grow.event_name or event_name,
                    recipient_type=grow.recipient_type,
                    delivery_channel=grow.delivery_channel,
                    notification_services=copy.deepcopy(services),
                    recipient_config=(
                        dict(grow.recipient_config)
                        if isinstance(grow.recipient_config, dict)
                        else None
                    ),
                    condition_key=grow.condition_key or "default",
                    emit_condition=(
                        dict(grow.emit_condition)
                        if isinstance(grow.emit_condition, dict)
                        else None
                    ),
                )
                created.append(row)
        return created

    async def populate_from_defaults(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        engagement_type_id: int,
    ) -> list[EngagementNotification]:
        del engagement_type_id
        from modules.notifications.event_applicability import profile_from_engagement

        engagement = await db.get(Engagement, engagement_id)
        if engagement is None:
            return []
        profile = await profile_from_engagement(db, engagement)
        return await self.copy_global_bindings_to_engagement(
            db, engagement_id, profile, replace_existing=True
        )

    async def _prepare_binding_items(
        self, db: AsyncSession, notifications: list[dict]
    ) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        for item in notifications:
            event_code = item.get("event_code")
            if not event_code:
                continue
            event_code = map_legacy_event_code(str(event_code).strip())
            ev = get_event(event_code)
            if ev is None:
                continue
            configs = await validate_notification_service_configs(
                db,
                notification_services=item.get("notification_services"),
                event_label=event_code,
            )
            if not configs:
                continue
            recipient_type = (
                str(item.get("recipient_type") or "").strip()
                or (ev.default_recipients[0] if ev.default_recipients else "user")
            )
            delivery_channel = str(item.get("delivery_channel") or "whatsapp").strip().lower()
            condition_key = str(item.get("condition_key") or "default").strip() or "default"
            rc = item.get("recipient_config")
            recipient_config = dict(rc) if isinstance(rc, dict) else None
            ec = item.get("emit_condition")
            emit_condition = dict(ec) if isinstance(ec, dict) else None
            prepared.append(
                {
                    "event_code": event_code,
                    "event_name": str(item.get("event_name") or ev.name),
                    "recipient_type": recipient_type,
                    "delivery_channel": delivery_channel,
                    "notification_services": serialize_for_db(configs),
                    "recipient_config": recipient_config,
                    "condition_key": condition_key,
                    "emit_condition": emit_condition,
                }
            )
        return prepared
