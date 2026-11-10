"""Resolve effective notification bindings for an event emit."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.service_config import (
    extract_service_keys,
    normalize_notification_services,
    serialize_for_db,
)
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagements.models import Engagement
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.events_catalog import get_event


@dataclass(frozen=True)
class ResolvedBinding:
    recipient_type: str
    notification_services: list[dict]
    event_name: str


class BindingResolver:
    def __init__(
        self,
        bindings_repo: NotificationBindingsRepository | None = None,
        en_repo: EngagementNotificationsRepository | None = None,
    ) -> None:
        self._bindings = bindings_repo or NotificationBindingsRepository()
        self._en_repo = en_repo or EngagementNotificationsRepository()

    async def resolve(
        self,
        db: AsyncSession,
        *,
        event_code: str,
        engagement_id: int | None = None,
        engagement_type_id: int | None = None,
        context: dict | None = None,
    ) -> list[ResolvedBinding]:
        ctx = context or {}
        ev = get_event(event_code)
        if ev is None:
            return []

        if engagement_type_id is None and engagement_id is not None:
            engagement_type_id = await self._engagement_type_id(db, engagement_id)

        bindings = await self._bindings.list_for_event(
            db,
            event_code=event_code,
            engagement_type_id=engagement_type_id,
        )
        if not bindings:
            fallback_services = ctx.get("fallback_service_keys") or []
            if isinstance(fallback_services, str):
                fallback_services = [fallback_services]
            if ctx.get("service_key"):
                fallback_services = list(fallback_services) + [str(ctx["service_key"])]
            if fallback_services and ev.default_recipients:
                return [
                    ResolvedBinding(
                        recipient_type=ev.default_recipients[0],
                        notification_services=serialize_for_db(
                            normalize_notification_services(
                                [{"service_key": k} for k in fallback_services if k]
                            )
                        ),
                        event_name=ev.name,
                    )
                ]
            return []

        override_services: list[dict] | None = None
        if engagement_id is not None:
            en = await self._en_repo.get_for_engagement_and_event_code(
                db, engagement_id=engagement_id, event_code=event_code
            )
            if en is not None and extract_service_keys(en.notification_services):
                override_services = en.notification_services

        resolved: list[ResolvedBinding] = []
        seen_recipient: set[str] = set()
        for row in bindings:
            if row.recipient_type in seen_recipient:
                continue
            seen_recipient.add(row.recipient_type)
            services = override_services if override_services is not None else row.notification_services
            if not extract_service_keys(services):
                continue
            resolved.append(
                ResolvedBinding(
                    recipient_type=row.recipient_type,
                    notification_services=services,
                    event_name=row.event_name,
                )
            )
        return resolved

    async def _engagement_type_id(self, db: AsyncSession, engagement_id: int) -> int | None:
        result = await db.execute(
            select(Engagement.engagement_type).where(Engagement.engagement_id == engagement_id)
        )
        value = result.scalar_one_or_none()
        return int(value) if value is not None else None
