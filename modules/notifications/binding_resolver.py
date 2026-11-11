"""Resolve effective notification bindings for an event emit."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.service_config import (
    extract_service_keys,
    normalize_notification_services,
    serialize_for_db,
)
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.emit_condition import matches_condition
from modules.notifications.events_catalog import get_event


@dataclass(frozen=True)
class ResolvedBinding:
    recipient_type: str
    notification_services: list[dict]
    event_name: str
    recipient_config: dict | None = None


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
        del engagement_type_id
        ctx = context or {}
        ev = get_event(event_code)
        if ev is None:
            return []

        if engagement_id is not None:
            rows = await self._en_repo.list_for_event(
                db, engagement_id=engagement_id, event_code=event_code
            )
            resolved: list[ResolvedBinding] = []
            for row in rows:
                raw_condition = getattr(row, "emit_condition", None)
                condition = raw_condition if isinstance(raw_condition, dict) else None
                if not matches_condition(ctx, condition):
                    continue
                services = row.notification_services
                if not extract_service_keys(services):
                    continue
                resolved.append(
                    ResolvedBinding(
                        recipient_type=row.recipient_type,
                        notification_services=services,
                        event_name=row.event_name or (ev.name if ev else event_code),
                        recipient_config=(
                            row.recipient_config if isinstance(row.recipient_config, dict) else None
                        ),
                    )
                )
            return resolved

        bindings = await self._bindings.list_for_event(db, event_code=event_code)
        has_configured_services = any(
            extract_service_keys(row.notification_services) for row in bindings
        )
        if not bindings or not has_configured_services:
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
                        recipient_config=None,
                    )
                ]
            return []

        resolved = []
        for row in bindings:
            raw_condition = getattr(row, "emit_condition", None)
            condition = raw_condition if isinstance(raw_condition, dict) else None
            if not matches_condition(ctx, condition):
                continue
            services = row.notification_services
            if not extract_service_keys(services):
                continue
            resolved.append(
                ResolvedBinding(
                    recipient_type=row.recipient_type,
                    notification_services=services,
                    event_name=row.event_name,
                    recipient_config=(
                        row.recipient_config if isinstance(row.recipient_config, dict) else None
                    ),
                )
            )
        return resolved
