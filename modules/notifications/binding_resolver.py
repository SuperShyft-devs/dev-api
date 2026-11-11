"""Resolve effective notification bindings for an event emit."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.service_config import (
    extract_service_keys,
    normalize_notification_services,
    serialize_for_db,
)
from modules.engagements.models import Engagement
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.emit_condition import matches_condition
from modules.notifications.event_applicability import engagement_applicability_for_event
from modules.notifications.events_catalog import get_event


@dataclass(frozen=True)
class ResolvedBinding:
    recipient_type: str
    notification_services: list[dict]
    event_name: str
    recipient_config: dict | None = None


def _is_platform_only_event(event_code: str) -> bool:
    rule = engagement_applicability_for_event(event_code)
    return rule is not None and rule.platform_only


def _resolve_rows(
    rows: list[Any],
    *,
    ctx: dict,
    default_event_name: str,
) -> list[ResolvedBinding]:
    resolved: list[ResolvedBinding] = []
    for row in rows:
        raw_condition = getattr(row, "emit_condition", None)
        condition = raw_condition if isinstance(raw_condition, dict) else None
        if not matches_condition(ctx, condition):
            continue
        services = row.notification_services
        if not extract_service_keys(services):
            continue
        event_name = getattr(row, "event_name", None) or default_event_name
        resolved.append(
            ResolvedBinding(
                recipient_type=row.recipient_type,
                notification_services=services,
                event_name=event_name,
                recipient_config=(
                    row.recipient_config if isinstance(row.recipient_config, dict) else None
                ),
            )
        )
    return resolved


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

        use_engagement_slots = (
            engagement_id is not None and not _is_platform_only_event(event_code)
        )

        if use_engagement_slots:
            slots = await self._en_repo.list_slots_for_event(
                db, engagement_id=engagement_id, event_code=event_code
            )
            if slots:
                return _resolve_rows(
                    slots,
                    ctx=ctx,
                    default_event_name=ev.name,
                )

        if engagement_type_id is None and engagement_id is not None:
            engagement_type_id = await self._engagement_type_id(db, engagement_id)

        bindings = await self._bindings.list_for_event(
            db,
            event_code=event_code,
            engagement_type_id=engagement_type_id,
        )
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

        return _resolve_rows(bindings, ctx=ctx, default_event_name=ev.name)

    async def _engagement_type_id(self, db: AsyncSession, engagement_id: int) -> int | None:
        result = await db.execute(
            select(Engagement.engagement_type).where(Engagement.engagement_id == engagement_id)
        )
        value = result.scalar_one_or_none()
        return int(value) if value is not None else None
