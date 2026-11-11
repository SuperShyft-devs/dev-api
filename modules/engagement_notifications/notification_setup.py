"""Build applicable notification events + defaults for engagement admin UI."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.service_config import (
    configs_to_api,
    normalize_notification_services,
)
from modules.engagements.models import Engagement
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.bindings_service import NotificationBindingsService
from modules.notifications.event_applicability import (
    EngagementNotificationProfile,
    engagement_notification_applicable,
    list_applicable_event_codes,
    profile_from_engagement,
    profile_from_mapping,
    profile_key,
    profile_label,
    show_on_engagement_wizard,
)
from modules.notifications.events_catalog import get_event, list_events


def _slot_to_api(row) -> dict[str, Any]:
    return {
        "id": row.id,
        "engagement_id": row.engagement_id,
        "event_code": row.event_code,
        "event_name": row.event_name,
        "recipient_type": row.recipient_type,
        "delivery_channel": row.delivery_channel,
        "notification_services": configs_to_api(row.notification_services),
        "recipient_config": row.recipient_config,
        "condition_key": row.condition_key,
        "emit_condition": row.emit_condition,
        "source_binding_id": row.source_binding_id,
    }


def _global_binding_to_api(row) -> dict[str, Any]:
    return NotificationBindingsService()._to_response(row).model_dump()


async def build_notification_setup_for_profile(
    db: AsyncSession,
    profile: EngagementNotificationProfile,
    *,
    engagement_id: int | None = None,
    extra_event_codes: frozenset[str] | None = None,
) -> dict[str, Any]:
    bindings_repo = NotificationBindingsRepository()
    en_repo = EngagementNotificationsRepository()

    applicable = set(list_applicable_event_codes(profile))
    engagement_slots_by_event: dict[str, list] = {}
    extra_codes: set[str] = set()
    if engagement_id is not None:
        for slot in await en_repo.list_for_engagement(db, engagement_id):
            engagement_slots_by_event.setdefault(slot.event_code, []).append(slot)
            if slot.event_code not in applicable:
                extra_codes.add(slot.event_code)
    if extra_event_codes:
        extra_codes |= set(extra_event_codes)

    all_event_codes = sorted(applicable | extra_codes)
    events_out: list[dict[str, Any]] = []
    for event_code in all_event_codes:
        ev = get_event(event_code)
        global_rows = await bindings_repo.list_for_event(
            db, event_code=event_code, engagement_type_id=None
        )
        global_only = [r for r in global_rows if r.engagement_type_id is None]
        slots = engagement_slots_by_event.get(event_code, [])
        events_out.append(
            {
                "event_code": event_code,
                "name": ev.name if ev else event_code,
                "is_applicable": event_code in applicable,
                "is_extra": event_code in extra_codes and event_code not in applicable,
                "global_bindings": [_global_binding_to_api(r) for r in global_only],
                "engagement_bindings": [_slot_to_api(s) for s in slots],
            }
        )

    return {
        "profile_key": profile_key(profile),
        "profile_label": profile_label(profile),
        "engagement_id": engagement_id,
        "events": events_out,
        "applicable_event_count": len(applicable),
    }


async def build_notification_setup_for_engagement(
    db: AsyncSession,
    engagement_id: int,
) -> dict[str, Any]:
    row = await db.get(Engagement, engagement_id)
    if row is None:
        return {
            "profile_key": "",
            "profile_label": "",
            "engagement_id": engagement_id,
            "events": [],
            "applicable_event_count": 0,
        }
    profile = await profile_from_engagement(db, row)
    return await build_notification_setup_for_profile(db, profile, engagement_id=engagement_id)


def list_addable_event_codes(profile: EngagementNotificationProfile) -> list[dict[str, str]]:
    applicable = set(list_applicable_event_codes(profile))
    out: list[dict[str, str]] = []
    for ev in list_events():
        if not show_on_engagement_wizard(ev.code):
            continue
        if ev.code in applicable:
            continue
        if engagement_notification_applicable(ev.code, profile):
            continue
        out.append({"code": ev.code, "name": ev.name})
    return sorted(out, key=lambda x: x["code"])


async def collect_global_default_services_for_applicable(
    db: AsyncSession,
    profile: EngagementNotificationProfile,
) -> list[dict]:
    """Legacy helper: one merged service list per applicable event."""
    bindings_repo = NotificationBindingsRepository()
    from modules.engagement_notifications.repository import (
        EngagementNotificationsRepository,
    )

    en_repo = EngagementNotificationsRepository()
    items: list[dict] = []
    for event_code in list_applicable_event_codes(profile):
        bindings = await bindings_repo.list_for_event(db, event_code=event_code, engagement_type_id=None)
        merged = []
        seen: set[str] = set()
        for row in bindings:
            if row.engagement_type_id is not None:
                continue
            for cfg in normalize_notification_services(row.notification_services):
                if cfg.service_key and cfg.service_key not in seen:
                    seen.add(cfg.service_key)
                    merged.append(cfg.model_dump(mode="json"))
        if merged:
            items.append(
                {
                    "event_code": event_code,
                    "notification_services": merged,
                }
            )
    _ = en_repo
    return items
