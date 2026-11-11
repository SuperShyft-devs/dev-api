"""Build applicable notification events + defaults for engagement admin UI."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.engagement_notifications.service_config import (
    NotificationServiceConfigItem,
    extract_service_keys,
    normalize_notification_services,
    serialize_for_db,
)
from modules.engagements.models import Engagement
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.event_applicability import (
    catalog_groups_for_event,
    profile_from_engagement,
)
from modules.notifications.events_catalog import get_event
from modules.notifications.models import NotificationService
from sqlalchemy import select


def _merge_service_configs(bindings: list) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in bindings:
        for cfg in normalize_notification_services(row.notification_services):
            if cfg.service_key and cfg.service_key not in seen:
                seen.add(cfg.service_key)
                out.append(cfg.model_dump(mode="json"))
    return out


async def build_notification_setup_for_profile(
    db: AsyncSession,
    profile,
    *,
    engagement_id: int | None = None,
) -> list[dict[str, Any]]:
    from modules.notifications.event_applicability import list_applicable_event_codes

    bindings_repo = NotificationBindingsRepository()
    en_repo = EngagementNotificationsRepository()
    overrides: dict[str, list] = {}
    if engagement_id is not None:
        for row in await en_repo.list_for_engagement(db, engagement_id):
            overrides[row.event_code] = row.notification_services or []

    result: list[dict[str, Any]] = []
    for event_code in list_applicable_event_codes(profile):
        ev = get_event(event_code)
        bindings = await bindings_repo.list_for_event(db, event_code=event_code)
        default_services = _merge_service_configs(bindings)
        override_raw = overrides.get(event_code)
        override_services = (
            [c.model_dump(mode="json") for c in normalize_notification_services(override_raw)]
            if override_raw
            else []
        )
        result.append(
            {
                "event_code": event_code,
                "name": ev.name if ev else event_code,
                "catalog_groups": list(catalog_groups_for_event(event_code)),
                "default_services": default_services,
                "override_services": override_services,
            }
        )
    return result


async def build_notification_setup_for_engagement(
    db: AsyncSession,
    engagement_id: int,
) -> list[dict[str, Any]]:
    row = await db.get(Engagement, engagement_id)
    if row is None:
        return []
    profile = await profile_from_engagement(db, row)
    return await build_notification_setup_for_profile(db, profile, engagement_id=engagement_id)


async def collect_global_default_services_for_applicable(
    db: AsyncSession,
    profile,
) -> list[dict]:
    """Services to seed engagement_notifications (populate_from_defaults)."""
    bindings_repo = NotificationBindingsRepository()
    items: list[dict] = []
    from modules.notifications.event_applicability import list_applicable_event_codes

    for event_code in list_applicable_event_codes(profile):
        bindings = await bindings_repo.list_for_event(db, event_code=event_code)
        configs = _merge_service_configs(bindings)
        if not configs:
            continue
        keys = [c["service_key"] for c in configs if c.get("service_key")]
        if not keys:
            continue
        svc_rows = await db.execute(
            select(NotificationService).where(NotificationService.service_key.in_(keys))
        )
        services_by_key = {s.service_key: s for s in svc_rows.scalars().all()}
        filtered: list[NotificationServiceConfigItem] = []
        for cfg in configs:
            sk = str(cfg.get("service_key") or "")
            svc = services_by_key.get(sk)
            if svc is None:
                continue
            link = cfg.get("external_link")
            if svc.require_external_link and not (link and str(link).strip()):
                link = "https://app.supershyft.com"
            filtered.append(
                NotificationServiceConfigItem(
                    service_key=sk,
                    external_link=str(link).strip() if link else None,
                )
            )
        if filtered:
            items.append(
                {
                    "event_code": event_code,
                    "notification_services": serialize_for_db(filtered),
                }
            )
    return items
