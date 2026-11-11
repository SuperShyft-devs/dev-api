"""Build applicable notification events + bindings for engagement admin UI."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagement_notifications.engagement_bindings_service import engagement_binding_to_response
from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.notifications.bindings_repository import NotificationBindingsRepository
from modules.notifications.bindings_service import NotificationBindingsService
from modules.notifications.event_applicability import (
    catalog_groups_for_event,
    profile_from_engagement,
)
from modules.notifications.events_catalog import get_event


async def build_notification_setup_for_profile(
    db: AsyncSession,
    profile,
    *,
    engagement_id: int | None = None,
) -> list[dict[str, Any]]:
    from modules.notifications.event_applicability import list_applicable_event_codes

    bindings_repo = NotificationBindingsRepository()
    en_repo = EngagementNotificationsRepository()
    global_svc = NotificationBindingsService(bindings_repo)
    engagement_by_event: dict[str, list[dict]] = {}
    if engagement_id is not None:
        for row in await en_repo.list_for_engagement(db, engagement_id):
            engagement_by_event.setdefault(row.event_code, []).append(
                engagement_binding_to_response(row)
            )

    result: list[dict[str, Any]] = []
    for event_code in list_applicable_event_codes(profile):
        ev = get_event(event_code)
        global_rows = await bindings_repo.list_for_event(db, event_code=event_code)
        global_bindings = [global_svc._to_response(r).model_dump() for r in global_rows]
        result.append(
            {
                "event_code": event_code,
                "name": ev.name if ev else event_code,
                "catalog_groups": list(catalog_groups_for_event(event_code)),
                "global_bindings": global_bindings,
                "engagement_bindings": engagement_by_event.get(event_code, []),
            }
        )
    return result


async def build_notification_setup_for_engagement(
    db: AsyncSession,
    engagement_id: int,
) -> list[dict[str, Any]]:
    from modules.engagements.models import Engagement

    row = await db.get(Engagement, engagement_id)
    if row is None:
        return []
    profile = await profile_from_engagement(db, row)
    return await build_notification_setup_for_profile(db, profile, engagement_id=engagement_id)
