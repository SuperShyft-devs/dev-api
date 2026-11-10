"""Shared helpers for notification event binding test fixtures."""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def upsert_engagement_notification(
    session: AsyncSession,
    *,
    engagement_id: int,
    event_code: str,
    service_keys: list[str] | str,
) -> None:
    if isinstance(service_keys, str):
        keys = [k.strip() for k in service_keys.split(",") if k.strip()]
    else:
        keys = list(service_keys)
    services_json = json.dumps(
        [{"service_key": key, "external_link": None} for key in keys]
    )
    await session.execute(
        text(
            """
            INSERT INTO engagement_notifications (engagement_id, event_code, notification_services)
            VALUES (:eid, :event_code, CAST(:services AS jsonb))
            ON CONFLICT (engagement_id, event_code) DO UPDATE
            SET notification_services = EXCLUDED.notification_services
            """
        ),
        {"eid": engagement_id, "event_code": event_code, "services": services_json},
    )
