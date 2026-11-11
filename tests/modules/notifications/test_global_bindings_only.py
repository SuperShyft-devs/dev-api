"""Global-only default resolution for notification_events."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from modules.notifications.bindings_repository import NotificationBindingsRepository


@pytest.mark.asyncio
async def test_list_for_event_ignores_type_scoped_rows(test_db_session):
    from tests.helpers.engagement_types import engagement_type_id

    type_id = await engagement_type_id(test_db_session, "bio_ai")
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, delivery_channel,
                notification_services, engagement_type_id
            )
            VALUES
            (
                'reminders.pretest', 'Pretest global', 'user', 'whatsapp',
                CAST(:global_svc AS jsonb), NULL
            ),
            (
                'reminders.pretest', 'Pretest type', 'user', 'whatsapp',
                CAST(:type_svc AS jsonb), :etype
            )
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "global_svc": json.dumps([{"service_key": "global-only"}]),
            "type_svc": json.dumps([{"service_key": "type-only"}]),
            "etype": type_id,
        },
    )
    repo = NotificationBindingsRepository()
    rows = await repo.list_for_event(test_db_session, event_code="reminders.pretest")
    keys = {
        s.get("service_key")
        for row in rows
        for s in (row.notification_services or [])
        if isinstance(s, dict)
    }
    assert "global-only" in keys
    assert "type-only" not in keys
