"""Tests for engagement notification default population."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from modules.engagement_notifications.repository import EngagementNotificationsRepository


@pytest.mark.asyncio
async def test_populate_from_defaults_merges_global_when_type_empty(test_db_session):
    from tests.helpers.engagement_types import engagement_type_id

    type_id = await engagement_type_id(test_db_session, "bio_ai")
    eid = 88001
    await test_db_session.execute(
        text(
            """
            INSERT INTO engagements (
                engagement_id, engagement_name, engagement_code, engagement_type,
                assessment_package_id, diagnostic_package_id, city, slot_duration,
                start_date, end_date, status
            )
            VALUES (
                :eid, 'E2E Notify', 'E2E88001', :etype, 1, 1, 'BLR', 20,
                '2026-12-01', '2026-12-01', 3
            )
            ON CONFLICT (engagement_id) DO NOTHING
            """
        ),
        {"eid": eid, "etype": type_id},
    )
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_services (
                service_key, display_name, channel, webhook_path, is_active
            )
            VALUES ('pretest-whatsapp', 'Pretest', 'whatsapp', '/pretest', true)
            ON CONFLICT (service_key) DO NOTHING
            """
        )
    )
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, delivery_channel,
                notification_services, engagement_type_id
            )
            VALUES (
                'reminders.pretest', 'Pretest', 'user', 'whatsapp',
                CAST(:services AS jsonb), NULL
            )
            """
        ),
        {"services": json.dumps([{"service_key": "pretest-whatsapp"}])},
    )
    repo = EngagementNotificationsRepository()
    rows = await repo.populate_from_defaults(
        test_db_session, engagement_id=eid, engagement_type_id=type_id
    )
    await test_db_session.commit()
    assert rows
    codes = {r.event_code for r in rows}
    assert "reminders.pretest" in codes
