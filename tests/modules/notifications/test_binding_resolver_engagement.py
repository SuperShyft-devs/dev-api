"""BindingResolver uses engagement_notifications only when engagement_id is set."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from modules.notifications.binding_resolver import BindingResolver


@pytest.mark.asyncio
async def test_engagement_resolver_ignores_global(test_db_session):
    from tests.helpers.engagement_types import engagement_type_id

    type_id = await engagement_type_id(test_db_session, "blood_test")
    eid = 88002
    await test_db_session.execute(
        text(
            """
            INSERT INTO engagements (
                engagement_id, engagement_name, engagement_code, engagement_type,
                assessment_package_id, diagnostic_package_id, city, slot_duration,
                start_date, end_date, status, blood_collection_type
            )
            VALUES (
                :eid, 'Resolve Eng', 'E2E88002', :etype, 1, 1, 'BLR', 20,
                '2026-12-02', '2026-12-02', 3, 'camp_collection'
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
            VALUES
            ('global-only-svc', 'G', 'whatsapp', '/g', true),
            ('eng-only-svc', 'E', 'whatsapp', '/e', true)
            ON CONFLICT (service_key) DO NOTHING
            """
        )
    )
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, delivery_channel,
                notification_services, engagement_type_id, condition_key
            )
            VALUES (
                'reminders.pretest', 'Pretest', 'user', 'whatsapp',
                CAST(:svc AS jsonb), NULL, 'default'
            )
            """
        ),
        {"svc": json.dumps([{"service_key": "global-only-svc"}])},
    )
    await test_db_session.execute(
        text(
            """
            INSERT INTO engagement_notifications (
                engagement_id, event_code, event_name, recipient_type, delivery_channel,
                notification_services, condition_key
            )
            VALUES (
                :eid, 'reminders.pretest', 'Pretest', 'user', 'whatsapp',
                CAST(:svc AS jsonb), 'default'
            )
            """
        ),
        {"eid": eid, "svc": json.dumps([{"service_key": "eng-only-svc"}])},
    )
    await test_db_session.commit()

    resolver = BindingResolver()
    resolved = await resolver.resolve(
        test_db_session,
        event_code="reminders.pretest",
        engagement_id=eid,
    )
    keys = {
        s.get("service_key")
        for b in resolved
        for s in b.notification_services
        if isinstance(s, dict)
    }
    assert "eng-only-svc" in keys
    assert "global-only-svc" not in keys
