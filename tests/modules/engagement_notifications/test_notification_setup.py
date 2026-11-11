"""Engagement notification profile setup and seed tests."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from modules.engagement_notifications.repository import EngagementNotificationsRepository
from modules.notifications.binding_resolver import BindingResolver
from modules.notifications.event_applicability import (
    engagement_notification_applicable,
    profile_from_profile_key,
    profile_key,
)
def test_profile_key_matches_canonical_camp_bio_ai():
    profile = profile_from_profile_key(
        "engagement_type_bio_aiandpayment_type_paid_by_groupandblood_collection_campandauto_booking_off"
    )
    assert profile_key(profile) == (
        "engagement_type_bio_aiandpayment_type_paid_by_groupandblood_collection_campandauto_booking_off"
    )
    assert engagement_notification_applicable("reminders.booking_guide", profile)


def test_profile_key_home_excludes_camp_reminder():
    profile = profile_from_profile_key(
        "engagement_type_bio_aiandpayment_type_paid_by_patientandblood_collection_homeandauto_booking_on"
    )
    assert not engagement_notification_applicable("reminders.booking_guide", profile)
    assert engagement_notification_applicable("bookings.confirmed", profile)


@pytest.mark.asyncio
async def test_seed_from_global_defaults_clones_bindings(test_db_session):
    from tests.helpers.engagement_types import engagement_type_id

    type_id = await engagement_type_id(test_db_session, "bio_ai")
    eid = 88002
    await test_db_session.execute(
        text(
            """
            INSERT INTO engagements (
                engagement_id, engagement_name, engagement_code, engagement_type,
                assessment_package_id, diagnostic_package_id, city, slot_duration,
                start_date, end_date, status, blood_collection_type, payment_type
            )
            VALUES (
                :eid, 'Seed Notify', 'E2E88002', :etype, 1, 1, 'BLR', 20,
                '2026-12-01', '2026-12-01', 3, 'camp_collection', 'paid_by_group'
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
                notification_services, engagement_type_id, condition_key
            )
            VALUES (
                'reminders.pretest', 'Pretest', 'user', 'whatsapp',
                CAST(:services AS jsonb), NULL, 'default'
            )
            ON CONFLICT DO NOTHING
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

    resolver = BindingResolver()
    resolved = await resolver.resolve(
        test_db_session,
        event_code="reminders.pretest",
        engagement_id=eid,
    )
    assert resolved
    assert resolved[0].recipient_type == "user"


@pytest.mark.asyncio
async def test_resolver_falls_back_to_global_without_engagement_slots(test_db_session):
    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_services (
                service_key, display_name, channel, webhook_path, is_active
            )
            VALUES ('fallback-pretest', 'P', 'whatsapp', '/p', true)
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
            ON CONFLICT DO NOTHING
            """
        ),
        {"svc": json.dumps([{"service_key": "fallback-pretest"}])},
    )
    await test_db_session.commit()

    resolver = BindingResolver()
    resolved = await resolver.resolve(
        test_db_session,
        event_code="reminders.pretest",
        engagement_id=999999,
    )
    assert resolved
    keys = {
        s.get("service_key")
        for b in resolved
        for s in b.notification_services
        if isinstance(s, dict)
    }
    assert "fallback-pretest" in keys


@pytest.mark.asyncio
async def test_resolver_platform_event_uses_global_even_with_engagement_slots(test_db_session):
    from tests.helpers.engagement_types import engagement_type_id

    type_id = await engagement_type_id(test_db_session, "bio_ai")
    eid = 88003
    await test_db_session.execute(
        text(
            """
            INSERT INTO engagements (
                engagement_id, engagement_name, engagement_code, engagement_type,
                assessment_package_id, diagnostic_package_id, city, slot_duration,
                start_date, end_date, status
            )
            VALUES (
                :eid, 'Otp Eng', 'E2E88003', :etype, 1, 1, 'BLR', 20,
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
            VALUES
            ('global-otp', 'GO', 'whatsapp', '/go', true),
            ('eng-otp-wrong', 'EW', 'whatsapp', '/ew', true)
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
                'auth.otp_sent', 'OTP', 'user', 'whatsapp',
                CAST(:svc AS jsonb), NULL, 'default'
            )
            ON CONFLICT DO NOTHING
            """
        ),
        {"svc": json.dumps([{"service_key": "global-otp"}])},
    )
    await test_db_session.execute(
        text(
            """
            INSERT INTO engagement_notifications (
                engagement_id, event_code, event_name, recipient_type, delivery_channel,
                notification_services, condition_key
            )
            VALUES (
                :eid, 'auth.otp_sent', 'OTP', 'user', 'whatsapp',
                CAST(:svc AS jsonb), 'default'
            )
            """
        ),
        {"eid": eid, "svc": json.dumps([{"service_key": "eng-otp-wrong"}])},
    )
    await test_db_session.commit()

    resolver = BindingResolver()
    resolved = await resolver.resolve(
        test_db_session,
        event_code="auth.otp_sent",
        engagement_id=eid,
    )
    keys = {
        s.get("service_key")
        for b in resolved
        for s in b.notification_services
        if isinstance(s, dict)
    }
    assert "global-otp" in keys
    assert "eng-otp-wrong" not in keys
