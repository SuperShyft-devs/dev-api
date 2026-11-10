"""Unit tests for support-query notifications to ops department staff."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from modules.employee.models import Employee
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.notifications.support_notify import notify_default_onboarding_assistants_on_support_query
from modules.platform_settings.repository import PlatformSettingsRepository
from modules.support.models import SupportTicket
from modules.users.models import User
from tests.helpers.employee_departments import seed_employee_departments


async def _seed_notification_service(
    test_db_session,
    *,
    service_key: str,
    is_active: bool = True,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'email', 'notify-admin-contact-query', :active, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = :active"
        ),
        {"sk": service_key, "dn": service_key, "active": is_active},
    )
    await test_db_session.commit()


async def _ensure_packages(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) VALUES "
            "(1, 'P1', 'One', 1) ON CONFLICT (package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'R1', 'D1', 'healthians', 1) ON CONFLICT (diagnostic_package_id) DO UPDATE SET status = EXCLUDED.status"
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_support_notify_dispatches_to_default_assistants(test_db_session, monkeypatch):
    service_key = "support-contact-email"
    await _seed_notification_service(test_db_session, service_key=service_key)
    await _ensure_packages(test_db_session)

    user = User(
        user_id=9401,
        age=30,
        phone="94010000001",
        email="user@example.com",
        first_name="Pat",
        last_name="User",
        status="active",
    )
    test_db_session.add(user)
    await test_db_session.flush()
    test_db_session.add(
        Employee(
            employee_id=9402,
            name="Omar Assistant",
            phone="0000009402",
            email="oa@example.com",
            role="admin",
            status="active",
        )
    )
    await test_db_session.flush()
    await seed_employee_departments(test_db_session, employee_id=9402, departments=["ops"], commit=False)

    await test_db_session.execute(
        text("DELETE FROM notification_events WHERE event_code = 'support.ticket_created'")
    )

    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_support_query_notification) "
            "VALUES (1, 1, 1, :sk)"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    ticket = SupportTicket(
        user_id=9401,
        contact_input="94010000001",
        query_text="Need help with my report",
        status="open",
    )
    test_db_session.add(ticket)
    await test_db_session.flush()

    from modules.notifications.engine import get_notification_engine, reset_notification_engine

    reset_notification_engine()
    dispatch_mock = AsyncMock(return_value={"notification_id": 1})
    engine = get_notification_engine()
    monkeypatch.setattr(engine._notifications_service, "dispatch_to_contacts", dispatch_mock)

    await notify_default_onboarding_assistants_on_support_query(
        test_db_session,
        platform_settings_repository=PlatformSettingsRepository(),
        ticket=ticket,
        user=user,
    )

    dispatch_mock.assert_awaited_once()
    kwargs = dispatch_mock.await_args.kwargs
    assert kwargs["service_key"] == service_key
    assert kwargs["contacts"][0]["phone"] == "0000009402"
    assert kwargs["contacts"][0]["email"] == "oa@example.com"
    pd = kwargs["participant_details"]
    assert pd["support"]["query_text"] == "Need help with my report"
    assert pd["support"]["ticket_id"] == str(ticket.ticket_id)
    assert pd["user"]["name"] == "Pat User"
    assert pd["user"]["phone"] == "94010000001"


@pytest.mark.asyncio
async def test_support_notify_noop_when_no_service_keys(test_db_session, monkeypatch):
    await test_db_session.execute(
        text("DELETE FROM notification_events WHERE event_code = 'support.ticket_created'")
    )
    await test_db_session.commit()
    await _ensure_packages(test_db_session)

    user = User(user_id=9411, age=30, phone="94110000001", status="active")
    test_db_session.add(user)
    await test_db_session.flush()

    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_support_query_notification) "
            "VALUES (1, 1, 1, NULL)"
        )
    )
    await test_db_session.commit()

    ticket = SupportTicket(
        user_id=9411,
        contact_input="94110000001",
        query_text="Hello",
        status="open",
    )
    test_db_session.add(ticket)
    await test_db_session.flush()

    from modules.notifications.engine import get_notification_engine, reset_notification_engine

    reset_notification_engine()
    dispatch_mock = AsyncMock()
    engine = get_notification_engine()
    monkeypatch.setattr(engine._notifications_service, "dispatch_to_contacts", dispatch_mock)

    await notify_default_onboarding_assistants_on_support_query(
        test_db_session,
        platform_settings_repository=PlatformSettingsRepository(),
        ticket=ticket,
        user=user,
    )

    dispatch_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_support_notify_noop_when_no_assistants(test_db_session, monkeypatch):
    service_key = "support-no-assistants"
    await _seed_notification_service(test_db_session, service_key=service_key)
    await _ensure_packages(test_db_session)

    user = User(user_id=9421, age=30, phone="94210000001", status="active")
    test_db_session.add(user)
    await test_db_session.flush()

    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_support_query_notification) "
            "VALUES (1, 1, 1, :sk)"
        ),
        {"sk": service_key},
    )
    await test_db_session.commit()

    ticket = SupportTicket(
        user_id=9421,
        contact_input="94210000001",
        query_text="Anyone there?",
        status="open",
    )
    test_db_session.add(ticket)
    await test_db_session.flush()

    import json

    await test_db_session.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, notification_services, engagement_type_id
            )
            VALUES (
                'support.ticket_created',
                'Support ticket created',
                'departments.ops',
                CAST(:services AS json),
                NULL
            )
            ON CONFLICT ON CONSTRAINT uq_notification_events_binding DO UPDATE
            SET notification_services = EXCLUDED.notification_services
            """
        ),
        {"services": json.dumps([{"service_key": service_key}])},
    )
    await test_db_session.commit()

    from modules.notifications.engine import get_notification_engine, reset_notification_engine

    reset_notification_engine()
    dispatch_mock = AsyncMock()
    engine = get_notification_engine()
    monkeypatch.setattr(engine._notifications_service, "dispatch_to_contacts", dispatch_mock)

    await notify_default_onboarding_assistants_on_support_query(
        test_db_session,
        platform_settings_repository=PlatformSettingsRepository(),
        ticket=ticket,
        user=user,
    )

    dispatch_mock.assert_not_awaited()
