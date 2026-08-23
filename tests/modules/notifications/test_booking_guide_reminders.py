"""Tests for booking guide reminder dispatch."""

from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.booking_guide_reminders import dispatch_booking_guide_reminders
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService

BOOKING_GUIDE_WHATSAPP_KEY = "phlebo-booking-guide-whatsapp"
BOOKING_GUIDE_EMAIL_KEY = "booking-guide-email"
DEFAULT_BOOKING_GUIDE_KEYS = f"{BOOKING_GUIDE_WHATSAPP_KEY},{BOOKING_GUIDE_EMAIL_KEY}"


async def _seed_dependencies(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (1, 'PKG1', 'Test Package', 'active') ON CONFLICT (package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    for service_key, channel, webhook_path in (
        (BOOKING_GUIDE_WHATSAPP_KEY, "whatsapp", "send-phlebo-booking-guide-whatsapp-v1"),
        (BOOKING_GUIDE_EMAIL_KEY, "email", "booking-guide-email"),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO notification_services "
                "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, "
                "require_bio_ai_report_url, require_participant_detail) "
                "VALUES (:sk, :dn, :ch, :wp, true, false, false, false) "
                "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
            ),
            {"sk": service_key, "dn": service_key, "ch": channel, "wp": webhook_path},
        )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai', 'BioAI', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
        )
    ).one()
    type_id = int(type_row[0])
    await test_db_session.execute(
        text(
            "INSERT INTO auto_notification_events (event_code, display_name, engagement_type_ids, description) "
            "VALUES ('booking_guide', 'Booking Guide', :type_ids, 'Test booking guide event') "
            "ON CONFLICT (event_code) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"type_ids": [type_id]},
    )
    await test_db_session.commit()


async def _booking_guide_event_id(test_db_session) -> int:
    row = (
        await test_db_session.execute(
            text("SELECT id FROM auto_notification_events WHERE event_code = 'booking_guide'")
        )
    ).one()
    return int(row[0])


async def _bind_booking_guide_services(
    test_db_session,
    *,
    engagement_id: int,
    service_keys: str = DEFAULT_BOOKING_GUIDE_KEYS,
) -> None:
    evt_id = await _booking_guide_event_id(test_db_session)
    services_json = json.dumps(
        [{"service_key": key.strip(), "external_link": None} for key in service_keys.split(",") if key.strip()]
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_notifications (engagement_id, notification_event_id, notification_services) "
            "VALUES (:eid, :evt, CAST(:services AS jsonb)) "
            "ON CONFLICT (engagement_id, notification_event_id) DO UPDATE "
            "SET notification_services = EXCLUDED.notification_services"
        ),
        {"eid": engagement_id, "evt": evt_id, "services": services_json},
    )


async def _insert_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    status: str,
    start_date: str = "2026-06-01",
    end_date: str = "2026-06-30",
    slot_detail_id: int | None = None,
    bind_services: bool = True,
    service_keys: str = DEFAULT_BOOKING_GUIDE_KEYS,
) -> None:
    slot_value = "NULL" if slot_detail_id is None else str(slot_detail_id)
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, organization_id, slot_detail_id) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', 'bio_ai', 1, 1, 'BLR', 20, "
            f"'{start_date}', '{end_date}', '{status}', NULL, {slot_value})"
        )
    )
    if bind_services:
        await _bind_booking_guide_services(
            test_db_session,
            engagement_id=engagement_id,
            service_keys=service_keys,
        )


async def _insert_slot_detail(
    test_db_session,
    *,
    slot_detail_id: int,
    slot_detail: dict,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_slot_info (slot_detail_id, slot_detail) "
            "VALUES (:sid, CAST(:detail AS jsonb))"
        ),
        {"sid": slot_detail_id, "detail": json.dumps(slot_detail)},
    )


async def _insert_assistant(
    test_db_session,
    *,
    employee_id: int,
    user_id: int,
    role: str,
    engagement_id: int,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status) "
            f"VALUES ({user_id}, 30, '{user_id}000000000', 'active') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO employee (employee_id, user_id, role, status) "
            f"VALUES ({employee_id}, {user_id}, '{role}', 'active') "
            "ON CONFLICT (employee_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO onboarding_assistant_assignment (employee_id, engagement_id) "
            f"VALUES ({employee_id}, {engagement_id})"
        )
    )


def _fake_httpx_client(webhook_calls: list[dict]):
    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"message": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, json=None):
            webhook_calls.append({"url": url, "json": json})
            return _FakeResponse()

    return _FakeClient


def _services():
    notifications_repository = NotificationsRepository()
    return (
        NotificationsService(notifications_repository),
        EngagementsRepository(),
    )


@pytest.mark.asyncio
async def test_booking_guide_reminders_dispatch_to_admin_and_onboarding_assistant(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    slot_detail = {
        "blood_collection": {
            collection_date: {"is_enable": True, "cabins": []},
        }
    }
    await _insert_slot_detail(test_db_session, slot_detail_id=9701, slot_detail=slot_detail)
    await _insert_engagement(
        test_db_session,
        engagement_id=9701,
        engagement_code="ENG9701",
        status="running",
        slot_detail_id=9701,
    )
    await _insert_assistant(
        test_db_session,
        employee_id=701,
        user_id=97011,
        role="admin",
        engagement_id=9701,
    )
    await _insert_assistant(
        test_db_session,
        employee_id=702,
        user_id=97012,
        role="onboarding_assistant",
        engagement_id=9701,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 2
    assert result["sent"] == 2
    assert result["skipped"] == 0
    assert result["failed"] == 0
    assert len(webhook_calls) == 4


@pytest.mark.asyncio
async def test_booking_guide_reminders_falls_back_to_start_date(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    as_of = date(2026, 6, 1)
    await _insert_engagement(
        test_db_session,
        engagement_id=9702,
        engagement_code="ENG9702",
        status="scheduled",
        start_date="2026-06-02",
        end_date="2026-06-02",
        slot_detail_id=None,
    )
    await _insert_assistant(
        test_db_session,
        employee_id=703,
        user_id=97021,
        role="onboarding_assistant",
        engagement_id=9702,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 1
    assert len(webhook_calls) == 2


@pytest.mark.asyncio
async def test_booking_guide_reminders_excludes_completed_engagements(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    as_of = date(2026, 6, 1)
    await _insert_engagement(
        test_db_session,
        engagement_id=9703,
        engagement_code="ENG9703",
        status="completed",
        start_date="2026-06-02",
        end_date="2026-06-02",
    )
    await _insert_assistant(
        test_db_session,
        employee_id=704,
        user_id=97031,
        role="admin",
        engagement_id=9703,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_booking_guide_reminders_skips_wrong_collection_date(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    as_of = date(2026, 6, 1)
    slot_detail = {
        "blood_collection": {
            "2026-06-05": {"is_enable": True, "cabins": []},
        }
    }
    await _insert_slot_detail(test_db_session, slot_detail_id=9704, slot_detail=slot_detail)
    await _insert_engagement(
        test_db_session,
        engagement_id=9704,
        engagement_code="ENG9704",
        status="running",
        start_date="2026-06-05",
        slot_detail_id=9704,
    )
    await _insert_assistant(
        test_db_session,
        employee_id=705,
        user_id=97041,
        role="admin",
        engagement_id=9704,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_booking_guide_reminders_dry_run_does_not_dispatch(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    as_of = date(2026, 6, 1)
    await _insert_engagement(
        test_db_session,
        engagement_id=9705,
        engagement_code="ENG9705",
        status="running",
        start_date="2026-06-02",
    )
    await _insert_assistant(
        test_db_session,
        employee_id=706,
        user_id=97051,
        role="admin",
        engagement_id=9705,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=True,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["dry_run"] is True
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_booking_guide_reminders_dedup_skips_already_sent(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    as_of = date(2026, 6, 1)
    await _insert_engagement(
        test_db_session,
        engagement_id=9706,
        engagement_code="ENG9706",
        status="running",
        start_date="2026-06-02",
    )
    await _insert_assistant(
        test_db_session,
        employee_id=707,
        user_id=97061,
        role="onboarding_assistant",
        engagement_id=9706,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    first_call_count = len(webhook_calls)
    assert first_call_count == 2

    await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert len(webhook_calls) == first_call_count


@pytest.mark.asyncio
async def test_booking_guide_reminders_does_not_notify_expert_only_assignees(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    as_of = date(2026, 6, 1)
    await _insert_engagement(
        test_db_session,
        engagement_id=9707,
        engagement_code="ENG9707",
        status="running",
        start_date="2026-06-02",
    )
    await _insert_assistant(
        test_db_session,
        employee_id=708,
        user_id=97071,
        role="expert",
        engagement_id=9707,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_booking_guide_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0
