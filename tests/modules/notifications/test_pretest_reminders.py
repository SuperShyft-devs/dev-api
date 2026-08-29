"""Tests for pretest blood-collection reminder dispatch."""

from __future__ import annotations

import json
from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.pretest_reminders import (
    dispatch_pretest_reminders,
    format_blood_collection_slot,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.users.models import User

PRETEST_WHATSAPP_KEY = "pretest-whatsapp-1-1-v1"
PRETEST_EMAIL_KEY = "pretest-email-1-1-v1"
DEFAULT_PRETEST_KEYS = f"{PRETEST_WHATSAPP_KEY},{PRETEST_EMAIL_KEY}"


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
        (PRETEST_WHATSAPP_KEY, "whatsapp", "pretest-whatsapp-1+1-v1"),
        (PRETEST_EMAIL_KEY, "email", "pretest-email-1+1-v1"),
        ("custom-pretest-only", "email", "custom-pretest-only"),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO notification_services "
                "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, "
                "require_bio_ai_report_url, require_participant_detail, require_session_details) "
                "VALUES (:sk, :dn, :ch, :wp, true, false, false, false, true) "
                "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_session_details = true"
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
            "VALUES ('pretest_guidelines', 'Pretest Guidelines', :type_ids, 'Test pretest event') "
            "ON CONFLICT (event_code) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"type_ids": [type_id]},
    )
    await test_db_session.commit()


async def _pretest_event_id(test_db_session) -> int:
    row = (
        await test_db_session.execute(
            text("SELECT id FROM auto_notification_events WHERE event_code = 'pretest_guidelines'")
        )
    ).one()
    return int(row[0])


async def _bind_pretest_services(
    test_db_session,
    *,
    engagement_id: int,
    service_keys: str = DEFAULT_PRETEST_KEYS,
) -> None:
    evt_id = await _pretest_event_id(test_db_session)
    services_json = json.dumps(
        [
            {"service_key": key.strip(), "external_link": None}
            for key in service_keys.split(",")
            if key.strip()
        ]
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
    bind_services: bool = True,
    service_keys: str = DEFAULT_PRETEST_KEYS,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, organization_id) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', "
            "(SELECT id FROM engagement_types WHERE code = 'bio_ai'), 1, 1, 'BLR', 20, "
            f"'2026-06-01', '2026-06-30', '{status}', NULL)"
        )
    )
    if bind_services:
        await _bind_pretest_services(
            test_db_session, engagement_id=engagement_id, service_keys=service_keys
        )


async def _insert_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    engagement_date: str,
    slot_start_time: str | None,
) -> None:
    test_db_session.add(
        User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active")
    )
    await test_db_session.flush()
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants "
            "(engagement_id, user_id, engagement_date, slot_start_time, booked_by_user_id) "
            "VALUES (:eid, :uid, :ed, :slot, :uid)"
        ),
        {
            "eid": engagement_id,
            "uid": user_id,
            "ed": date.fromisoformat(engagement_date),
            "slot": time.fromisoformat(slot_start_time) if slot_start_time else None,
        },
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


def test_format_blood_collection_slot():
    assert format_blood_collection_slot(time(8, 30)) == "8:30 AM"
    assert format_blood_collection_slot(time(14, 0)) == "2:00 PM"
    assert format_blood_collection_slot(None) == ""


@pytest.mark.asyncio
async def test_pretest_reminders_all_participants(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9601, engagement_code="ENG9601", status="running"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9601,
        user_id=96011,
        engagement_date=collection_date,
        slot_start_time="08:30:00",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9601,
        user_id=96012,
        engagement_date=collection_date,
        slot_start_time="09:00:00",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9601,
        user_id=96013,
        engagement_date=collection_date,
        slot_start_time="10:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 3
    assert result["sent"] == 3
    assert result["skipped"] == 0
    assert result["failed"] == 0
    assert len(webhook_calls) == 6

    for call in webhook_calls:
        member = call["json"]["members"][0]
        assert "session_details" in member
        assert member["session_details"]["date"] == collection_date
        assert member["session_details"]["slot"]
        assert member["session_details"]["expert_type"] == "blood_collection"

    notification_count = (
        await test_db_session.execute(text("SELECT COUNT(*) FROM notifications"))
    ).scalar_one()
    assert notification_count == 6


@pytest.mark.asyncio
async def test_pretest_reminders_excludes_completed_engagements(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9603, engagement_code="ENG9603", status="completed"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9603,
        user_id=96031,
        engagement_date=collection_date,
        slot_start_time="08:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
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
async def test_pretest_reminders_includes_scheduled_engagements(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9605, engagement_code="ENG9605", status="scheduled"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9605,
        user_id=96051,
        engagement_date=collection_date,
        slot_start_time="08:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 1
    assert result["skipped"] == 0
    assert result["failed"] == 0
    assert len(webhook_calls) == 2
    assert webhook_calls[0]["json"]["members"][0]["session_details"]["slot"] == "8:00 AM"


@pytest.mark.asyncio
async def test_pretest_reminders_dry_run_does_not_dispatch(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9604, engagement_code="ENG9604", status="running"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9604,
        user_id=96041,
        engagement_date=collection_date,
        slot_start_time="08:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
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

    notification_count = (
        await test_db_session.execute(text("SELECT COUNT(*) FROM notifications"))
    ).scalar_one()
    assert notification_count == 0


@pytest.mark.asyncio
async def test_pretest_reminders_one_user_id_per_dispatch(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9606, engagement_code="ENG9606", status="running"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9606,
        user_id=96061,
        engagement_date=collection_date,
        slot_start_time="07:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert len(webhook_calls) == 2
    for call in webhook_calls:
        assert call["json"]["members"][0]["session_details"]["slot"] == "7:00 AM"
        assert len(call["json"]["members"]) == 1


@pytest.mark.asyncio
async def test_pretest_reminders_skips_when_no_keys_configured(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9607,
        engagement_code="ENG9607",
        status="running",
        bind_services=False,
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9607,
        user_id=96071,
        engagement_date=collection_date,
        slot_start_time="08:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
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
async def test_pretest_reminders_single_custom_key(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9608,
        engagement_code="ENG9608",
        status="running",
        service_keys="custom-pretest-only",
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9608,
        user_id=96081,
        engagement_date=collection_date,
        slot_start_time="09:15:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 1
    assert len(webhook_calls) == 1
    assert webhook_calls[0]["json"]["members"][0]["session_details"]["slot"] == "9:15 AM"

    rows = (
        await test_db_session.execute(text("SELECT service_key FROM notifications"))
    ).all()
    assert len(rows) == 1
    assert rows[0].service_key == "custom-pretest-only"


@pytest.mark.asyncio
async def test_pretest_reminders_skips_missing_slot(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9609, engagement_code="ENG9609", status="running"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9609,
        user_id=96091,
        engagement_date=collection_date,
        slot_start_time=None,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 0
    assert result["skipped"] == 1
    assert len(webhook_calls) == 0
    assert result["details"][0]["reason"] == "missing slot_start_time for session_details"
