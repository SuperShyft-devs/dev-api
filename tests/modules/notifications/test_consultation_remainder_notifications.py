"""Tests for consultation remainder notification dispatch."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.consultation_remainder_notifications import (
    dispatch_consultation_remainder_notifications,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.users.models import User

REMAINDER_WHATSAPP_KEY = "consultation-remainder-whatsapp"
REMAINDER_EMAIL_KEY = "consultation-remainder-email"
DEFAULT_REMAINDER_KEYS = f"{REMAINDER_WHATSAPP_KEY},{REMAINDER_EMAIL_KEY}"
_IST = ZoneInfo("Asia/Kolkata")


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
    for service_key, channel, webhook_path, require_session in (
        (REMAINDER_WHATSAPP_KEY, "whatsapp", "consultation-remainder-whatsapp", False),
        (REMAINDER_EMAIL_KEY, "email", "consultation-remainder-email", True),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO notification_services "
                "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, "
                "require_bio_ai_report_url, require_participant_detail, require_session_details) "
                "VALUES (:sk, :dn, :ch, :wp, true, false, false, false, :rsd) "
                "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_session_details = :rsd"
            ),
            {
                "sk": service_key,
                "dn": service_key,
                "ch": channel,
                "wp": webhook_path,
                "rsd": require_session,
            },
        )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai_with_consultation', 'BioAI with Consultation', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai_with_consultation'")
        )
    ).one()
    type_id = int(type_row[0])
    await test_db_session.execute(
        text(
            "INSERT INTO auto_notification_events (event_code, display_name, engagement_type_ids, description) "
            "VALUES ('consultation_remainder', 'Consultation Remainder', :type_ids, 'Test remainder event') "
            "ON CONFLICT (event_code) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"type_ids": [type_id]},
    )
    await test_db_session.commit()


async def _event_id(test_db_session, event_code: str) -> int:
    row = (
        await test_db_session.execute(
            text("SELECT id FROM auto_notification_events WHERE event_code = :code"),
            {"code": event_code},
        )
    ).one()
    return int(row[0])


async def _insert_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    status: str,
    service_keys: str | None = DEFAULT_REMAINDER_KEYS,
) -> None:
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai_with_consultation'")
        )
    ).one()
    type_id = int(type_row[0])
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, "
            "organization_id, consultations) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', {type_id}, 1, 1, 'BLR', 20, "
            f"'2026-06-01', '2026-06-30', '{status}', NULL, '{json.dumps({'doctor': True})}')"
        )
    )
    if service_keys:
        evt_id = await _event_id(test_db_session, "consultation_remainder")
        keys_array = "{" + ",".join(f'"{k.strip()}"' for k in service_keys.split(",")) + "}"
        await test_db_session.execute(
            text(
                "INSERT INTO engagement_notifications (engagement_id, notification_event_id, notification_services) "
                f"VALUES ({engagement_id}, {evt_id}, '{keys_array}')"
            )
        )


async def _insert_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    engagement_date: str = "2026-06-01",
) -> int:
    test_db_session.add(
        User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active")
    )
    await test_db_session.flush()
    row = (
        await test_db_session.execute(
            text(
                "INSERT INTO engagement_participants "
                "(engagement_id, user_id, engagement_date, slot_start_time, booked_by_user_id) "
                "VALUES (:eid, :uid, :ed, :slot, :uid) "
                "RETURNING engagement_participant_id"
            ),
            {
                "eid": engagement_id,
                "uid": user_id,
                "ed": date.fromisoformat(engagement_date),
                "slot": time.fromisoformat("08:30:00"),
            },
        )
    ).one()
    return int(row[0])


async def _insert_consultation_booking(
    test_db_session,
    *,
    consultation_id: int,
    participant_id: int,
    expert_type: str,
    consultation_date: str,
    want: bool = True,
    consultation_slot: str | None = None,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO consultation_bookings "
            "(consultation_id, engagement_participant_id, expert_type, want, consultation_date, consultation_slot) "
            "VALUES (:cid, :pid, :etype, :want, :cdate, :cslot)"
        ),
        {
            "cid": consultation_id,
            "pid": participant_id,
            "etype": expert_type,
            "want": want,
            "cdate": date.fromisoformat(consultation_date),
            "cslot": consultation_slot,
        },
    )
    await test_db_session.execute(
        text(
            "UPDATE engagement_participants "
            "SET consultation_booking_ids = array_append(COALESCE(consultation_booking_ids, ARRAY[]::integer[]), :cid) "
            "WHERE engagement_participant_id = :pid"
        ),
        {"cid": consultation_id, "pid": participant_id},
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
    return NotificationsService(NotificationsRepository()), EngagementsRepository()


@pytest.mark.asyncio
async def test_consultation_remainder_sends_for_today_booking(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9801,
        engagement_code="ENG9801",
        status="running",
    )
    as_of = date(2026, 6, 10)
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9801,
        user_id=98011,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980101,
        participant_id=participant_id,
        expert_type="doctor",
        consultation_date="2026-06-10",
        consultation_slot="10:00-10:30",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_remainder_notifications(
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
    email_call = next(
        call
        for call in webhook_calls
        if call["json"]["members"][0].get("session_details") is not None
    )
    assert email_call["json"]["members"][0]["session_details"] == {
        "want": True,
        "date": "2026-06-10",
        "slot": "10:00-10:30",
        "expert_type": "doctor",
    }


@pytest.mark.asyncio
async def test_consultation_remainder_skips_tomorrow_booking(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9802,
        engagement_code="ENG9802",
        status="running",
    )
    as_of = date(2026, 6, 10)
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9802,
        user_id=98021,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980201,
        participant_id=participant_id,
        expert_type="doctor",
        consultation_date="2026-06-11",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_remainder_notifications(
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
async def test_consultation_remainder_two_bookings_same_day_dispatches_per_booking(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9803,
        engagement_code="ENG9803",
        status="running",
    )
    as_of = date(2026, 6, 10)
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9803,
        user_id=98031,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980301,
        participant_id=participant_id,
        expert_type="doctor",
        consultation_date="2026-06-10",
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980302,
        participant_id=participant_id,
        expert_type="nutritionist",
        consultation_date="2026-06-10",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_remainder_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 2
    assert result["sent"] == 2
    assert len(webhook_calls) == 4
    expert_types = {
        call["json"]["members"][0]["session_details"]["expert_type"]
        for call in webhook_calls
        if call["json"]["members"][0].get("session_details")
    }
    assert expert_types == {"doctor", "nutritionist"}


@pytest.mark.asyncio
async def test_consultation_remainder_second_run_same_day_skips(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9804,
        engagement_code="ENG9804",
        status="running",
    )
    as_of = date(2026, 6, 10)
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9804,
        user_id=98041,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980401,
        participant_id=participant_id,
        expert_type="doctor",
        consultation_date="2026-06-10",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    first = await dispatch_consultation_remainder_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()
    assert first["sent"] == 1
    assert len(webhook_calls) == 2

    second = await dispatch_consultation_remainder_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()
    assert second["matched"] == 1
    assert second["skipped"] == 1
    assert len(webhook_calls) == 2


@pytest.mark.asyncio
async def test_consultation_remainder_sends_again_on_later_consultation_day(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9805,
        engagement_code="ENG9805",
        status="running",
    )
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9805,
        user_id=98051,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980501,
        participant_id=participant_id,
        expert_type="doctor",
        consultation_date="2026-06-10",
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=980502,
        participant_id=participant_id,
        expert_type="nutritionist",
        consultation_date="2026-06-17",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()

    first_day = date(2026, 6, 10)
    first = await dispatch_consultation_remainder_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=first_day,
        dry_run=False,
    )
    await test_db_session.commit()
    assert first["sent"] == 1
    assert len(webhook_calls) == 2

    # Mark first-day notifications as sent on that day so second-day dispatch is allowed.
    dispatched_at = datetime(2026, 6, 10, 9, 0, tzinfo=_IST).astimezone(timezone.utc)
    await test_db_session.execute(
        text(
            "UPDATE notifications SET status = 'sent', dispatched_at = :ts, completed_at = :ts "
            "WHERE engagement_id = 9805"
        ),
        {"ts": dispatched_at},
    )
    await test_db_session.commit()

    webhook_calls.clear()
    second_day = date(2026, 6, 17)
    second = await dispatch_consultation_remainder_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=second_day,
        dry_run=False,
    )
    await test_db_session.commit()
    assert second["sent"] == 1
    assert len(webhook_calls) == 2
