"""Tests for book-expert notification dispatch."""

from __future__ import annotations

import json
from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.book_expert_notifications import dispatch_book_expert_notifications
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.users.models import User

BOOK_WHATSAPP_KEY = "book-expert-whatsapp"
BOOK_EMAIL_KEY = "book-expert-email"
DEFAULT_BOOK_KEYS = f"{BOOK_WHATSAPP_KEY},{BOOK_EMAIL_KEY}"


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
        (BOOK_WHATSAPP_KEY, "whatsapp", "book-expert-whatsapp"),
        (BOOK_EMAIL_KEY, "email", "book-expert-email"),
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
            "VALUES ('book_expert', 'Book Expert', :type_ids, 'Test book expert event') "
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
    consultations: dict | None = None,
    service_keys: str | None = DEFAULT_BOOK_KEYS,
) -> None:
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai_with_consultation'")
        )
    ).one()
    type_id = int(type_row[0])
    consultations_json = "NULL" if consultations is None else f"'{json.dumps(consultations)}'"
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, "
            "organization_id, consultations) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', {type_id}, 1, 1, 'BLR', 20, "
            f"'2026-06-01', '2026-06-30', '{status}', NULL, {consultations_json})"
        )
    )
    if service_keys:
        evt_id = await _event_id(test_db_session, "book_expert")
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
    engagement_date: str,
    slot_start_time: str = "08:30:00",
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
                "slot": time.fromisoformat(slot_start_time),
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
    want: bool,
    consultation_date: str | None = None,
) -> None:
    date_val = "NULL" if consultation_date is None else f"'{consultation_date}'"
    await test_db_session.execute(
        text(
            "INSERT INTO consultation_bookings "
            "(consultation_id, engagement_participant_id, expert_type, want, consultation_date) "
            f"VALUES ({consultation_id}, {participant_id}, '{expert_type}', {str(want).lower()}, {date_val})"
        )
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
async def test_book_expert_sends_for_unbooked_participant(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9701,
        engagement_code="ENG9701",
        status="running",
        consultations={"doctor": True, "nutritionist": True},
    )
    camp_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9701,
        user_id=97011,
        engagement_date=camp_date,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_book_expert_notifications(
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
async def test_book_expert_skips_when_all_offered_types_booked(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9702,
        engagement_code="ENG9702",
        status="running",
        consultations={"doctor": True, "nutritionist": True},
    )
    camp_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9702,
        user_id=97021,
        engagement_date=camp_date,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=970201,
        participant_id=participant_id,
        expert_type="doctor",
        want=True,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=970202,
        participant_id=participant_id,
        expert_type="nutritionist",
        want=True,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_book_expert_notifications(
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
async def test_book_expert_sends_when_one_offered_type_unbooked(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9703,
        engagement_code="ENG9703",
        status="running",
        consultations={"doctor": True, "nutritionist": True},
    )
    camp_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    participant_id = await _insert_participant(
        test_db_session,
        engagement_id=9703,
        user_id=97031,
        engagement_date=camp_date,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=970301,
        participant_id=participant_id,
        expert_type="doctor",
        want=True,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_book_expert_notifications(
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
async def test_book_expert_skips_engagement_without_consultations(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9704,
        engagement_code="ENG9704",
        status="running",
        consultations=None,
    )
    camp_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9704,
        user_id=97041,
        engagement_date=camp_date,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_book_expert_notifications(
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
async def test_book_expert_excludes_completed_engagements(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9705,
        engagement_code="ENG9705",
        status="completed",
        consultations={"doctor": True},
    )
    camp_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9705,
        user_id=97051,
        engagement_date=camp_date,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_book_expert_notifications(
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
async def test_book_expert_skips_already_sent(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9706,
        engagement_code="ENG9706",
        status="running",
        consultations={"doctor": True},
    )
    camp_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9706,
        user_id=97061,
        engagement_date=camp_date,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    first = await dispatch_book_expert_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()
    assert first["sent"] == 1
    assert len(webhook_calls) == 2

    second = await dispatch_book_expert_notifications(
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
