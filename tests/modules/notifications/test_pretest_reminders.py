"""Tests for pretest blood-collection reminder dispatch."""

from __future__ import annotations

import json
from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.pretest_reminders import (
    PRETEST_EMAIL_KEY,
    PRETEST_WHATSAPP_KEY,
    dispatch_pretest_reminders,
)
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.users.models import User


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
        (PRETEST_WHATSAPP_KEY, "whatsapp", "pretest-whatsapp"),
        (PRETEST_EMAIL_KEY, "email", "pretest-email"),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO notification_services "
                "(service_key, display_name, channel, webhook_path, is_active, require_record_id, "
                "require_participant_detail) "
                "VALUES (:sk, :dn, :ch, :wp, true, false, false) "
                "ON CONFLICT (service_key) DO UPDATE SET is_active = true, require_record_id = false"
            ),
            {"sk": service_key, "dn": service_key, "ch": channel, "wp": webhook_path},
        )
    await test_db_session.commit()


async def _insert_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    status: str,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, participant_count, "
            "organization_id, notification_service_key) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', 'bio_ai', 1, 1, 'BLR', 20, "
            f"'2026-06-01', '2026-06-30', '{status}', 0, NULL, 'pretest-whatsapp')"
        )
    )


async def _insert_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    engagement_date: str,
    slot_start_time: str,
) -> None:
    test_db_session.add(
        User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active")
    )
    await test_db_session.flush()
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_participants (engagement_id, user_id, engagement_date, slot_start_time) "
            "VALUES (:eid, :uid, :ed, :slot)"
        ),
        {
            "eid": engagement_id,
            "uid": user_id,
            "ed": date.fromisoformat(engagement_date),
            "slot": time.fromisoformat(slot_start_time),
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
        notifications_repository,
        EngagementsRepository(),
    )


@pytest.mark.asyncio
async def test_pretest_reminders_early_window(test_db_session, monkeypatch):
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

    notifications_service, notifications_repository, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=notifications_repository,
        engagements_repository=engagements_repository,
        window="early",
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 2
    assert result["whatsapp_sent"] == 2
    assert result["email_sent"] == 2
    assert result["failed"] == 0
    assert len(webhook_calls) == 4

    user_ids_per_call = [call["json"]["members"][0] for call in webhook_calls]
    assert all("phone" in m or "email" in m for m in user_ids_per_call)

    notification_count = (
        await test_db_session.execute(text("SELECT COUNT(*) FROM notifications"))
    ).scalar_one()
    assert notification_count == 4


@pytest.mark.asyncio
async def test_pretest_reminders_late_window(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session, engagement_id=9602, engagement_code="ENG9602", status="running"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9602,
        user_id=96021,
        engagement_date=collection_date,
        slot_start_time="09:00:00",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9602,
        user_id=96022,
        engagement_date=collection_date,
        slot_start_time="10:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, notifications_repository, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=notifications_repository,
        engagements_repository=engagements_repository,
        window="late",
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["whatsapp_sent"] == 1
    assert result["email_sent"] == 1
    assert len(webhook_calls) == 2


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

    notifications_service, notifications_repository, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=notifications_repository,
        engagements_repository=engagements_repository,
        window="early",
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0


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

    notifications_service, notifications_repository, engagements_repository = _services()
    result = await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=notifications_repository,
        engagements_repository=engagements_repository,
        window="early",
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
        test_db_session, engagement_id=9605, engagement_code="ENG9605", status="running"
    )
    collection_date = "2026-06-02"
    as_of = date(2026, 6, 1)
    await _insert_participant(
        test_db_session,
        engagement_id=9605,
        user_id=96051,
        engagement_date=collection_date,
        slot_start_time="07:00:00",
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, notifications_repository, engagements_repository = _services()
    await dispatch_pretest_reminders(
        test_db_session,
        notifications_service=notifications_service,
        notifications_repository=notifications_repository,
        engagements_repository=engagements_repository,
        window="early",
        as_of=as_of,
        dry_run=False,
    )
    await test_db_session.commit()

    rows = (
        await test_db_session.execute(
            text(
                'SELECT service_key, "user" AS user_payload, engagement_id '
                "FROM notifications ORDER BY notification_id"
            )
        )
    ).all()
    assert len(rows) == 2
    for row in rows:
        user_payload = row.user_payload
        if isinstance(user_payload, str):
            user_payload = json.loads(user_payload)
        assert len(user_payload["user_ids"]) == 1
        assert user_payload["user_ids"][0] == 96051
        assert row.engagement_id == 9605
    service_keys = {row.service_key for row in rows}
    assert service_keys == {PRETEST_WHATSAPP_KEY, PRETEST_EMAIL_KEY}
