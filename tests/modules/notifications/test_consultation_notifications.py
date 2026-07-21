"""Tests for consultation readiness notification dispatch."""

from __future__ import annotations

from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.consultation_notifications import dispatch_consultation_notifications
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport
from modules.users.models import User

CONSULT_WHATSAPP_KEY = "consult-whatsapp"
CONSULT_EMAIL_KEY = "consult-email"
DEFAULT_CONSULT_KEYS = f"{CONSULT_WHATSAPP_KEY},{CONSULT_EMAIL_KEY}"


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
        (CONSULT_WHATSAPP_KEY, "whatsapp", "consult-whatsapp"),
        (CONSULT_EMAIL_KEY, "email", "consult-email"),
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
    await test_db_session.commit()


async def _insert_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    status: str,
    engagement_type: str = "bio_ai_with_consultation",
    blood_collection_type: str = "home_collection",
    notify_users_for_consultation: str | None = DEFAULT_CONSULT_KEYS,
) -> None:
    notify_value = (
        "NULL"
        if notify_users_for_consultation is None
        else f"'{notify_users_for_consultation}'"
    )
    collection_value = (
        "NULL" if blood_collection_type is None else f"'{blood_collection_type}'"
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, "
            "organization_id, blood_collection_type, notify_users_for_consultation) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', "
            f"'{engagement_type}', 1, 1, 'BLR', 20, "
            f"'2026-06-01', '2026-06-30', '{status}', NULL, {collection_value}, {notify_value})"
        )
    )


async def _insert_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    engagement_date: str = "2026-06-02",
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
            "slot": time.fromisoformat("08:30:00"),
        },
    )


async def _insert_bioai_ihr(
    test_db_session,
    *,
    report_id: int,
    user_id: int,
    engagement_id: int,
    ready: bool = True,
) -> None:
    test_db_session.add(
        IndividualHealthReport(
            report_id=report_id,
            user_id=user_id,
            engagement_id=engagement_id,
            reports={"summary": "ok"} if ready else None,
            report_url="https://example.com/bioai.pdf" if ready else None,
        )
    )


async def _insert_blood_ihr(
    test_db_session,
    *,
    report_id: int,
    user_id: int,
    engagement_id: int,
    ready: bool = True,
) -> None:
    test_db_session.add(
        IndividualHealthReport(
            report_id=report_id,
            user_id=user_id,
            engagement_id=engagement_id,
            blood_report_raw={"hemoglobin": 14} if ready else None,
            diagnostic_report_url="https://example.com/blood.pdf" if ready else None,
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
    return NotificationsService(NotificationsRepository()), EngagementsRepository()


@pytest.mark.asyncio
async def test_consultation_notifications_bioai_ready(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9701,
        engagement_code="ENG9701",
        status="running",
        engagement_type="bio_ai_with_consultation",
    )
    await _insert_participant(test_db_session, engagement_id=9701, user_id=97011)
    await _insert_bioai_ihr(
        test_db_session, report_id=9701, user_id=97011, engagement_id=9701, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 1
    assert result["skipped"] == 0
    assert result["failed"] == 0
    assert len(webhook_calls) == 2


@pytest.mark.asyncio
async def test_consultation_notifications_blood_ready(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9702,
        engagement_code="ENG9702",
        status="scheduled",
        engagement_type="blood_test_with_consultation",
    )
    await _insert_participant(test_db_session, engagement_id=9702, user_id=97021)
    await _insert_blood_ihr(
        test_db_session, report_id=9702, user_id=97021, engagement_id=9702, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 1
    assert len(webhook_calls) == 2


@pytest.mark.asyncio
async def test_consultation_notifications_skips_camp_collection(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9703,
        engagement_code="ENG9703",
        status="running",
        blood_collection_type="camp_collection",
    )
    await _insert_participant(test_db_session, engagement_id=9703, user_id=97031)
    await _insert_bioai_ihr(
        test_db_session, report_id=9703, user_id=97031, engagement_id=9703, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_consultation_notifications_skips_when_report_not_ready(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9704,
        engagement_code="ENG9704",
        status="running",
    )
    await _insert_participant(test_db_session, engagement_id=9704, user_id=97041)
    await _insert_bioai_ihr(
        test_db_session, report_id=9704, user_id=97041, engagement_id=9704, ready=False
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_consultation_notifications_skips_completed_engagement(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9705,
        engagement_code="ENG9705",
        status="completed",
    )
    await _insert_participant(test_db_session, engagement_id=9705, user_id=97051)
    await _insert_bioai_ihr(
        test_db_session, report_id=9705, user_id=97051, engagement_id=9705, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_consultation_notifications_dry_run_does_not_dispatch(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9706,
        engagement_code="ENG9706",
        status="running",
    )
    await _insert_participant(test_db_session, engagement_id=9706, user_id=97061)
    await _insert_bioai_ihr(
        test_db_session, report_id=9706, user_id=97061, engagement_id=9706, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=True,
    )
    await test_db_session.commit()

    assert result["matched"] == 1
    assert result["sent"] == 0
    assert result["dry_run"] is True
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_consultation_notifications_skips_already_sent(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9707,
        engagement_code="ENG9707",
        status="running",
        notify_users_for_consultation=CONSULT_WHATSAPP_KEY,
    )
    await _insert_participant(test_db_session, engagement_id=9707, user_id=97071)
    await _insert_bioai_ihr(
        test_db_session, report_id=9707, user_id=97071, engagement_id=9707, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    first = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()
    assert first["sent"] == 1
    assert len(webhook_calls) == 1

    # Mark as sent so dedup skips on second run
    await test_db_session.execute(
        text("UPDATE notifications SET status = 'sent' WHERE engagement_id = 9707")
    )
    await test_db_session.commit()

    webhook_calls.clear()
    second = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert second["matched"] == 1
    assert second["sent"] == 0
    assert second["skipped"] == 1
    assert len(webhook_calls) == 0


@pytest.mark.asyncio
async def test_consultation_notifications_skips_wrong_engagement_type(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9708,
        engagement_code="ENG9708",
        status="running",
        engagement_type="bio_ai",
    )
    await _insert_participant(test_db_session, engagement_id=9708, user_id=97081)
    await _insert_bioai_ihr(
        test_db_session, report_id=9708, user_id=97081, engagement_id=9708, ready=True
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["matched"] == 0
    assert len(webhook_calls) == 0
