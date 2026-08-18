"""Tests for consultation readiness notification dispatch."""

from __future__ import annotations

import json
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
    for code, display_name in (
        ("bio_ai_with_consultation", "BioAI with Consultation"),
        ("blood_test_with_consultation", "Blood Test with Consultation"),
        ("bio_ai", "BioAI"),
        ("consultation", "Consultation"),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO engagement_types (code, display_name, is_active) "
                "VALUES (:code, :display_name, true) "
                "ON CONFLICT (code) DO UPDATE SET display_name = EXCLUDED.display_name, is_active = true"
            ),
            {"code": code, "display_name": display_name},
        )
    type_ids = [
        int(row[0])
        for row in (
            await test_db_session.execute(
                text(
                    "SELECT id FROM engagement_types "
                    "WHERE code IN ('bio_ai_with_consultation', 'blood_test_with_consultation', 'consultation')"
                )
            )
        ).all()
    ]
    await test_db_session.execute(
        text(
            "INSERT INTO auto_notification_events (event_code, display_name, engagement_type_ids, description) "
            "VALUES ('consultation_ready', 'Consultation Notification', :type_ids, 'Test consultation-ready event') "
            "ON CONFLICT (event_code) DO UPDATE SET display_name = EXCLUDED.display_name"
        ),
        {"type_ids": type_ids},
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


async def _type_id(test_db_session, type_code: str) -> int:
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": type_code},
        )
    ).one()
    return int(row[0])


async def _insert_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    status: str,
    engagement_type: str = "bio_ai_with_consultation",
    blood_collection_type: str = "home_collection",
    consultations: dict | None = None,
    service_keys: str | None = DEFAULT_CONSULT_KEYS,
) -> None:
    type_id = await _type_id(test_db_session, engagement_type)
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, "
            "organization_id, blood_collection_type, consultations) "
            "VALUES (:eid, :name, :code, :type_id, 1, 1, 'BLR', 20, "
            "'2026-06-01', '2026-06-30', :status, NULL, :collection_type, CAST(:consultations AS jsonb))"
        ),
        {
            "eid": engagement_id,
            "name": f"Camp {engagement_id}",
            "code": engagement_code,
            "type_id": type_id,
            "status": status,
            "collection_type": blood_collection_type,
            "consultations": json.dumps(consultations) if consultations is not None else None,
        },
    )
    if service_keys:
        evt_id = await _event_id(test_db_session, "consultation_ready")
        services_json = json.dumps(
            [{"service_key": k.strip(), "external_link": None} for k in service_keys.split(",") if k.strip()]
        )
        await test_db_session.execute(
            text(
                "INSERT INTO engagement_notifications (engagement_id, notification_event_id, notification_services) "
                "VALUES (:engagement_id, :event_id, CAST(:services AS jsonb))"
            ),
            {"engagement_id": engagement_id, "event_id": evt_id, "services": services_json},
        )


async def _insert_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    engagement_date: str = "2026-06-02",
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
    want: bool,
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO consultation_bookings "
            "(consultation_id, engagement_participant_id, expert_type, want) "
            "VALUES (:cid, :pid, :etype, :want)"
        ),
        {"cid": consultation_id, "pid": participant_id, "etype": expert_type, "want": want},
    )
    await test_db_session.execute(
        text(
            "UPDATE engagement_participants "
            "SET consultation_booking_ids = array_append(COALESCE(consultation_booking_ids, ARRAY[]::integer[]), :cid) "
            "WHERE engagement_participant_id = :pid"
        ),
        {"cid": consultation_id, "pid": participant_id},
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
async def test_consultation_notifications_bioai_ready_with_no_bookings(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9701,
        engagement_code="ENG9701",
        status="running",
        engagement_type="bio_ai_with_consultation",
        consultations={"doctor": True, "nutritionist": True},
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
async def test_consultation_notifications_blood_ready_for_camp_collection(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9702,
        engagement_code="ENG9702",
        status="scheduled",
        engagement_type="blood_test_with_consultation",
        blood_collection_type="camp_collection",
        consultations={"doctor": True},
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
async def test_consultation_notifications_skips_when_all_offered_types_are_booked(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9703,
        engagement_code="ENG9703",
        status="running",
        consultations={"doctor": True, "nutritionist": True},
    )
    participant_id = await _insert_participant(test_db_session, engagement_id=9703, user_id=97031)
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=97031,
        participant_id=participant_id,
        expert_type="doctor",
        want=True,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=97032,
        participant_id=participant_id,
        expert_type="nutritionist",
        want=True,
    )
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
async def test_consultation_notifications_sends_when_any_offered_type_has_want_false(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9704,
        engagement_code="ENG9704",
        status="running",
        consultations={"doctor": True, "nutritionist": True},
    )
    participant_id = await _insert_participant(test_db_session, engagement_id=9704, user_id=97041)
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=97041,
        participant_id=participant_id,
        expert_type="doctor",
        want=True,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=97042,
        participant_id=participant_id,
        expert_type="nutritionist",
        want=False,
    )
    await _insert_bioai_ihr(
        test_db_session, report_id=9704, user_id=97041, engagement_id=9704, ready=True
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
async def test_consultation_notifications_ignores_non_offered_false_booking(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9705,
        engagement_code="ENG9705",
        status="running",
        consultations={"doctor": True},
    )
    participant_id = await _insert_participant(test_db_session, engagement_id=9705, user_id=97051)
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=97051,
        participant_id=participant_id,
        expert_type="doctor",
        want=True,
    )
    await _insert_consultation_booking(
        test_db_session,
        consultation_id=97052,
        participant_id=participant_id,
        expert_type="nutritionist",
        want=False,
    )
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
async def test_consultation_notifications_skips_when_report_not_ready(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9706,
        engagement_code="ENG9706",
        status="running",
        consultations={"doctor": True},
    )
    await _insert_participant(test_db_session, engagement_id=9706, user_id=97061)
    await _insert_bioai_ihr(
        test_db_session, report_id=9706, user_id=97061, engagement_id=9706, ready=False
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
async def test_consultation_notifications_dry_run_does_not_dispatch(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9707,
        engagement_code="ENG9707",
        status="running",
        consultations={"doctor": True},
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
        engagement_id=9708,
        engagement_code="ENG9708",
        status="running",
        consultations={"doctor": True},
        service_keys=CONSULT_WHATSAPP_KEY,
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
    first = await dispatch_consultation_notifications(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        dry_run=False,
    )
    await test_db_session.commit()
    assert first["sent"] == 1
    assert len(webhook_calls) == 1

    await test_db_session.execute(
        text("UPDATE notifications SET status = 'sent' WHERE engagement_id = 9708")
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
async def test_consultation_notifications_skips_wrong_engagement_type(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9709,
        engagement_code="ENG9709",
        status="running",
        engagement_type="bio_ai",
        consultations={"doctor": True},
    )
    await _insert_participant(test_db_session, engagement_id=9709, user_id=97091)
    await _insert_bioai_ihr(
        test_db_session, report_id=9709, user_id=97091, engagement_id=9709, ready=True
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
