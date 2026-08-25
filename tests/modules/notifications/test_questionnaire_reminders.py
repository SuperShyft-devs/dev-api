"""Tests for questionnaire reminder dispatch."""

from __future__ import annotations

import json
from datetime import date, time

import pytest
from sqlalchemy import text

from modules.engagements.repository import EngagementsRepository
from modules.notifications.questionnaire_reminders import dispatch_questionnaire_reminders
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.users.models import User

QR_SERVICE_KEY = "qr-reminder-whatsapp"
ENGAGEMENT_DATE = "2026-06-10"


async def _seed_dependencies(test_db_session) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'METSIGHTS_BASIC', 'Metsights Basic', '1', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, "
            "require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'whatsapp', 'qr-reminder', true, false, false, false) "
            "ON CONFLICT (service_key) DO UPDATE SET is_active = true"
        ),
        {"sk": QR_SERVICE_KEY, "dn": QR_SERVICE_KEY},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai', 'BioAI', true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        )
    )
    type_id = int(
        (
            await test_db_session.execute(
                text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
            )
        ).scalar_one()
    )
    for event_code, display_name in (
        ("questionnaire_reminder_before", "Questionnaire Reminder Before"),
        ("questionnaire_reminder_after", "Questionnaire Reminder After"),
    ):
        await test_db_session.execute(
            text(
                "INSERT INTO auto_notification_events (event_code, display_name, engagement_type_ids, description) "
                "VALUES (:code, :dn, :type_ids, 'Test questionnaire reminder event') "
                "ON CONFLICT (event_code) DO UPDATE SET display_name = EXCLUDED.display_name"
            ),
            {"code": event_code, "dn": display_name, "type_ids": [type_id]},
        )
    await test_db_session.commit()


async def _event_id(test_db_session, event_code: str) -> int:
    return int(
        (
            await test_db_session.execute(
                text("SELECT id FROM auto_notification_events WHERE event_code = :code"),
                {"code": event_code},
            )
        ).scalar_one()
    )


async def _bind_questionnaire_services(
    test_db_session,
    *,
    engagement_id: int,
    service_key: str = QR_SERVICE_KEY,
) -> None:
    services_json = json.dumps([{"service_key": service_key, "external_link": None}])
    for event_code in ("questionnaire_reminder_before", "questionnaire_reminder_after"):
        evt_id = await _event_id(test_db_session, event_code)
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
    status: str = "running",
) -> None:
    type_id = int(
        (
            await test_db_session.execute(
                text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
            )
        ).scalar_one()
    )
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, engagement_type, assessment_package_id, "
            "diagnostic_package_id, city, slot_duration, start_date, end_date, status, organization_id) "
            f"VALUES ({engagement_id}, 'Camp {engagement_id}', '{engagement_code}', {type_id}, 1, 1, 'BLR', 20, "
            f"'2026-06-01', '2026-06-30', '{status}', NULL)"
        )
    )
    await _bind_questionnaire_services(test_db_session, engagement_id=engagement_id)


async def _insert_participant(
    test_db_session,
    *,
    engagement_id: int,
    user_id: int,
    engagement_date: str = ENGAGEMENT_DATE,
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
            "slot": time.fromisoformat("10:00:00"),
        },
    )


async def _insert_assessment_instance(
    test_db_session,
    *,
    assessment_instance_id: int,
    user_id: int,
    engagement_id: int,
    metsights_record_id: str = "QR-TEST-RECORD",
) -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_instances "
            "(assessment_instance_id, user_id, package_id, engagement_id, status, metsights_record_id) "
            "VALUES (:aid, :uid, 1, :eid, 'active', :record_id)"
        ),
        {
            "aid": assessment_instance_id,
            "uid": user_id,
            "eid": engagement_id,
            "record_id": metsights_record_id,
        },
    )


class FakeMetsightsService:
    def __init__(self, *, resource_complete: dict[str, bool]):
        self._resource_complete = resource_complete

    async def get_record_subresource_or_none(self, *, record_id: str, resource: str):
        if resource not in self._resource_complete:
            return None
        return {"is_complete": self._resource_complete[resource]}


class FakeCategoriesService:
    def __init__(self, *, categories: list[dict]):
        self._categories = categories

    async def list_category_completion_for_assessment_instance(
        self,
        db,
        *,
        user_id: int,
        assessment_instance_id: int,
    ) -> list[dict]:
        return self._categories


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


def _incomplete_metsights() -> FakeMetsightsService:
    return FakeMetsightsService(
        resource_complete={
            "diet-lifestyle-parameters": False,
            "physical-measurement": True,
        }
    )


def _required_metsights_complete() -> FakeMetsightsService:
    return FakeMetsightsService(
        resource_complete={
            "diet-lifestyle-parameters": True,
            "physical-measurement": True,
        }
    )


def _incomplete_internal_categories() -> FakeCategoriesService:
    return FakeCategoriesService(
        categories=[
            {
                "category_key": "diet-lifestyle-parameters",
                "status": "incomplete",
            }
        ]
    )


def _only_health_vitals_incomplete() -> FakeCategoriesService:
    return FakeCategoriesService(
        categories=[
            {
                "category_key": "health_vitals",
                "status": "incomplete",
            }
        ]
    )


@pytest.mark.asyncio
async def test_questionnaire_reminders_same_service_sends_before_and_after(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9901,
        engagement_code="ENG9901",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9901,
        user_id=99011,
    )
    await _insert_assessment_instance(
        test_db_session,
        assessment_instance_id=99011,
        user_id=99011,
        engagement_id=9901,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    metsights_service = _incomplete_metsights()
    categories_service = _incomplete_internal_categories()

    before_result = await dispatch_questionnaire_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        metsights_service=metsights_service,
        categories_service=categories_service,
        as_of=date(2026, 6, 9),
        dry_run=False,
    )
    await test_db_session.commit()

    assert before_result["sent"] == 1
    assert len(webhook_calls) == 1

    after_result = await dispatch_questionnaire_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        metsights_service=metsights_service,
        categories_service=categories_service,
        as_of=date(2026, 6, 11),
        dry_run=False,
    )
    await test_db_session.commit()

    assert after_result["sent"] == 1
    assert len(webhook_calls) == 2


@pytest.mark.asyncio
async def test_questionnaire_reminders_same_day_rerun_is_skipped(test_db_session, monkeypatch):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9902,
        engagement_code="ENG9902",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9902,
        user_id=99021,
    )
    await _insert_assessment_instance(
        test_db_session,
        assessment_instance_id=99021,
        user_id=99021,
        engagement_id=9902,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    kwargs = {
        "notifications_service": notifications_service,
        "engagements_repository": engagements_repository,
        "metsights_service": _incomplete_metsights(),
        "categories_service": _incomplete_internal_categories(),
        "as_of": date(2026, 6, 9),
        "dry_run": False,
    }

    first = await dispatch_questionnaire_reminders(test_db_session, **kwargs)
    await test_db_session.commit()
    assert first["sent"] == 1
    assert len(webhook_calls) == 1

    second = await dispatch_questionnaire_reminders(test_db_session, **kwargs)
    await test_db_session.commit()
    assert second["sent"] == 0
    assert second["skipped"] == 1
    assert len(webhook_calls) == 1
    assert any("already sent" in d["reason"] for d in second["details"])


@pytest.mark.asyncio
async def test_questionnaire_reminders_skip_when_only_metsights_vitals_would_be_pending(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9903,
        engagement_code="ENG9903",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9903,
        user_id=99031,
    )
    await _insert_assessment_instance(
        test_db_session,
        assessment_instance_id=99031,
        user_id=99031,
        engagement_id=9903,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_questionnaire_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        metsights_service=_required_metsights_complete(),
        categories_service=_incomplete_internal_categories(),
        as_of=date(2026, 6, 9),
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["sent"] == 0
    assert result["skipped"] == 1
    assert len(webhook_calls) == 0
    assert any(
        d["reason"] == "questionnaire complete on Metsights" for d in result["details"]
    )


@pytest.mark.asyncio
async def test_questionnaire_reminders_skip_when_only_health_vitals_pending(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9904,
        engagement_code="ENG9904",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9904,
        user_id=99041,
    )
    await _insert_assessment_instance(
        test_db_session,
        assessment_instance_id=99041,
        user_id=99041,
        engagement_id=9904,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_questionnaire_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        metsights_service=_incomplete_metsights(),
        categories_service=_only_health_vitals_incomplete(),
        as_of=date(2026, 6, 9),
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["sent"] == 0
    assert result["skipped"] == 1
    assert len(webhook_calls) == 0
    assert any(
        d["reason"] == "questionnaire complete in internal DB" for d in result["details"]
    )


@pytest.mark.asyncio
async def test_questionnaire_reminders_send_when_required_category_incomplete(
    test_db_session, monkeypatch
):
    await _seed_dependencies(test_db_session)
    await _insert_engagement(
        test_db_session,
        engagement_id=9905,
        engagement_code="ENG9905",
    )
    await _insert_participant(
        test_db_session,
        engagement_id=9905,
        user_id=99051,
    )
    await _insert_assessment_instance(
        test_db_session,
        assessment_instance_id=99051,
        user_id=99051,
        engagement_id=9905,
    )
    await test_db_session.commit()

    webhook_calls: list[dict] = []
    monkeypatch.setattr(
        "modules.notifications.service.httpx.AsyncClient",
        _fake_httpx_client(webhook_calls),
    )

    notifications_service, engagements_repository = _services()
    result = await dispatch_questionnaire_reminders(
        test_db_session,
        notifications_service=notifications_service,
        engagements_repository=engagements_repository,
        metsights_service=_incomplete_metsights(),
        categories_service=_incomplete_internal_categories(),
        as_of=date(2026, 6, 9),
        dry_run=False,
    )
    await test_db_session.commit()

    assert result["sent"] == 1
    assert len(webhook_calls) == 1
    assert any(d["action"] == "sent" for d in result["details"])
