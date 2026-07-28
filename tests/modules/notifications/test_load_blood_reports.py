"""Tests for load_blood_reports cron job."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest
from sqlalchemy import select, text

from core.config import settings
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.models import AssessmentInstance
from modules.engagements.dependencies import get_engagements_service
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.notifications.load_blood_reports import load_blood_reports, _match_customer_by_name
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.models import IndividualHealthReport
from modules.users.models import User
from modules.users.repository import UsersRepository

_REPORT_URL = "https://example.com/blood-report.pdf"
_VERIFIED_AT = "2025-12-04 19:55:56"
_VERIFIED_AT_DT = datetime(2025, 12, 4, 19, 55, 56, tzinfo=timezone.utc)


def _report_payload(
    *,
    full_report: int = 1,
    verified_at: str = _VERIFIED_AT,
    cust_name: str = "John Doe",
    report_url: str = _REPORT_URL,
) -> dict:
    return {
        "data": [
            {
                "cust_name": cust_name,
                "report_url": report_url,
                "full_report": full_report,
                "verified_at": verified_at,
            }
        ]
    }


async def _seed_notification_service(test_db_session, *, service_key: str = "booking-alert-whatsapp") -> None:
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'email', 'test-webhook', true, false, false, false) "
            "ON CONFLICT (service_key) DO NOTHING"
        ),
        {"sk": service_key, "dn": service_key},
    )
    await test_db_session.commit()


async def _seed_running_participant(
    test_db_session,
    *,
    user_id: int = 88001,
    engagement_id: int = 88001,
    assessment_id: int = 88001,
    diagnostic_package_id: int = 17,
    booking_id: str = "BOOK-88001",
    metsights_record_id: str = "MS-BLOOD-CRON",
    existing_blood_parameters=None,
    existing_verified_at=None,
    existing_full_report=None,
    existing_diag_url=None,
    blood_report_notification: str | None = "booking-alert-whatsapp",
):
    await _seed_notification_service(test_db_session)

    test_db_session.add(
        User(
            user_id=user_id,
            first_name="John",
            last_name="Doe",
            phone=f"{user_id}000000",
            age=30,
            status="active",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Blood Cron Engagement",
            engagement_code=f"ENG-BLOOD-CRON-{engagement_id}",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=diagnostic_package_id,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() + timedelta(days=7),
            status="running",
            blood_report_notification=blood_report_notification,
        )
    )
    await test_db_session.flush()

    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date.today() - timedelta(days=1),
            slot_start_time=time(9, 0),
            booking_id=booking_id,
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=1,
            engagement_id=engagement_id,
            status="active",
            metsights_record_id=metsights_record_id,
        )
    )
    if (
        existing_blood_parameters is not None
        or existing_verified_at is not None
        or existing_full_report is not None
        or existing_diag_url is not None
    ):
        test_db_session.add(
            IndividualHealthReport(
                report_id=88000 + engagement_id,
                user_id=user_id,
                engagement_id=engagement_id,
                assessment_instance_id=assessment_id,
                blood_parameters=existing_blood_parameters,
                diagnostic_report_url=existing_diag_url,
                blood_parameters_verified_at=existing_verified_at,
                blood_parameters_full_report=existing_full_report,
            )
        )
    await test_db_session.commit()


def _build_services(monkeypatch) -> tuple[MetsightsService, MetsightsSyncService, AssessmentsService, NotificationsService]:
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())
    sync_service = MetsightsSyncService(
        metsights_service=metsights_service,
        users_repository=UsersRepository(),
        engagements_service=get_engagements_service(),
        assessments_service=get_assessments_service(),
        platform_settings_service=get_platform_settings_service_readonly(),
        questionnaire_repository=QuestionnaireRepository(),
    )
    assessments_service = get_assessments_service()
    notifications_service = NotificationsService(NotificationsRepository())
    return metsights_service, sync_service, assessments_service, notifications_service


def _fake_group_factory():
    async def _fake_group(_db, _raw, *, diagnostic_package_id):
        grouped = [
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 91.0, "unit": "mg/dL"}],
            }
        ]
        return grouped, _raw

    return _fake_group


def test_match_customer_by_name_accepts_cust_name():
    matched = _match_customer_by_name(
        [{"cust_name": "John Doe", "report_url": _REPORT_URL}],
        "John",
        "Doe",
    )
    assert matched is not None
    assert matched["cust_name"] == "John Doe"


@pytest.mark.asyncio
async def test_load_blood_reports_calls_report_before_digital_value(
    test_db_session, monkeypatch
):
    existing_blood = [
        {
            "group_name": "Metabolic",
            "test_count": 1,
            "tests": [{"parameter_key": "glucose_fasting", "value": 80.0, "unit": "mg/dL"}],
        }
    ]
    await _seed_running_participant(test_db_session, existing_blood_parameters=existing_blood)

    call_order: list[str] = []

    async def _fake_token():
        return "token"

    async def _fake_report(_token, booking_id):
        call_order.append("report")
        return _report_payload()

    async def _fake_digital(_token, booking_id):
        call_order.append("digital")
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "digital_data": [{"parameter_id": "1", "value": "91.0", "unit": "mg/dL"}],
                }
            ]
        }

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports._group_provider_blood",
        _fake_group_factory(),
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        return {"fields_pushed": ["glucose_fasting_value"]}

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _fake_push,
    )

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        return {"dispatched": 1}

    monkeypatch.setattr(
        "modules.notifications.service.NotificationsService.dispatch",
        _fake_dispatch,
    )

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert call_order == ["report", "digital"]
    assert result["loaded"] >= 1

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == 88001)
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url == _REPORT_URL
    assert ihr.blood_parameters_full_report is True
    assert ihr.blood_parameters_verified_at == _VERIFIED_AT_DT


@pytest.mark.asyncio
async def test_load_blood_reports_skips_digital_when_report_fails(
    test_db_session, monkeypatch
):
    await _seed_running_participant(test_db_session, engagement_id=88010, assessment_id=88010, user_id=88010)

    digital_calls: list[str] = []

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        raise RuntimeError("Healthians getBookingReport failed")

    async def _fake_digital(_token, booking_id):
        digital_calls.append(booking_id)
        return {"data": []}

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert digital_calls == []
    skipped = [d for d in result["details"] if d["action"] == "skipped"]
    assert any("getBookingReport failed" in d["reason"] for d in skipped)


@pytest.mark.asyncio
async def test_load_blood_reports_skips_reload_when_verified_at_unchanged(
    test_db_session, monkeypatch
):
    existing_blood = [
        {
            "group_name": "Metabolic",
            "test_count": 1,
            "tests": [{"parameter_key": "glucose_fasting", "value": 80.0, "unit": "mg/dL"}],
        }
    ]
    await _seed_running_participant(
        test_db_session,
        user_id=88011,
        engagement_id=88011,
        assessment_id=88011,
        existing_blood_parameters=existing_blood,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_full_report=True,
        existing_diag_url=_REPORT_URL,
    )

    digital_calls: list[str] = []

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    async def _fake_digital(_token, booking_id):
        digital_calls.append(booking_id)
        return {"data": []}

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        return {"dispatched": 1}

    monkeypatch.setattr(
        "modules.notifications.service.NotificationsService.dispatch",
        _fake_dispatch,
    )

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert digital_calls == []
    assert any(
        "verified_at unchanged" in d["reason"]
        for d in result["details"]
        if d["action"] == "skipped"
    )


@pytest.mark.asyncio
async def test_load_blood_reports_skips_notification_when_full_report_zero(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88012,
        engagement_id=88012,
        assessment_id=88012,
    )

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload(full_report=0)

    async def _fake_digital(_token, _booking_id):
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "digital_data": [{"parameter_id": "1", "value": "91.0", "unit": "mg/dL"}],
                }
            ]
        }

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports._group_provider_blood",
        _fake_group_factory(),
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        return {"fields_pushed": []}

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _fake_push,
    )

    dispatch_calls: list[str] = []

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        dispatch_calls.append(payload.service_key)
        return {"dispatched": 1}

    monkeypatch.setattr(
        "modules.notifications.service.NotificationsService.dispatch",
        _fake_dispatch,
    )

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert dispatch_calls == []
    assert any(
        "full_report is 0" in d["reason"]
        for d in result["details"]
        if d["action"] == "skipped"
    )

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == 88012)
        )
    ).scalar_one()
    assert ihr.blood_parameters_full_report is False
    assert ihr.diagnostic_report_url == _REPORT_URL


@pytest.mark.asyncio
async def test_load_blood_reports_always_fetches_digital_value_even_when_blood_exists(
    test_db_session, monkeypatch
):
    existing_blood = [
        {
            "group_name": "Metabolic",
            "test_count": 1,
            "tests": [{"parameter_key": "glucose_fasting", "value": 80.0, "unit": "mg/dL"}],
        }
    ]
    await _seed_running_participant(test_db_session, existing_blood_parameters=existing_blood)

    digital_calls: list[str] = []

    async def _fake_token():
        return "token"

    async def _fake_digital(_token, booking_id):
        digital_calls.append(booking_id)
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "digital_data": [{"parameter_id": "1", "value": "91.0", "unit": "mg/dL"}],
                }
            ]
        }

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports._group_provider_blood",
        _fake_group_factory(),
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameters_from_report",
        _fake_draft,
    )

    push_calls: list[str] = []

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        push_calls.append(category_key)
        return {"fields_pushed": ["glucose_fasting_value"]}

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _fake_push,
    )

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        return {"dispatched": 1}

    monkeypatch.setattr(
        "modules.notifications.service.NotificationsService.dispatch",
        _fake_dispatch,
    )

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert len(digital_calls) == 1
    assert digital_calls[0] == "BOOK-88001"
    assert push_calls == ["blood-parameters"]
    drafted = [d for d in result["details"] if d["action"] == "drafted"]
    assert len(drafted) == 1


@pytest.mark.asyncio
async def test_load_blood_reports_skips_metsights_push_when_bioai_report_generated(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        engagement_id=88002,
        assessment_id=88002,
        user_id=88002,
    )

    async def _fake_token():
        return "token"

    async def _fake_digital(_token, _booking_id):
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "digital_data": [{"parameter_id": "1", "value": "91.0", "unit": "mg/dL"}],
                }
            ]
        }

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    async def _report_exists(self, *, record_id: str, assessment_type_code: str | None):
        return True

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports._group_provider_blood",
        _fake_group_factory(),
    )
    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_exists,
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameters_from_report",
        _fake_draft,
    )

    push_calls: list[str] = []

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        push_calls.append(category_key)
        return {"fields_pushed": []}

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _fake_push,
    )

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        return {"dispatched": 1}

    monkeypatch.setattr(
        "modules.notifications.service.NotificationsService.dispatch",
        _fake_dispatch,
    )

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert push_calls == []
    skipped = [
        d for d in result["details"]
        if d["action"] == "skipped" and "BioAI report already generated" in d["reason"]
    ]
    assert len(skipped) == 1

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == 88002)
        )
    ).scalar_one_or_none()
    assert ihr is not None
    assert ihr.blood_parameters is not None


@pytest.mark.asyncio
async def test_load_blood_reports_uses_fetch_collections_data_booking_id(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88003,
        engagement_id=88003,
        assessment_id=88003,
        booking_id=None,
        metsights_record_id="5650A9ED33FD",
    )

    fetch_payload = {
        "reference_id": None,
        "is_success": False,
        "data": {
            "file_type": "pdf",
            "booking_id": "19121084542",
            "file_category": "blood_report_pdf",
        },
        "provider": {
            "name": "Healthians (No Package)",
            "lab_provider": {"code": "Healthians"},
        },
    }

    digital_calls: list[str] = []

    async def _fake_fetch_collections(self, *, record_id: str):
        assert record_id == "5650A9ED33FD"
        return fetch_payload

    async def _fake_token():
        return "token"

    async def _fake_digital(_token, booking_id):
        digital_calls.append(booking_id)
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "digital_data": [{"parameter_id": "1", "value": "91.0", "unit": "mg/dL"}],
                }
            ]
        }

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.get_fetch_collections",
        _fake_fetch_collections,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_access_token",
        _fake_token,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_digital_value",
        _fake_digital,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.healthians_client.get_booking_report",
        _fake_report,
    )
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports._group_provider_blood",
        _fake_group_factory(),
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameters_from_report",
        _fake_draft,
    )

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        return {"fields_pushed": ["glucose_fasting_value"]}

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _fake_push,
    )

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        return {"dispatched": 1}

    monkeypatch.setattr(
        "modules.notifications.service.NotificationsService.dispatch",
        _fake_dispatch,
    )

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert len(digital_calls) == 1
    assert digital_calls[0] == "19121084542"
    loaded = [d for d in result["details"] if d["action"] == "loaded"]
    assert any("Metsights reference_id" in d["reason"] for d in loaded)
