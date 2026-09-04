"""Tests for load_blood_reports cron job."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone

import pytest
from sqlalchemy import select, text

from core.config import settings
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance, AssessmentPackageCategory
from modules.engagements.dependencies import get_engagements_service
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.notifications.load_blood_reports import load_blood_reports, _match_customer_by_name
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.models import QuestionnaireCategory
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.blood_report_archival import is_archived_blood_report_url
from modules.reports.models import IndividualHealthReport
from modules.users.models import User
from modules.users.repository import UsersRepository

_REPORT_URL = "https://example.com/blood-report.pdf"
_ARCHIVED_REPORT_URL = "https://supershyft.com/reports/AbCdEfGhIjKlMnOp.pdf"
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


async def _engagement_type_id(test_db_session, code: str = "bio_ai") -> int:
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES (:code, :dn, true) "
            "ON CONFLICT (code) DO UPDATE SET is_active = true"
        ),
        {"code": code, "dn": code},
    )
    await test_db_session.flush()
    row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = :code"),
            {"code": code},
        )
    ).one()
    return int(row[0])


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
    engagement_type_id = await _engagement_type_id(test_db_session)

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
            engagement_type=engagement_type_id,
            assessment_package_id=1,
            diagnostic_package_id=diagnostic_package_id,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() + timedelta(days=7),
            status="running",
        )
    )
    await test_db_session.flush()

    if blood_report_notification:
        evt_row = (
            await test_db_session.execute(
                text(
                    "SELECT id FROM auto_notification_events "
                    "WHERE event_code = 'blood_report_ready'"
                )
            )
        ).one_or_none()
        if evt_row is not None:
            services_json = json.dumps(
                [{"service_key": blood_report_notification, "external_link": None}]
            )
            await test_db_session.execute(
                text(
                    "INSERT INTO engagement_notifications "
                    "(engagement_id, notification_event_id, notification_services) "
                    "VALUES (:eid, :evt, CAST(:services AS jsonb)) "
                    "ON CONFLICT (engagement_id, notification_event_id) DO UPDATE "
                    "SET notification_services = EXCLUDED.notification_services"
                ),
                {
                    "eid": engagement_id,
                    "evt": int(evt_row[0]),
                    "services": services_json,
                },
            )

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


async def _fake_resolve_persistable_diagnostic_report_url(
    healthians_url: str,
    *,
    is_full_report: bool,
    existing_url: str | None,
    assessment_instance_id: int,
) -> str | None:
    healthians = (healthians_url or "").strip()
    existing = (existing_url or "").strip()
    if not is_full_report:
        if existing and is_archived_blood_report_url(existing):
            return existing
        return None
    if existing and is_archived_blood_report_url(existing):
        return existing
    if not healthians:
        return None
    return _ARCHIVED_REPORT_URL


def _build_services(monkeypatch) -> tuple[MetsightsService, MetsightsSyncService, AssessmentsService, NotificationsService]:
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.resolve_persistable_diagnostic_report_url",
        _fake_resolve_persistable_diagnostic_report_url,
    )
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


async def _ensure_metsights_blood_categories(
    test_db_session,
    *,
    package_id: int,
    instance_id: int,
    submitted: bool = False,
    submitted_keys: frozenset[str] | set[str] | None = None,
) -> None:
    keys = (
        ("blood-parameters", "Blood Parameters", 98111),
        ("advanced-blood-parameters", "Advanced Blood Parameters", 98112),
    )
    for key, name, category_id in keys:
        category = (
            await test_db_session.execute(
                select(QuestionnaireCategory).where(
                    QuestionnaireCategory.category_key == key,
                    QuestionnaireCategory.category_of == "metsights",
                )
            )
        ).scalar_one_or_none()
        if category is None:
            category = QuestionnaireCategory(
                category_id=category_id,
                category_key=key,
                display_name=name,
                category_of="metsights",
                status="active",
            )
            test_db_session.add(category)
            await test_db_session.flush()
        link = (
            await test_db_session.execute(
                select(AssessmentPackageCategory).where(
                    AssessmentPackageCategory.package_id == package_id,
                    AssessmentPackageCategory.category_id == category.category_id,
                )
            )
        ).scalar_one_or_none()
        if link is None:
            test_db_session.add(
                AssessmentPackageCategory(
                    package_id=package_id,
                    category_id=int(category.category_id),
                )
            )
        mark_submitted = (
            key in submitted_keys if submitted_keys is not None else submitted
        )
        if mark_submitted:
            existing_progress = (
                await test_db_session.execute(
                    select(AssessmentCategoryProgress).where(
                        AssessmentCategoryProgress.assessment_instance_id == instance_id,
                        AssessmentCategoryProgress.category_id == category.category_id,
                    )
                )
            ).scalar_one_or_none()
            if existing_progress is None:
                test_db_session.add(
                    AssessmentCategoryProgress(
                        assessment_instance_id=instance_id,
                        category_id=int(category.category_id),
                        status="complete",
                        is_submitted=True,
                    )
                )
            else:
                existing_progress.is_submitted = True
                existing_progress.status = "complete"
    await test_db_session.commit()


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
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL
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
        existing_diag_url=_ARCHIVED_REPORT_URL,
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

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
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

    assert digital_calls == []
    assert any(
        "verified_at unchanged" in d["reason"]
        for d in result["details"]
        if d["action"] == "skipped"
    )
    assert "blood-parameters" in push_calls


@pytest.mark.asyncio
async def test_load_blood_reports_reloads_when_full_report_missing_archived_url(
    test_db_session, monkeypatch
):
    """Full report with verified_at match but no archived PDF must re-enter load path."""
    existing_blood = [
        {
            "group_name": "Metabolic",
            "test_count": 1,
            "tests": [{"parameter_key": "glucose_fasting", "value": 80.0, "unit": "mg/dL"}],
        }
    ]
    await _seed_running_participant(
        test_db_session,
        user_id=880111,
        engagement_id=880111,
        assessment_id=880111,
        booking_id="BOOK-880111",
        existing_blood_parameters=existing_blood,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_full_report=True,
        existing_diag_url=None,
    )

    digital_calls: list[str] = []

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    async def _fake_digital(_token, booking_id):
        digital_calls.append(booking_id)
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "tests": [{"parameter": "Glucose Fasting", "value": "90", "unit": "mg/dL"}],
                }
            ]
        }

    async def _fake_group(db, raw_customer, *, diagnostic_package_id):
        return (
            [
                {
                    "group_name": "Metabolic",
                    "test_count": 1,
                    "tests": [{"parameter_key": "glucose_fasting", "value": 90.0, "unit": "mg/dL"}],
                }
            ],
            {"raw": True},
        )

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
        _fake_group,
    )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
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

    assert digital_calls == ["BOOK-880111"]
    assert result["loaded"] >= 1
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == 880111)
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL


@pytest.mark.asyncio
async def test_load_blood_reports_reloads_when_verified_at_unchanged_but_blood_missing(
    test_db_session, monkeypatch
):
    """After remove-reports, metadata may match Healthians but blood_parameters are gone."""
    await _seed_running_participant(
        test_db_session,
        user_id=990115,
        engagement_id=990115,
        assessment_id=990115,
        booking_id="BOOK-990115",
        existing_blood_parameters=None,
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
        return {
            "data": [
                {
                    "customer_name": "John Doe",
                    "deal_id": "BOOK-990115",
                    "test_values": [{"test_name": "Glucose Fasting", "value": "80", "unit": "mg/dL"}],
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

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    result = await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
        engagement_id=990115,
        user_ids={990115},
        all_engagements=True,
    )

    assert "BOOK-990115" in digital_calls
    assert result["loaded"] >= 1

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(IndividualHealthReport.assessment_instance_id == 990115)
        )
    ).scalar_one()
    assert ihr.blood_parameters is not None


@pytest.mark.asyncio
async def test_load_blood_reports_skips_metsights_retry_when_categories_submitted(
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
        user_id=88013,
        engagement_id=88013,
        assessment_id=88013,
        existing_blood_parameters=existing_blood,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_full_report=True,
        existing_diag_url=_ARCHIVED_REPORT_URL,
    )
    await _ensure_metsights_blood_categories(
        test_db_session,
        package_id=1,
        instance_id=88013,
        submitted=True,
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

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        raise AssertionError("should not draft when blood categories are already submitted")

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

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

    assert digital_calls == []
    assert push_calls == []
    assert any(
        "already submitted to Metsights" in d["reason"]
        for d in result["details"]
        if d["action"] == "skipped"
    )


@pytest.mark.asyncio
async def test_load_blood_reports_repushes_blood_when_missing_on_metsights(
    test_db_session, monkeypatch
):
    """Stale local blood-parameters submitted flag must not block advanced push."""
    existing_blood = [
        {
            "group_name": "Metabolic",
            "test_count": 1,
            "tests": [{"parameter_key": "glucose_fasting", "value": 80.0, "unit": "mg/dL"}],
        }
    ]
    await _seed_running_participant(
        test_db_session,
        user_id=88014,
        engagement_id=88014,
        assessment_id=88014,
        existing_blood_parameters=existing_blood,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_full_report=True,
        existing_diag_url=_ARCHIVED_REPORT_URL,
    )
    await _ensure_metsights_blood_categories(
        test_db_session,
        package_id=1,
        instance_id=88014,
        submitted_keys={"blood-parameters"},
    )

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    async def _fake_digital(_token, booking_id):
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

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 11}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _fake_fallback(db, *, user_id, assessment_instance_id, category_keys=None):
        return {"responses_drafted": 0}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameter_internal_fallbacks",
        _fake_fallback,
    )

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
    )

    async def _parent_missing(self, *, record_id: str, resource: str):
        return None

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.get_record_subresource_or_none",
        _parent_missing,
    )

    push_calls: list[str] = []

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        push_calls.append(category_key)
        return {"fields_pushed": ["field"]}

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

    assert push_calls[:2] == ["blood-parameters", "advanced-blood-parameters"]
    assert any(
        "missing on Metsights; re-pushing before advanced-blood-parameters" in d["reason"]
        for d in result["details"]
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
    assert ihr.diagnostic_report_url is None


@pytest.mark.asyncio
async def test_load_blood_reports_send_notifications_false_never_dispatches(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88013,
        engagement_id=88013,
        assessment_id=88013,
    )

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload(full_report=1)

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
        send_notifications=False,
    )

    assert dispatch_calls == []
    assert result["notified"] == 0
    assert result["loaded"] >= 1


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
    assert "blood-parameters" in push_calls
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


@pytest.mark.asyncio
async def test_load_blood_reports_persists_metsights_sync_log_on_push_failure(
    test_db_session, monkeypatch
):
    """Failed Metsights blood pushes must still write integration_sync_logs."""
    await _seed_running_participant(
        test_db_session,
        user_id=88004,
        engagement_id=88004,
        assessment_id=88004,
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
        return {"responses_drafted": 11}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
    )

    async def _fake_fallback(db, *, user_id, assessment_instance_id, category_keys=None):
        return {"responses_drafted": 0, "fallback_keys": []}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameter_internal_fallbacks",
        _fake_fallback,
    )

    from modules.audit.models import IntegrationSyncLog
    from modules.audit.repository import AuditRepository

    async def _failing_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        audit_repo = AuditRepository()
        sync_log = await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=88004,
                user_id=user_id,
                provider="metsights",
                api_endpoint_url=f"/records/MS-BLOOD-CRON/{category_key}/",
                request_payload={"category": category_key},
                status="pending",
            ),
        )
        await audit_repo.update_sync_log_status(
            db,
            sync_log_id=sync_log.sync_log_id,
            status="failed",
            error_message=f"{category_key} rejected by Metsights",
        )
        raise RuntimeError(f"{category_key} rejected by Metsights")

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _failing_push,
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

    failed = [d for d in result["details"] if d["action"] == "failed" and "metsights push failed" in d["reason"]]
    assert len(failed) >= 1

    rows = (
        await test_db_session.execute(
            text(
                "SELECT provider, status, api_endpoint_url, error_message "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND user_id = 88004 AND status = 'failed' ORDER BY sync_log_id"
            )
        )
    ).mappings().all()
    assert len(rows) >= 1
    assert all(row["provider"] == "metsights" for row in rows)
    assert any("blood-parameters" in row["api_endpoint_url"] for row in rows)
    assert any("rejected by Metsights" in (row["error_message"] or "") for row in rows)


@pytest.mark.asyncio
async def test_load_blood_reports_retries_with_avg_fallbacks_on_full_report_push_failure(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88005,
        engagement_id=88005,
        assessment_id=88005,
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
        return _report_payload(full_report=1)

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

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
    )

    fallback_calls: list[str] = []

    async def _fake_fallback(db, *, user_id, assessment_instance_id, category_keys=None):
        fallback_calls.extend(category_keys or [])
        return {"responses_drafted": 3, "fallback_keys": ["glucose_fasting", "total_cholesterol", "hdlc_value"]}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameter_internal_fallbacks",
        _fake_fallback,
    )

    push_attempts: list[str] = []

    async def _flaky_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        push_attempts.append(category_key)
        if push_attempts.count(category_key) == 1:
            raise RuntimeError(f"{category_key} rejected by Metsights")
        return {"fields_pushed": ["glucose_fasting_value", "total_cholesterol"]}

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _flaky_push,
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

    assert "blood-parameters" in fallback_calls
    assert push_attempts.count("blood-parameters") == 2
    assert any(
        "internal average blood fallbacks" in d["reason"]
        for d in result["details"]
        if d["action"] == "drafted"
    )
    assert any(
        "after average fallbacks" in d["reason"]
        for d in result["details"]
        if d["action"] == "pushed"
    )


@pytest.mark.asyncio
async def test_load_blood_reports_skips_avg_fallbacks_on_partial_report_push_failure(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88006,
        engagement_id=88006,
        assessment_id=88006,
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
        return _report_payload(full_report=0)

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

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
    )

    async def _should_not_fallback(*args, **kwargs):
        raise AssertionError("average fallbacks must not run for partial blood reports")

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameter_internal_fallbacks",
        _should_not_fallback,
    )

    push_attempts: list[str] = []

    async def _failing_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        push_attempts.append(category_key)
        raise RuntimeError(f"{category_key} rejected by Metsights")

    monkeypatch.setattr(
        "modules.metsights.sync_service.MetsightsSyncService._push_category_to_metsights",
        _failing_push,
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

    assert push_attempts == ["blood-parameters"]
    assert any(
        "metsights push failed for blood-parameters" in d["reason"]
        for d in result["details"]
        if d["action"] == "failed"
    )
    assert not any("average fallbacks" in d["reason"] for d in result["details"])
    assert any(
        "full_report is 0" in d["reason"]
        for d in result["details"]
        if d["action"] == "skipped"
    )


def test_blood_parameter_internal_fallbacks_map_known_keys():
    from db.seed.blood_parameters_registry import (
        BLOOD_PARAMETER_INTERNAL_FALLBACKS,
        FIELD_BY_KEY,
    )

    expected = {
        "total_cholesterol": 170.0,
        "hdlc_value": 50.0,
        "ldlc_value": 100.0,
        "triglycerides": 100.0,
        "glucose_fasting": 90.0,
        "glycated_haemoglobin": 5.2,
        "insulin": 6.0,
        "triiodothyronine": 1.1,
        "thyroxine": 8.0,
        "tsh_value": 2.0,
        "wbc_value": 7.0,
        "platelets": 250.0,
        "monocytes": 6.0,
        "alt_value": 20.0,
        "ast_value": 20.0,
        "uric_acid": 5.0,
        "ggt_value": 20.0,
        "lh_value": 5.0,
        "fsh_value": 5.0,
        "testosterone": 400.0,
    }
    assert set(BLOOD_PARAMETER_INTERNAL_FALLBACKS) == set(expected)
    for key, value in expected.items():
        assert key in FIELD_BY_KEY
        assert BLOOD_PARAMETER_INTERNAL_FALLBACKS[key][0] == value


@pytest.mark.asyncio
async def test_load_blood_reports_applies_proactive_fallbacks_before_push(
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
        user_id=88007,
        engagement_id=88007,
        assessment_id=88007,
        existing_blood_parameters=existing_blood,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_full_report=True,
        existing_diag_url=_ARCHIVED_REPORT_URL,
    )

    digital_calls: list[str] = []
    call_order: list[str] = []

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

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 1}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
    )

    async def _fake_fallback(db, *, user_id, assessment_instance_id, category_keys=None):
        call_order.append("fallback")
        return {"responses_drafted": 2, "fallback_keys": ["total_cholesterol"]}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameter_internal_fallbacks",
        _fake_fallback,
    )

    async def _fake_push(self, db, *, assessment_instance_id, user_id, category_key, category_of="metsights"):
        call_order.append(f"push:{category_key}")
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

    assert digital_calls == []
    assert call_order[0] == "fallback"
    assert any(item.startswith("push:") for item in call_order)
    assert any(
        "before Metsights push (full report)" in d["reason"]
        for d in result["details"]
        if d["action"] == "drafted"
    )


@pytest.mark.asyncio
async def test_load_blood_reports_persists_full_report_when_verified_at_unchanged(
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
        user_id=88008,
        engagement_id=88008,
        assessment_id=88008,
        existing_blood_parameters=existing_blood,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_full_report=False,
        existing_diag_url=_ARCHIVED_REPORT_URL,
    )

    digital_calls: list[str] = []

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload(full_report=1)

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

    async def _fake_draft(db, *, user_id, assessment_instance_id, allow_completed=False):
        return {"responses_drafted": 0}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    async def _fake_fallback(db, *, user_id, assessment_instance_id, category_keys=None):
        return {"responses_drafted": 0}

    monkeypatch.setattr(
        assessments_service,
        "draft_blood_parameter_internal_fallbacks",
        _fake_fallback,
    )

    async def _report_missing(self, *, record_id: str, assessment_type_code: str | None):
        return False

    monkeypatch.setattr(
        "modules.metsights.service.MetsightsService.is_bioai_report_generated",
        _report_missing,
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

    await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert digital_calls == []
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == 88008
            )
        )
    ).scalar_one_or_none()
    assert ihr is not None
    assert ihr.blood_parameters_full_report is True


@pytest.mark.asyncio
async def test_load_blood_reports_archival_failure_skips_healthians_url(
    test_db_session, monkeypatch
):
    await _seed_running_participant(test_db_session, user_id=88020, engagement_id=88020, assessment_id=88020)

    async def _fail_resolve(*args, **kwargs):
        if kwargs.get("is_full_report"):
            return None
        return (args[0] or "").strip() or None

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.resolve_persistable_diagnostic_report_url",
        _fail_resolve,
    )

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload()

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
        return {"responses_drafted": 0}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

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
        "blood report PDF archival failed" in d["reason"]
        for d in result["details"]
    )
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == 88020
            )
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url is None
    assert ihr.blood_parameters is not None


@pytest.mark.asyncio
async def test_load_blood_reports_reuses_existing_archived_url_without_rearchive(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88021,
        engagement_id=88021,
        assessment_id=88021,
        existing_diag_url=_ARCHIVED_REPORT_URL,
        existing_full_report=True,
        existing_verified_at=_VERIFIED_AT_DT,
        existing_blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 80.0, "unit": "mg/dL"}],
            }
        ],
    )

    resolve_calls: list[int] = []

    async def _tracking_resolve(healthians_url, *, is_full_report, existing_url, assessment_instance_id):
        resolve_calls.append(assessment_instance_id)
        return await _fake_resolve_persistable_diagnostic_report_url(
            healthians_url,
            is_full_report=is_full_report,
            existing_url=existing_url,
            assessment_instance_id=assessment_instance_id,
        )

    metsights_service, sync_service, assessments_service, notifications_service = _build_services(monkeypatch)

    monkeypatch.setattr(
        "modules.notifications.load_blood_reports.resolve_persistable_diagnostic_report_url",
        _tracking_resolve,
    )

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload()

    async def _fake_digital(_token, _booking_id):
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

    await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    assert resolve_calls == [88021]
    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == 88021
            )
        )
    ).scalar_one()
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL


@pytest.mark.asyncio
async def test_load_blood_reports_maps_archived_url_to_assessment_instance(
    test_db_session, monkeypatch
):
    await _seed_running_participant(
        test_db_session,
        user_id=88022,
        engagement_id=88022,
        assessment_id=88022,
    )

    async def _fake_token():
        return "token"

    async def _fake_report(_token, _booking_id):
        return _report_payload()

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
        return {"responses_drafted": 0}

    monkeypatch.setattr(assessments_service, "draft_blood_parameters_from_report", _fake_draft)

    await load_blood_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=notifications_service,
        assessments_service=assessments_service,
        sync_service=sync_service,
    )

    ihr = (
        await test_db_session.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == 88022
            )
        )
    ).scalar_one()
    assert ihr.assessment_instance_id == 88022
    assert ihr.user_id == 88022
    assert ihr.engagement_id == 88022
    assert ihr.diagnostic_report_url == _ARCHIVED_REPORT_URL

