"""Tests for integration_sync_logs on report-load cron jobs."""

from __future__ import annotations

from datetime import date, time, timedelta

import pytest
from sqlalchemy import text

from core.config import settings
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.notifications.load_bioai_reports import load_bioai_reports
from modules.notifications.load_fitprint_reports import load_fitprint_reports
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.reports.models import IndividualHealthReport
from modules.assessments.models import AssessmentInstance
from modules.users.models import User


async def _seed_bioai_participant(
    test_db_session,
    *,
    user_id: int = 99001,
    engagement_id: int = 99001,
    assessment_id: int = 99001,
    assessment_type_code: str = "2",
    metsights_record_id: str = "MS-BIOAI-CRON",
    bioai_notification: str | None = "booking-alert-whatsapp",
):
    await test_db_session.execute(
        text(
            "INSERT INTO notification_services "
            "(service_key, display_name, channel, webhook_path, is_active, "
            "require_blood_report_url, require_bio_ai_report_url, require_participant_detail) "
            "VALUES (:sk, :dn, 'email', 'test-webhook', true, false, false, false) "
            "ON CONFLICT (service_key) DO NOTHING"
        ),
        {"sk": "booking-alert-whatsapp", "dn": "booking-alert-whatsapp"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (1, 'PRO', 'Pro', '2', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        )
    )
    test_db_session.add(
        User(
            user_id=user_id,
            first_name="Jane",
            last_name="Doe",
            phone=f"{user_id}000000",
            age=30,
            status="active",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="BioAI Cron Engagement",
            engagement_code=f"ENG-BIOAI-CRON-{engagement_id}",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() + timedelta(days=7),
            status="running",
            bioai_report_notification=bioai_notification,
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date.today() - timedelta(days=1),
            slot_start_time=time(9, 0),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=1,
            engagement_id=engagement_id,
            status="completed",
            metsights_record_id=metsights_record_id,
        )
    )
    await test_db_session.commit()


async def _seed_fitprint_participant(
    test_db_session,
    *,
    user_id: int = 99101,
    engagement_id: int = 99101,
    assessment_id: int = 99101,
    metsights_record_id: str = "MS-FITPRINT-CRON",
):
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, package_name, diagnostic_provider, status) "
            "VALUES (1, 'Test Diagnostic', 'test_provider', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, assessment_type_code, status) "
            "VALUES (7, 'FITPRINT', 'FitPrint', '7', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = EXCLUDED.assessment_type_code"
        )
    )
    test_db_session.add(
        User(
            user_id=user_id,
            first_name="Fit",
            last_name="Print",
            phone=f"{user_id}000000",
            age=30,
            status="active",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="FitPrint Cron Engagement",
            engagement_code=f"ENG-FITPRINT-CRON-{engagement_id}",
            engagement_type="bio_ai",
            assessment_package_id=7,
            diagnostic_package_id=1,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() + timedelta(days=7),
            status="running",
            bioai_report_notification="booking-alert-whatsapp",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        EngagementParticipant(
            engagement_id=engagement_id,
            user_id=user_id,
            engagement_date=date.today() - timedelta(days=1),
            slot_start_time=time(9, 0),
        )
    )
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=7,
            engagement_id=engagement_id,
            status="completed",
            metsights_record_id=metsights_record_id,
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_load_bioai_reports_creates_metsights_sync_logs(test_db_session, monkeypatch):
    await _seed_bioai_participant(test_db_session, bioai_notification=None)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())

    async def _fake_blood_params(*, record_id: str):
        return {"is_complete": True}

    async def _fake_report(*, record_id: str, assessment_type_code: str | None):
        return {"file": "https://example.com/bioai.pdf", "record_id": record_id}

    async def _fake_report_pdf(*, record_id: str, assessment_type_code: str | None):
        return {"file": "https://example.com/bioai.pdf"}

    monkeypatch.setattr(metsights_service, "get_blood_parameters", _fake_blood_params)
    monkeypatch.setattr(metsights_service, "get_report", _fake_report)
    monkeypatch.setattr(metsights_service, "get_report_pdf", _fake_report_pdf)

    result = await load_bioai_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=NotificationsService(NotificationsRepository()),
    )

    assert result["loaded"] == 1
    rows = (
        await test_db_session.execute(
            text(
                "SELECT provider, status, api_endpoint_url "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND user_id = 99001 ORDER BY sync_log_id"
            )
        )
    ).all()
    assert len(rows) >= 2
    assert all(row.provider == "metsights" for row in rows)
    assert all(row.status == "success" for row in rows)
    endpoints = " ".join(row.api_endpoint_url for row in rows)
    assert "blood-parameters" in endpoints
    assert "/reports/" in endpoints


@pytest.mark.asyncio
async def test_load_bioai_reports_sends_notifications_when_configured(test_db_session, monkeypatch):
    await _seed_bioai_participant(test_db_session, bioai_notification="booking-alert-whatsapp")
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())

    async def _fake_blood_params(*, record_id: str):
        return {"is_complete": True}

    async def _fake_report(*, record_id: str, assessment_type_code: str | None):
        return {"file": "https://example.com/bioai.pdf"}

    async def _fake_report_pdf(*, record_id: str, assessment_type_code: str | None):
        return {"file": "https://example.com/bioai.pdf"}

    monkeypatch.setattr(metsights_service, "get_blood_parameters", _fake_blood_params)
    monkeypatch.setattr(metsights_service, "get_report", _fake_report)
    monkeypatch.setattr(metsights_service, "get_report_pdf", _fake_report_pdf)

    dispatch_calls: list[int] = []

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        dispatch_calls.extend(payload.user_ids)
        return {"dispatched": len(payload.user_ids)}

    monkeypatch.setattr(NotificationsService, "dispatch", _fake_dispatch)

    result = await load_bioai_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=NotificationsService(NotificationsRepository()),
    )

    assert result["loaded"] == 1
    assert result["notified"] == 1
    assert dispatch_calls == [99001]


@pytest.mark.asyncio
async def test_load_fitprint_reports_creates_sync_logs_without_notifications(
    test_db_session, monkeypatch
):
    await _seed_fitprint_participant(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())

    async def _fake_report(*, record_id: str, assessment_type_code: str | None):
        return {"file": "https://example.com/fitprint.pdf", "record_id": record_id}

    async def _fake_report_pdf(*, record_id: str, assessment_type_code: str | None):
        return {"file": "https://example.com/fitprint.pdf"}

    monkeypatch.setattr(metsights_service, "get_report", _fake_report)
    monkeypatch.setattr(metsights_service, "get_report_pdf", _fake_report_pdf)

    dispatch_calls: list[int] = []

    async def _fake_dispatch(self, db, *, payload, triggered_by_user_id=None):
        dispatch_calls.extend(payload.user_ids)
        return {"dispatched": len(payload.user_ids)}

    monkeypatch.setattr(NotificationsService, "dispatch", _fake_dispatch)

    result = await load_fitprint_reports(
        test_db_session,
        metsights_service=metsights_service,
    )

    assert result["loaded"] == 1
    assert "notified" not in result
    assert dispatch_calls == []

    rows = (
        await test_db_session.execute(
            text(
                "SELECT provider, status, api_endpoint_url "
                "FROM integration_sync_logs WHERE provider = 'metsights' "
                "AND user_id = 99101 ORDER BY sync_log_id"
            )
        )
    ).all()
    assert len(rows) >= 2
    assert all("fitness-reports" in row.api_endpoint_url for row in rows)


@pytest.mark.asyncio
async def test_load_bioai_reports_skips_fitprint_type(test_db_session, monkeypatch):
    """FitPrint (type 7) must never be picked up by load_bioai_reports."""
    await _seed_fitprint_participant(test_db_session)
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())

    async def _unexpected(*args, **kwargs):
        raise AssertionError("load_bioai_reports should not call MetSights for FitPrint")

    monkeypatch.setattr(metsights_service, "get_blood_parameters", _unexpected)
    monkeypatch.setattr(metsights_service, "get_report", _unexpected)

    result = await load_bioai_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=NotificationsService(NotificationsRepository()),
    )

    assert result["matched"] == 0
