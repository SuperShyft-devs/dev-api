"""Tests for BioAI load vitals BP fallback (120/80)."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from core.config import settings
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.models import AssessmentInstance
from modules.engagements.dependencies import get_engagements_service
from modules.engagements.models import Engagement, EngagementParticipant
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.notifications.load_bioai_reports import load_bioai_reports
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.models import QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.models import User
from modules.users.repository import UsersRepository


async def _seed_bioai_participant(
    test_db_session,
    *,
    user_id: int = 199201,
    engagement_id: int = 199201,
    assessment_id: int = 199201,
    metsights_record_id: str = "MS-BIOAI-VITALS",
):
    await test_db_session.execute(
        text(
            "INSERT INTO engagement_types (code, display_name, is_active) "
            "VALUES ('bio_ai', 'Bio AI', true) ON CONFLICT (code) DO NOTHING"
        )
    )
    type_row = (
        await test_db_session.execute(
            text("SELECT id FROM engagement_types WHERE code = 'bio_ai'")
        )
    ).one()
    engagement_type_id = int(type_row[0])
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
            first_name="Vitals",
            last_name="Fallback",
            phone=f"{user_id}000000",
            age=30,
            status="active",
        )
    )
    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="BioAI Vitals Fallback Engagement",
            engagement_code=f"ENG-BIOAI-VITALS-{engagement_id}",
            engagement_type=engagement_type_id,
            assessment_package_id=1,
            diagnostic_package_id=1,
            city="Bengaluru",
            slot_duration=20,
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() + timedelta(days=7),
            status="running",
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


def _build_sync_service(metsights_service: MetsightsService) -> MetsightsSyncService:
    return MetsightsSyncService(
        metsights_service=metsights_service,
        users_repository=UsersRepository(),
        engagements_service=get_engagements_service(),
        assessments_service=get_assessments_service(),
        platform_settings_service=get_platform_settings_service_readonly(),
        questionnaire_repository=QuestionnaireRepository(),
    )


@pytest.mark.asyncio
async def test_draft_vitals_blood_pressure_fallbacks_writes_defaults(test_db_session):
    await _seed_bioai_participant(test_db_session)
    assessments_service = get_assessments_service()

    assert await assessments_service.is_vitals_blood_pressure_missing(
        test_db_session,
        assessment_instance_id=199201,
    )

    result = await assessments_service.draft_vitals_blood_pressure_fallbacks(
        test_db_session,
        user_id=199201,
        assessment_instance_id=199201,
    )
    await test_db_session.commit()

    assert result["responses_drafted"] == 2
    assert set(result["fallback_keys"]) == {
        "systolic_blood_pressure",
        "diastolic_blood_pressure",
    }

    rows = (
        await test_db_session.execute(
            text(
                "SELECT qd.question_key, qr.answer "
                "FROM questionnaire_responses qr "
                "JOIN questionnaire_definitions qd ON qd.question_id = qr.question_id "
                "WHERE qr.assessment_instance_id = 199201 "
                "AND qd.question_key IN ('systolic_blood_pressure', 'diastolic_blood_pressure')"
            )
        )
    ).all()
    answers = {row[0]: row[1] for row in rows}
    assert answers["systolic_blood_pressure"]["value"] == 120.0
    assert answers["diastolic_blood_pressure"]["value"] == 80.0

    assert not await assessments_service.is_vitals_blood_pressure_missing(
        test_db_session,
        assessment_instance_id=199201,
    )


@pytest.mark.asyncio
async def test_draft_vitals_blood_pressure_fallbacks_does_not_overwrite_existing(test_db_session):
    await _seed_bioai_participant(
        test_db_session,
        user_id=199202,
        engagement_id=199202,
        assessment_id=199202,
    )
    assessments_service = get_assessments_service()

    systolic_id = (
        await test_db_session.execute(
            text(
                "SELECT question_id FROM questionnaire_definitions "
                "WHERE question_key = 'systolic_blood_pressure'"
            )
        )
    ).scalar_one()
    diastolic_id = (
        await test_db_session.execute(
            text(
                "SELECT question_id FROM questionnaire_definitions "
                "WHERE question_key = 'diastolic_blood_pressure'"
            )
        )
    ).scalar_one()
    vitals_category_id = (
        await test_db_session.execute(
            text(
                "SELECT category_id FROM questionnaire_categories "
                "WHERE category_key = 'vitals' AND category_of = 'metsights'"
            )
        )
    ).scalar_one()

    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=199202,
            question_id=int(systolic_id),
            category_ids=[int(vitals_category_id)],
            answer={"value": 135.0, "unit": "0"},
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=199202,
            question_id=int(diastolic_id),
            category_ids=[int(vitals_category_id)],
            answer={"value": 85.0, "unit": "0"},
        )
    )
    await test_db_session.commit()

    result = await assessments_service.draft_vitals_blood_pressure_fallbacks(
        test_db_session,
        user_id=199202,
        assessment_instance_id=199202,
    )

    assert result["responses_drafted"] == 0
    assert not await assessments_service.is_vitals_blood_pressure_missing(
        test_db_session,
        assessment_instance_id=199202,
    )


@pytest.mark.asyncio
async def test_load_bioai_reports_recovers_with_default_bp_when_vitals_missing(
    test_db_session,
    monkeypatch,
):
    await _seed_bioai_participant(
        test_db_session,
        user_id=199204,
        engagement_id=199204,
        assessment_id=199204,
    )
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())
    assessments_service = get_assessments_service()
    sync_service = _build_sync_service(metsights_service)

    report_attempts = 0

    async def _fake_blood_params(*, record_id: str):
        return {"is_complete": True}

    async def _fake_report(*, record_id: str, assessment_type_code: str | None):
        nonlocal report_attempts
        report_attempts += 1
        if report_attempts == 1:
            return None
        return {"file": "https://example.com/bioai.pdf", "record_id": record_id}

    register_attempts = 0

    async def _fake_register(*args, **kwargs):
        nonlocal register_attempts
        register_attempts += 1
        if register_attempts == 1:
            raise RuntimeError("report not ready yet")
        return "https://bio-ai-reports.supershyft.com/r/vitals-recovery"

    push_calls: list[dict] = []

    async def _fake_push(db, **kwargs):
        push_calls.append(kwargs)
        return {"fields_pushed": ["systolic_blood_pressure", "diastolic_blood_pressure"]}

    monkeypatch.setattr(metsights_service, "get_blood_parameters", _fake_blood_params)
    monkeypatch.setattr(metsights_service, "get_report", _fake_report)
    monkeypatch.setattr(
        "modules.notifications.load_bioai_reports.register_permanent_bio_ai_report_url",
        _fake_register,
    )
    monkeypatch.setattr(sync_service, "_push_category_to_metsights", _fake_push)

    result = await load_bioai_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=NotificationsService(NotificationsRepository()),
        assessments_service=assessments_service,
        sync_service=sync_service,
        send_notifications=False,
        user_ids={199204},
    )

    assert result["loaded"] == 1
    assert report_attempts == 2
    assert register_attempts == 2
    assert len(push_calls) == 1
    assert push_calls[0]["category_key"] == "vitals"

    drafted = [d for d in result["details"] if d["action"] == "drafted"]
    pushed = [d for d in result["details"] if d["action"] == "pushed"]
    assert drafted
    assert pushed
    assert "120/80" in drafted[0]["reason"]


@pytest.mark.asyncio
async def test_load_bioai_reports_skips_recovery_when_bp_already_present(
    test_db_session,
    monkeypatch,
):
    await _seed_bioai_participant(
        test_db_session,
        user_id=199203,
        engagement_id=199203,
        assessment_id=199203,
    )
    assessments_service = get_assessments_service()
    await assessments_service.draft_vitals_blood_pressure_fallbacks(
        test_db_session,
        user_id=199203,
        assessment_instance_id=199203,
    )
    await test_db_session.commit()

    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    metsights_service = MetsightsService(client=MetsightsClient())
    sync_service = _build_sync_service(metsights_service)

    async def _fake_blood_params(*, record_id: str):
        return {"is_complete": True}

    async def _fake_report(*, record_id: str, assessment_type_code: str | None):
        return None

    push_mock = AsyncMock()
    monkeypatch.setattr(metsights_service, "get_blood_parameters", _fake_blood_params)
    monkeypatch.setattr(metsights_service, "get_report", _fake_report)
    monkeypatch.setattr(
        "modules.notifications.load_bioai_reports.register_permanent_bio_ai_report_url",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(sync_service, "_push_category_to_metsights", push_mock)

    result = await load_bioai_reports(
        test_db_session,
        metsights_service=metsights_service,
        notifications_service=NotificationsService(NotificationsRepository()),
        assessments_service=assessments_service,
        sync_service=sync_service,
        send_notifications=False,
        user_ids={199203},
    )

    assert result["loaded"] == 0
    assert result["skipped"] == 1
    push_mock.assert_not_called()
    reasons = [d["reason"] for d in result["details"]]
    assert any("no report data returned" in r for r in reasons)
