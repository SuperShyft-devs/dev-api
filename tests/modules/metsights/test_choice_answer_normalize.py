"""Tests for choice answer normalization (import + maintenance job)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select

from core.config import settings
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.engagements.dependencies import get_engagements_service
from modules.maintenance.normalize_questionnaire_choice_answers import (
    normalize_questionnaire_choice_answers,
)
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import (
    MetsightsSyncService,
    _normalize_pulled_choice_answer,
)
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.models import QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.models import User
from modules.users.repository import UsersRepository
from tests.modules.metsights.test_metsights_missing_greenlet import _ensure_manual_metsights_category
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement


def _opts(*pairs: tuple[str, str]) -> list[SimpleNamespace]:
    return [SimpleNamespace(option_value=ov, display_name=dn) for ov, dn in pairs]


class TestNormalizePulledChoiceAnswer:
    def test_single_choice_maps_display_label_to_option_value(self):
        opts = _opts(
            ("0", "Less than 5 hours"),
            ("1", "Between 5 to 7 hours"),
            ("2", "Between 7 to 9 hours"),
            ("3", "More than 9 hours"),
        )
        answer, reason = _normalize_pulled_choice_answer(
            "Between 7 to 9 hours",
            question_type="single_choice",
            db_options=opts,
        )
        assert reason is None
        assert answer == "2"

    def test_single_choice_keeps_existing_option_value(self):
        opts = _opts(("0", "Never"), ("1", "Rarely"))
        answer, reason = _normalize_pulled_choice_answer(
            "1",
            question_type="single_choice",
            db_options=opts,
        )
        assert reason is None
        assert answer == "1"

    def test_multiple_choice_maps_labels(self):
        opts = _opts(("0", "Type 2 diabetes"), ("1", "Hypertension"), ("none", "None"))
        answer, reason = _normalize_pulled_choice_answer(
            ["Type 2 diabetes", "Hypertension"],
            question_type="multiple_choice",
            db_options=opts,
        )
        assert reason is None
        assert answer == ["0", "1"]

    def test_unmappable_returns_skip_reason(self):
        opts = _opts(("0", "Never"), ("1", "Rarely"))
        answer, reason = _normalize_pulled_choice_answer(
            "Sometimes maybe",
            question_type="single_choice",
            db_options=opts,
        )
        assert answer is None
        assert reason is not None
        assert reason.startswith("unmappable:")

    def test_non_choice_passthrough(self):
        answer, reason = _normalize_pulled_choice_answer(
            {"value": 7.0, "unit": "0"},
            question_type="scale",
            db_options=[],
        )
        assert reason is None
        assert answer == {"value": 7.0, "unit": "0"}


def _build_sync_service(monkeypatch) -> MetsightsSyncService:
    monkeypatch.setattr(settings, "METSIGHTS_API_KEY", "test-key")
    return MetsightsSyncService(
        metsights_service=MetsightsService(client=MetsightsClient()),
        users_repository=UsersRepository(),
        engagements_service=get_engagements_service(),
        assessments_service=get_assessments_service(),
        platform_settings_service=get_platform_settings_service_readonly(),
        questionnaire_repository=QuestionnaireRepository(),
    )


@pytest.mark.asyncio
async def test_import_category_stores_option_value_not_display_label(test_db_session, monkeypatch):
    await _ensure_test_engagement(test_db_session)

    diet_cat = await _ensure_manual_metsights_category(
        test_db_session,
        category_id=99510,
        category_key="diet-lifestyle-parameters",
        display_name="Diet & Lifestyle",
        question_keys=["sleeping_hours"],
    )

    pkg_id = 99501
    aid = 99502
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="CHOICE_NORM_PKG",
            display_name="Choice Norm",
            assessment_type_code="2",
            status="active",
        )
    )
    test_db_session.add(
        AssessmentPackageCategory(package_id=pkg_id, category_id=int(diet_cat.category_id))
    )
    await test_db_session.commit()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=1,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id="MS-CHOICE-NORM-01",
        )
    )
    await test_db_session.commit()

    async def _fake_get_sub(self, *, record_id: str, resource: str, **kwargs):
        if resource == "diet-lifestyle-parameters":
            return {"sleeping_hours": "Between 7 to 9 hours", "is_complete": True}
        return None

    monkeypatch.setattr(MetsightsService, "get_record_subresource_or_none", _fake_get_sub)

    sync_service = _build_sync_service(monkeypatch)
    result = await sync_service.import_category_from_metsights(
        test_db_session,
        assessment_instance_id=aid,
        user_id=1,
        category_key="diet-lifestyle-parameters",
        category_of="metsights",
        reload=1,
    )
    assert result["responses_imported"] >= 1

    q_repo = QuestionnaireRepository()
    sleep_def = await q_repo.get_definition_by_key(test_db_session, question_key="sleeping_hours")
    assert sleep_def is not None
    row = (
        await test_db_session.execute(
            select(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == aid,
                QuestionnaireResponse.question_id == int(sleep_def.question_id),
            )
        )
    ).scalar_one()
    assert row.answer == "2"


@pytest.mark.asyncio
async def test_normalize_choice_answers_job_converts_labels(test_db_session):
    await _ensure_test_engagement(test_db_session)

    q_repo = QuestionnaireRepository()
    sleep_def = await q_repo.get_definition_by_key(test_db_session, question_key="sleeping_hours")
    salt_def = await q_repo.get_definition_by_key(test_db_session, question_key="extra_salt_frequency")
    water_def = await q_repo.get_definition_by_key(test_db_session, question_key="water_intake_frequency")
    assert sleep_def is not None and salt_def is not None and water_def is not None

    user_id = 99520
    aid = 99521
    phone = "9952099520"
    test_db_session.add(
        User(user_id=user_id, age=30, phone=phone, status="active")
    )
    test_db_session.add(
        AssessmentPackage(
            package_id=99522,
            package_code="CHOICE_NORM_JOB",
            display_name="Choice Norm Job",
            assessment_type_code="2",
            status="active",
        )
    )
    await test_db_session.commit()
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=user_id,
            package_id=99522,
            engagement_id=1,
            status="active",
            metsights_record_id="MS-CHOICE-JOB-01",
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=int(sleep_def.question_id),
            category_ids=[],
            answer="Between 7 to 9 hours",
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=int(salt_def.question_id),
            category_ids=[],
            answer="0",
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=int(water_def.question_id),
            category_ids=[],
            answer="not-a-real-option",
        )
    )
    await test_db_session.commit()

    dry = await normalize_questionnaire_choice_answers(
        test_db_session,
        dry_run=True,
        assessment_instance_id=aid,
    )
    assert dry.updated >= 1
    assert dry.unmatched >= 1
    sleep_row = (
        await test_db_session.execute(
            select(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == aid,
                QuestionnaireResponse.question_id == int(sleep_def.question_id),
            )
        )
    ).scalar_one()
    assert sleep_row.answer == "Between 7 to 9 hours"

    applied = await normalize_questionnaire_choice_answers(
        test_db_session,
        dry_run=False,
        phone=phone,
    )
    await test_db_session.commit()
    assert applied.updated >= 1

    sleep_row = (
        await test_db_session.execute(
            select(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == aid,
                QuestionnaireResponse.question_id == int(sleep_def.question_id),
            )
        )
    ).scalar_one()
    assert sleep_row.answer == "2"

    salt_row = (
        await test_db_session.execute(
            select(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == aid,
                QuestionnaireResponse.question_id == int(salt_def.question_id),
            )
        )
    ).scalar_one()
    assert salt_row.answer == "0"

    water_row = (
        await test_db_session.execute(
            select(QuestionnaireResponse).where(
                QuestionnaireResponse.assessment_instance_id == aid,
                QuestionnaireResponse.question_id == int(water_def.question_id),
            )
        )
    ).scalar_one()
    assert water_row.answer == "not-a-real-option"
