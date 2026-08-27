"""Regression tests for MissingGreenlet after release_request_transaction in Metsights sync."""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import select, text

from core.config import settings
from db.seed.metsights_sync_registry import build_metsights_sync
from modules.assessments.dependencies import get_assessments_service
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.engagements.dependencies import get_engagements_service
from modules.metsights.client import MetsightsClient
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.platform_settings.dependencies import get_platform_settings_service_readonly
from modules.questionnaire.models import QuestionnaireCategory, QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.users.repository import UsersRepository
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement


@pytest_asyncio.fixture(autouse=True)
async def _clean_missing_greenlet_rows(test_db_session):
    """Remove rows from prior runs when session teardown failed on category FK cleanup."""
    for sql in (
        "DELETE FROM questionnaire_responses WHERE assessment_instance_id IN (99402, 99404)",
        "DELETE FROM assessment_category_progress WHERE assessment_instance_id IN (99402, 99404)",
        "DELETE FROM assessment_instances WHERE assessment_instance_id IN (99402, 99404)",
        "DELETE FROM assessment_package_categories WHERE package_id IN (99401, 99403)",
        "DELETE FROM assessment_packages WHERE package_id IN (99401, 99403)",
    ):
        await test_db_session.execute(text(sql))
    await test_db_session.commit()
    yield


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


async def _ensure_manual_metsights_category(
    test_db_session,
    *,
    category_id: int,
    category_key: str,
    display_name: str,
    question_keys: list[str],
) -> QuestionnaireCategory:
    """Create a Metsights category for tests without touching seed package links."""
    q_repo = QuestionnaireRepository()

    category = (
        await test_db_session.execute(
            select(QuestionnaireCategory).where(
                QuestionnaireCategory.category_key == category_key,
                QuestionnaireCategory.category_of == "metsights",
            )
        )
    ).scalar_one_or_none()

    if category is None:
        category = QuestionnaireCategory(
            category_id=category_id,
            category_key=category_key,
            display_name=display_name,
            category_of="metsights",
            status="active",
        )
        test_db_session.add(category)
        await test_db_session.flush()

    question_ids: list[int] = []
    for question_key in question_keys:
        definition = await q_repo.get_definition_by_key(test_db_session, question_key=question_key)
        assert definition is not None, f"Missing seeded question {question_key!r}"
        definition.metsights_sync = build_metsights_sync(question_key)
        question_ids.append(int(definition.question_id))

    await q_repo.assign_questions_to_category(
        test_db_session,
        category_id=int(category.category_id),
        question_ids=question_ids,
    )
    await test_db_session.flush()
    return category


@pytest.mark.asyncio
async def test_import_category_from_metsights_survives_release_expire(test_db_session, monkeypatch):
    """Post-release import work must use scalar snapshots, not expired ORM attrs."""
    await _ensure_test_engagement(test_db_session)

    uid = 1
    diet_cat = await _ensure_manual_metsights_category(
        test_db_session,
        category_id=99410,
        category_key="diet-lifestyle-parameters",
        display_name="Diet & Lifestyle",
        question_keys=["living_region"],
    )

    pkg_id = 99401
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MG_IMPORT_PKG",
            display_name="MissingGreenlet Import",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(
        AssessmentPackageCategory(package_id=pkg_id, category_id=int(diet_cat.category_id))
    )
    await test_db_session.commit()

    aid = 99402
    rid = "MS-MG-IMPORT-01"
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=rid,
        )
    )
    await test_db_session.commit()

    async def _fake_get_sub(self, *, record_id: str, resource: str, **kwargs):
        if resource == "diet-lifestyle-parameters":
            return {"living_region": "1", "is_complete": True}
        return None

    monkeypatch.setattr(MetsightsService, "get_record_subresource_or_none", _fake_get_sub)

    sync_service = _build_sync_service(monkeypatch)
    result = await sync_service.import_category_from_metsights(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        category_key="diet-lifestyle-parameters",
        category_of="metsights",
        reload=1,
    )

    assert result["responses_imported"] >= 1


@pytest.mark.asyncio
async def test_push_category_to_metsights_survives_release_expire(test_db_session, monkeypatch):
    """Post-release push work must use scalar snapshots, not expired ORM attrs."""
    await _ensure_test_engagement(test_db_session)

    uid = 1
    phys_cat = await _ensure_manual_metsights_category(
        test_db_session,
        category_id=99411,
        category_key="physical-measurement",
        display_name="Physical Measurement",
        question_keys=["height", "weight"],
    )

    pkg_id = 99403
    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="MG_PUSH_PKG",
            display_name="MissingGreenlet Push",
            assessment_type_code="1",
            status="active",
        )
    )
    test_db_session.add(
        AssessmentPackageCategory(package_id=pkg_id, category_id=int(phys_cat.category_id))
    )
    await test_db_session.commit()

    aid = 99404
    rid = "MS-MG-PUSH-01"
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id=rid,
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=1,
            category_ids=[int(phys_cat.category_id)],
            answer={"value": 175.0, "unit": "0"},
        )
    )
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=aid,
            question_id=2,
            category_ids=[int(phys_cat.category_id)],
            answer={"value": 70.0, "unit": "0"},
        )
    )
    await test_db_session.commit()

    async def _fake_detail(self, *, record_id: str, **kwargs):
        return {"id": record_id, "assessment_code": "MET_BASIC", "assessment_type": "MetSights Basic"}

    async def _fake_options(self, *, record_id: str, resource: str, **kwargs):
        return {}

    async def _fake_upsert(self, *, record_id: str, resource: str, body: dict, **kwargs):
        return {}

    monkeypatch.setattr(MetsightsService, "get_record_detail", _fake_detail)
    monkeypatch.setattr(MetsightsService, "options_record_subresource", _fake_options)
    monkeypatch.setattr(MetsightsService, "upsert_record_subresource", _fake_upsert)

    sync_service = _build_sync_service(monkeypatch)
    result = await sync_service._push_category_to_metsights(
        test_db_session,
        assessment_instance_id=aid,
        user_id=uid,
        category_key="physical-measurement",
        category_of="metsights",
    )

    assert result["status"] == "success"
    assert "height" in result["fields_pushed"]
    assert "weight" in result["fields_pushed"]
