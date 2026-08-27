"""Tests for copy_responses_from_previous_instance exclusion rules."""

from __future__ import annotations

import pytest
from sqlalchemy import select, text

from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.engagements.models import Engagement, EngagementType
from modules.diagnostics.models import DiagnosticPackage
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireResponse,
)
from modules.questionnaire.repository import QuestionnaireRepository
from modules.questionnaire.service import QuestionnaireService
from modules.users.repository import UsersRepository


async def _seed_package(test_db_session, *, package_id: int):
    test_db_session.add(
        AssessmentPackage(
            package_id=package_id,
            package_code=f"COPY_PREV_{package_id}",
            display_name="Copy Prev Package",
            status="active",
            assessment_type_code="1",
        )
    )
    await test_db_session.commit()


async def _seed_category_question_for_package(
    test_db_session,
    *,
    package_id: int,
    category_id: int,
    category_key: str,
    question_id: int,
    question_key: str,
    mapping_id: int,
):
    existing = (
        await test_db_session.execute(
            text(
                "SELECT category_id FROM questionnaire_categories "
                "WHERE category_key = :key LIMIT 1"
            ),
            {"key": category_key},
        )
    ).scalar_one_or_none()
    if existing is None:
        test_db_session.add(
            QuestionnaireCategory(
                category_id=category_id,
                category_key=category_key,
                display_name=category_key,
                status="active",
            )
        )
        category_id_to_use = category_id
    else:
        category_id_to_use = int(existing)

    test_db_session.add(
        QuestionnaireDefinition(
            question_id=question_id,
            question_key=question_key,
            question_text=f"Question {question_id}",
            question_type="text",
            is_required=True,
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        AssessmentPackageCategory(package_id=package_id, category_id=category_id_to_use)
    )
    test_db_session.add(
        QuestionnaireCategoryQuestion(
            id=mapping_id,
            category_id=category_id_to_use,
            question_id=question_id,
        )
    )
    await test_db_session.commit()
    return category_id_to_use


async def _seed_engagement(test_db_session, *, engagement_id: int, package_id: int):
    diag_result = await test_db_session.execute(
        select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == 1)
    )
    if diag_result.scalar_one_or_none() is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=1,
                reference_id="TEST_DIAG",
                package_name="Test Diagnostic",
                diagnostic_provider="Test Provider",
                status="active",
            )
        )

    engagement_type_id = (
        await test_db_session.execute(
            select(EngagementType.id).where(EngagementType.code == "bio_ai").limit(1)
        )
    ).scalar_one_or_none()

    existing = await test_db_session.execute(
        select(Engagement).where(Engagement.engagement_id == engagement_id)
    )
    if existing.scalar_one_or_none() is None:
        test_db_session.add(
            Engagement(
                engagement_id=engagement_id,
                engagement_name="Copy Prev Engagement",
                engagement_code=f"COPYPREV{engagement_id}",
                engagement_type=engagement_type_id,
                assessment_package_id=package_id,
                diagnostic_package_id=1,
                slot_duration=20,
                status="active",
            )
        )
        await test_db_session.commit()


@pytest.mark.asyncio
async def test_copy_responses_skips_vitals_and_blood_categories(test_db_session):
    package_id = 9201
    engagement_id = 9201
    user_id = 92001
    await _seed_package(test_db_session, package_id=package_id)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, package_id=package_id)

    allowed_cat_id = await _seed_category_question_for_package(
        test_db_session,
        package_id=package_id,
        category_id=9201,
        category_key="diet-lifestyle-parameters",
        question_id=9201,
        question_key="copy_prev_allowed_q",
        mapping_id=9201,
    )
    vitals_cat_id = await _seed_category_question_for_package(
        test_db_session,
        package_id=package_id,
        category_id=9202,
        category_key="vitals",
        question_id=9202,
        question_key="copy_prev_vitals_q",
        mapping_id=9202,
    )
    blood_cat_id = await _seed_category_question_for_package(
        test_db_session,
        package_id=package_id,
        category_id=9203,
        category_key="blood-parameters",
        question_id=9203,
        question_key="copy_prev_blood_q",
        mapping_id=9203,
    )
    adv_blood_cat_id = await _seed_category_question_for_package(
        test_db_session,
        package_id=package_id,
        category_id=9204,
        category_key="advanced-blood-parameters",
        question_id=9204,
        question_key="copy_prev_adv_blood_q",
        mapping_id=9204,
    )

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status) "
            "VALUES (:uid, 30, '9200100000', 'active') ON CONFLICT (user_id) DO NOTHING"
        ),
        {"uid": user_id},
    )

    test_db_session.add(
        AssessmentInstance(
            user_id=user_id,
            package_id=package_id,
            engagement_id=engagement_id,
            status="active",
        )
    )
    test_db_session.add(
        AssessmentInstance(
            user_id=user_id,
            package_id=package_id,
            engagement_id=engagement_id,
            status="active",
        )
    )
    await test_db_session.flush()

    instances = (
        await test_db_session.execute(
            text(
                "SELECT assessment_instance_id FROM assessment_instances "
                "WHERE user_id = :uid ORDER BY assessment_instance_id"
            ),
            {"uid": user_id},
        )
    ).all()
    source_instance_id = int(instances[0].assessment_instance_id)
    dest_instance_id = int(instances[1].assessment_instance_id)

    test_db_session.add_all(
        [
            QuestionnaireResponse(
                assessment_instance_id=source_instance_id,
                question_id=9201,
                category_ids=[allowed_cat_id],
                answer="allowed answer",
            ),
            QuestionnaireResponse(
                assessment_instance_id=source_instance_id,
                question_id=9202,
                category_ids=[vitals_cat_id],
                answer="vitals answer",
            ),
            QuestionnaireResponse(
                assessment_instance_id=source_instance_id,
                question_id=9203,
                category_ids=[blood_cat_id],
                answer="blood answer",
            ),
            QuestionnaireResponse(
                assessment_instance_id=source_instance_id,
                question_id=9204,
                category_ids=[adv_blood_cat_id],
                answer="adv blood answer",
            ),
        ]
    )
    await test_db_session.commit()

    service = QuestionnaireService(
        repository=QuestionnaireRepository(),
        users_repository=UsersRepository(),
        audit_service=AuditService(AuditRepository()),
    )
    copied = await service.copy_responses_from_previous_instance(
        test_db_session,
        user_id=user_id,
        source_assessment_instance_id=source_instance_id,
        dest_assessment_instance_id=dest_instance_id,
        ip_address="127.0.0.1",
        user_agent="test",
        endpoint="/test",
    )
    assert copied == 1

    rows = (
        await test_db_session.execute(
            text(
                "SELECT question_id, answer FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid ORDER BY question_id"
            ),
            {"aid": dest_instance_id},
        )
    ).all()
    assert len(rows) == 1
    assert int(rows[0].question_id) == 9201
    assert rows[0].answer == "allowed answer"
