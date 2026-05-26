"""Tests for questionnaire category progress backfill (employee)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.employee.models import Employee
from modules.engagements.models import Engagement
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireResponse,
)
from modules.users.models import User


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _ensure_test_engagement(test_db_session, *, engagement_id: int = 1):
    from sqlalchemy import select
    from modules.diagnostics.models import DiagnosticPackage

    existing = (
        await test_db_session.execute(select(Engagement).where(Engagement.engagement_id == engagement_id))
    ).scalar_one_or_none()
    if existing is not None:
        return

    pkg = (
        await test_db_session.execute(select(AssessmentPackage).where(AssessmentPackage.package_id == 1))
    ).scalar_one_or_none()
    if pkg is None:
        test_db_session.add(
            AssessmentPackage(package_id=1, package_code="TEST_PKG", display_name="Test Package", status="active")
        )

    diag = (
        await test_db_session.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == 1)
        )
    ).scalar_one_or_none()
    if diag is None:
        test_db_session.add(
            DiagnosticPackage(
                diagnostic_package_id=1,
                reference_id="TEST_DIAG",
                package_name="Test Diagnostic",
                diagnostic_provider="Test Provider",
                status="active",
            )
        )

    test_db_session.add(
        Engagement(
            engagement_id=engagement_id,
            engagement_name="Test Engagement",
            engagement_code="TEST001",
            engagement_type="bio_ai",
            assessment_package_id=1,
            diagnostic_package_id=1,
            slot_duration=20,
            status="active",
            participant_count=0,
        )
    )
    await test_db_session.commit()


@pytest.mark.asyncio
async def test_refresh_category_progress_requires_auth(async_client):
    response = await async_client.post("/platform-settings/questionnaire-category-progress/refresh-all")
    assert response.status_code == 401
    response_page = await async_client.post(
        "/platform-settings/questionnaire-category-progress/refresh-page",
        json={"offset": 0},
    )
    assert response_page.status_code == 401
    response_stats = await async_client.get("/platform-settings/questionnaire-category-progress/refresh-stats")
    assert response_stats.status_code == 401


@pytest.mark.asyncio
async def test_refresh_category_progress_marks_complete_for_existing_answers(async_client, test_db_session):
    uid = 9201
    test_db_session.add(User(user_id=uid, age=30, phone="92010000001", status="active"))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9201, user_id=uid, role="admin", status="active"))
    await _ensure_test_engagement(test_db_session)

    package = AssessmentPackage(package_id=9201, package_code="PKG9201", display_name="Pkg", status="active")
    category = QuestionnaireCategory(category_id=9201, category_key="cat_9201", display_name="Cat", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()

    q = QuestionnaireDefinition(
        question_id=9201,
        question_key="q9201",
        question_text="Required",
        question_type="text",
        is_required=True,
        status="active",
    )
    test_db_session.add(q)
    await test_db_session.commit()
    test_db_session.add(QuestionnaireCategoryQuestion(id=92001, category_id=9201, question_id=9201))
    test_db_session.add(AssessmentPackageCategory(package_id=9201, category_id=9201))
    instance = AssessmentInstance(
        assessment_instance_id=9201,
        user_id=uid,
        package_id=9201,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=9201,
            question_id=9201,
            category_id=9201,
            answer="legacy answer",
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    stats = await async_client.get(
        "/platform-settings/questionnaire-category-progress/refresh-stats",
        headers=_auth_header(uid),
    )
    assert stats.status_code == 200
    assert stats.json()["data"]["assessment_instances_total"] >= 1

    refresh = await async_client.post(
        "/platform-settings/questionnaire-category-progress/refresh-page",
        headers=_auth_header(uid),
        json={"offset": 0},
    )
    assert refresh.status_code == 200
    page = refresh.json()["data"]
    assert page["processed"] == 1
    assert page["marked_complete"] >= 1
    assert page["has_more"] is False

    status = await async_client.get("/assessments/9201/status", headers=_auth_header(uid))
    assert status.status_code == 200
    assert status.json()["data"][0]["status"] == "complete"
