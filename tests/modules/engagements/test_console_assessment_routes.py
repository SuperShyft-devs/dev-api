"""Tests for console participant assessment/questionnaire proxy routes."""

from __future__ import annotations

from datetime import date, time, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import (
    AssessmentInstance,
    AssessmentPackage,
    AssessmentPackageCategory,
)
from modules.engagements.models import Engagement, EngagementParticipant
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
)
from modules.users.models import User

from tests.modules.engagements.test_console_routes import (
    _assign_assistant,
    _seed_employee,
    _seed_engagement,
)


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_participant_with_assessment(
    test_db_session,
    *,
    engagement_id: int,
    participant_user_id: int,
    assessment_instance_id: int,
    package_id: int = 8801,
    category_id: int = 8802,
):
    package = await test_db_session.get(AssessmentPackage, package_id)
    if package is None:
        test_db_session.add(
            AssessmentPackage(
                package_id=package_id,
                package_code="METSIGHTS_PRO",
                display_name="MetSights Pro",
                assessment_type_code="2",
                status="active",
            )
        )

    category = await test_db_session.get(QuestionnaireCategory, category_id)
    if category is None:
        test_db_session.add(
            QuestionnaireCategory(
                category_id=category_id,
                category_key="physical-measurement",
                display_name="Physical Measurement",
                category_of="metsights",
                status="active",
            )
        )

    question = await test_db_session.get(QuestionnaireDefinition, 8803)
    if question is None:
        test_db_session.add(
            QuestionnaireDefinition(
                question_id=8803,
                question_key="q_height",
                question_text="What is your height?",
                question_type="text",
                status="active",
            )
        )

    test_db_session.add(
        User(
            user_id=participant_user_id,
            age=30,
            phone=f"{participant_user_id}000000000",
            status="active",
            first_name="Pat",
            last_name="Test",
        )
    )
    await test_db_session.flush()

    participant = await test_db_session.get(EngagementParticipant, 88001)
    if participant is None:
        test_db_session.add(
            EngagementParticipant(
                engagement_participant_id=88001,
                engagement_id=engagement_id,
                user_id=participant_user_id,
                booked_by_user_id=participant_user_id,
                engagement_date=date.today(),
                slot_start_time=time(10, 0),
            )
        )

    instance = await test_db_session.get(AssessmentInstance, assessment_instance_id)
    if instance is None:
        test_db_session.add(
            AssessmentInstance(
                assessment_instance_id=assessment_instance_id,
                user_id=participant_user_id,
                package_id=package_id,
                engagement_id=engagement_id,
                status="active",
            )
        )

    link = await test_db_session.get(AssessmentPackageCategory, 8804)
    if link is None:
        test_db_session.add(AssessmentPackageCategory(id=8804, package_id=package_id, category_id=category_id))

    mapping = await test_db_session.get(QuestionnaireCategoryQuestion, 8805)
    if mapping is None:
        test_db_session.add(
            QuestionnaireCategoryQuestion(id=8805, category_id=category_id, question_id=8803)
        )

    await test_db_session.commit()


@pytest.mark.asyncio
async def test_console_list_participant_assessments_success(async_client, test_db_session):
    engagement_id = 8800
    admin_user_id = 88010
    participant_user_id = 88020
    assessment_instance_id = 88030

    await _seed_employee(test_db_session, user_id=admin_user_id, employee_id=880)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="running")
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
    )

    response = await async_client.get(
        f"/engagements/{engagement_id}/console/participants/{participant_user_id}/assessments",
        headers=_auth_header(admin_user_id),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 1
    assert data[0]["assessment_instance_id"] == assessment_instance_id
    assert data[0]["engagement_id"] == engagement_id
    assert data[0]["package_code"] == "METSIGHTS_PRO"


@pytest.mark.asyncio
async def test_console_assessment_routes_forbidden_for_unassigned_oa(async_client, test_db_session):
    engagement_id = 8801
    oa_user_id = 88011
    participant_user_id = 88021
    assessment_instance_id = 88031

    await _seed_employee(test_db_session, user_id=oa_user_id, employee_id=881, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="running")
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
        package_id=8806,
        category_id=8807,
    )

    base = f"/engagements/{engagement_id}/console/participants/{participant_user_id}"
    headers = _auth_header(oa_user_id)

    for path in [
        f"{base}/assessments",
        f"{base}/assessments/{assessment_instance_id}/status",
        f"{base}/questionnaire/{assessment_instance_id}/category/8807",
    ]:
        response = await async_client.get(path, headers=headers)
        assert response.status_code == 403
        assert response.json()["error_code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_console_assessment_status_wrong_engagement_returns_404(async_client, test_db_session):
    engagement_id = 8802
    other_engagement_id = 8803
    admin_user_id = 88012
    participant_user_id = 88022
    assessment_instance_id = 88032

    await _seed_employee(test_db_session, user_id=admin_user_id, employee_id=882)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="running")
    await _seed_engagement(test_db_session, engagement_id=other_engagement_id, status="running")
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
        package_id=8808,
        category_id=8809,
    )

    response = await async_client.get(
        f"/engagements/{other_engagement_id}/console/participants/{participant_user_id}/assessments/{assessment_instance_id}/status",
        headers=_auth_header(admin_user_id),
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "ASSESSMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_console_get_questionnaire_success(async_client, test_db_session):
    engagement_id = 8804
    admin_user_id = 88013
    participant_user_id = 88023
    assessment_instance_id = 88033
    category_id = 8810

    await _seed_employee(test_db_session, user_id=admin_user_id, employee_id=883)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="running")
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
        package_id=8811,
        category_id=category_id,
    )

    response = await async_client.get(
        f"/engagements/{engagement_id}/console/participants/{participant_user_id}/questionnaire/{assessment_instance_id}/category/{category_id}",
        headers=_auth_header(admin_user_id),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["assessment_instance_id"] == assessment_instance_id
    assert len(data["questions"]) == 1
    assert data["questions"][0]["question_id"] == 8803


@pytest.mark.asyncio
async def test_console_upsert_responses_requires_running_engagement(async_client, test_db_session):
    engagement_id = 8805
    admin_user_id = 88014
    participant_user_id = 88024
    assessment_instance_id = 88034
    category_id = 8812

    await _seed_employee(test_db_session, user_id=admin_user_id, employee_id=884)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="completed")
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
        package_id=8813,
        category_id=category_id,
    )

    response = await async_client.put(
        f"/engagements/{engagement_id}/console/participants/{participant_user_id}/questionnaire/{assessment_instance_id}/category/{category_id}/responses",
        headers=_auth_header(admin_user_id),
        json={"responses": [{"question_id": 8803, "answer": "170"}]},
    )
    assert response.status_code == 422
    assert response.json()["error_code"] == "ENGAGEMENT_NOT_RUNNING"


@pytest.mark.asyncio
async def test_console_upsert_responses_success(async_client, test_db_session):
    engagement_id = 8806
    oa_user_id = 88015
    participant_user_id = 88025
    assessment_instance_id = 88035
    category_id = 8814

    await _seed_employee(test_db_session, user_id=oa_user_id, employee_id=885, role="onboarding_assistant")
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="running")
    await _assign_assistant(
        test_db_session,
        assignment_id=88050,
        employee_id=885,
        engagement_id=engagement_id,
    )
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
        package_id=8815,
        category_id=category_id,
    )

    response = await async_client.put(
        f"/engagements/{engagement_id}/console/participants/{participant_user_id}/questionnaire/{assessment_instance_id}/category/{category_id}/responses",
        headers=_auth_header(oa_user_id),
        json={"responses": [{"question_id": 8803, "answer": "170"}]},
    )
    assert response.status_code == 200
    assert response.json()["data"]["message"] == "Responses saved successfully"


@pytest.mark.asyncio
async def test_console_submit_assessment_success(async_client, test_db_session):
    engagement_id = 8807
    admin_user_id = 88016
    participant_user_id = 88026
    assessment_instance_id = 88036
    category_id = 8816

    await _seed_employee(test_db_session, user_id=admin_user_id, employee_id=886)
    await _seed_engagement(test_db_session, engagement_id=engagement_id, status="running")
    await _seed_participant_with_assessment(
        test_db_session,
        engagement_id=engagement_id,
        participant_user_id=participant_user_id,
        assessment_instance_id=assessment_instance_id,
        package_id=8817,
        category_id=category_id,
    )

    mock_submit = AsyncMock(return_value={"status": "ok", "category": "physical-measurement"})
    with patch(
        "modules.metsights.sync_service.MetsightsSyncService.submit_category_to_metsights",
        mock_submit,
    ):
        response = await async_client.post(
            f"/engagements/{engagement_id}/console/participants/{participant_user_id}/assessments/{assessment_instance_id}/submit",
            headers=_auth_header(admin_user_id),
            json={"category": "physical-measurement", "category_of": "metsights"},
        )

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "ok"
    mock_submit.assert_awaited_once()
