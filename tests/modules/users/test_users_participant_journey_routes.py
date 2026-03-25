"""Tests for employee participant journey (assessment + questionnaire read APIs)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.employee.models import Employee
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.diagnostics.models import DiagnosticPackage
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


@pytest.mark.asyncio
async def test_participant_journey_requires_employee(async_client, test_db_session):
    test_db_session.add(User(user_id=9600, phone="96000000000", status="active", age=30))
    await test_db_session.commit()

    response = await async_client.get("/users/9600/participant-journey", headers=_auth_header(9600))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_participant_journey_summary_user_not_found(async_client, test_db_session):
    test_db_session.add(User(user_id=9601, phone="96010000000", status="active", age=30))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9601, user_id=9601, role="admin", status="active"))
    await test_db_session.commit()

    response = await async_client.get("/users/999999/participant-journey", headers=_auth_header(9601))
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_participant_journey_summary_invalid_pagination(async_client, test_db_session):
    test_db_session.add(User(user_id=9602, phone="96020000000", status="active", age=30))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9602, user_id=9602, role="admin", status="active"))
    test_db_session.add(User(user_id=9603, phone="96030000000", status="active", age=30))
    await test_db_session.commit()

    response = await async_client.get("/users/9603/participant-journey?page=0", headers=_auth_header(9602))
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_participant_journey_summary_returns_instance_and_counts(async_client, test_db_session):
    test_db_session.add(User(user_id=9610, phone="96100000000", status="active", age=30))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9610, user_id=9610, role="ops", status="active"))

    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package",
            diagnostic_provider="test_provider",
            no_of_tests=1,
            status="active",
            bookings_count=0,
        )
    )

    test_db_session.add(User(user_id=9611, phone="96110000000", status="active", age=25))
    # Flush to ensure `users` row exists before inserting `assessment_instances`.
    await test_db_session.flush()
    test_db_session.add(
        Organization(organization_id=9611, name="O1", organization_type="corporate", status="active")
    )
    test_db_session.add(
        AssessmentPackage(package_id=9611, package_code="PJ1", display_name="PJ Package", status="active")
    )
    # Flush FK parents before inserting Engagement (depends on organization_id + assessment_package_id).
    await test_db_session.flush()
    test_db_session.add(
        Engagement(
            engagement_id=9611,
            engagement_name="Camp 9611",
            organization_id=9611,
            engagement_code="PJ9611",
            engagement_type="b2b",
            assessment_package_id=9611,
            diagnostic_package_id=1,
            city="Pune",
            slot_duration=30,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
            status="active",
            participant_count=1,
        )
    )
    # Flush dependent rows so FKs exist before inserting AssessmentInstance.
    await test_db_session.flush()
    inst = AssessmentInstance(
        user_id=9611,
        package_id=9611,
        engagement_id=9611,
        status="active",
        assigned_at=datetime.now(timezone.utc),
        completed_at=None,
    )
    test_db_session.add(inst)
    await test_db_session.flush()

    test_db_session.add(
        QuestionnaireCategory(category_id=9611, category_key="cat_pj", display_name="Cat PJ", status="active")
    )
    # Flush so questionnaire_categories rows exist before assessment_package_categories.
    await test_db_session.flush()
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=9611,
            question_key="q_pj",
            question_text="Q text",
            question_type="text",
            status="active",
        )
    )
    # Flush so questionnaire_definitions exists before linking rows.
    await test_db_session.flush()
    test_db_session.add(QuestionnaireCategoryQuestion(category_id=9611, question_id=9611, display_order=1))
    test_db_session.add(AssessmentPackageCategory(package_id=9611, category_id=9611, display_order=1))
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=inst.assessment_instance_id,
            question_id=9611,
            category_id=9611,
            answer={"v": "draft"},
            submitted_at=None,
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=9612,
            question_key="q_pj_b",
            question_text="Q b",
            question_type="text",
            status="active",
        )
    )
    # Flush so questionnaire_definitions exists before linking rows.
    await test_db_session.flush()
    test_db_session.add(QuestionnaireCategoryQuestion(category_id=9611, question_id=9612, display_order=2))
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=inst.assessment_instance_id,
            question_id=9612,
            category_id=9611,
            answer={"v": "done"},
            submitted_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/users/9611/participant-journey", headers=_auth_header(9610))
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == 1
    instances = body["data"]["instances"]
    assert len(instances) == 1
    row = instances[0]
    assert row["assessment_instance_id"] == inst.assessment_instance_id
    assert row["engagement_code"] == "PJ9611"
    assert row["package_code"] == "PJ1"
    q = row["questionnaire"]
    assert q["response_count"] == 2
    assert q["draft_count"] == 1
    assert q["submitted_count"] == 1
    assert q["categories_touched"] == 1


@pytest.mark.asyncio
async def test_participant_journey_detail_not_found_for_wrong_user(async_client, test_db_session):
    test_db_session.add(User(user_id=9620, phone="96200000000", status="active", age=30))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9620, user_id=9620, role="admin", status="active"))
    test_db_session.add(User(user_id=9621, phone="96210000000", status="active", age=30))
    test_db_session.add(User(user_id=9622, phone="96220000000", status="active", age=30))
    # Flush so FK parent rows exist before inserting AssessmentInstance.
    await test_db_session.flush()
    test_db_session.add(
        Organization(organization_id=9621, name="O2", organization_type="corporate", status="active")
    )
    test_db_session.add(
        AssessmentPackage(package_id=9621, package_code="PJ2", display_name="PJ2", status="active")
    )
    # Flush FK parents before inserting Engagement/AssessmentInstance.
    await test_db_session.flush()

    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package",
            diagnostic_provider="test_provider",
            no_of_tests=1,
            status="active",
            bookings_count=0,
        )
    )

    test_db_session.add(
        Engagement(
            engagement_id=9621,
            engagement_name="E2",
            organization_id=9621,
            engagement_code="PJ9621",
            engagement_type="b2b",
            assessment_package_id=9621,
            diagnostic_package_id=1,
            city="Pune",
            slot_duration=30,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
            status="active",
            participant_count=0,
        )
    )
    # Flush so engagement exists before inserting AssessmentInstance.
    await test_db_session.flush()
    inst = AssessmentInstance(
        user_id=9621,
        package_id=9621,
        engagement_id=9621,
        status="active",
        assigned_at=datetime.now(timezone.utc),
        completed_at=None,
    )
    test_db_session.add(inst)
    await test_db_session.commit()

    url = f"/users/9622/participant-journey/{inst.assessment_instance_id}"
    response = await async_client.get(url, headers=_auth_header(9620))
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_participant_journey_detail_returns_categories_and_answer_state(async_client, test_db_session):
    test_db_session.add(User(user_id=9630, phone="96300000000", status="active", age=30))
    await test_db_session.flush()
    test_db_session.add(Employee(employee_id=9630, user_id=9630, role="admin", status="active"))
    test_db_session.add(User(user_id=9631, phone="96310000000", status="active", age=30))
    # Flush so `users` row exists before inserting AssessmentInstance.
    await test_db_session.flush()
    test_db_session.add(
        Organization(organization_id=9631, name="O3", organization_type="corporate", status="active")
    )
    test_db_session.add(
        AssessmentPackage(package_id=9631, package_code="PJ3", display_name="PJ3", status="active")
    )
    # Flush FK parents so AssessmentInstance insert doesn't violate package FK.
    await test_db_session.flush()

    test_db_session.add(
        DiagnosticPackage(
            diagnostic_package_id=1,
            reference_id="REF1",
            package_name="Diag Package",
            diagnostic_provider="test_provider",
            no_of_tests=1,
            status="active",
            bookings_count=0,
        )
    )

    test_db_session.add(
        Engagement(
            engagement_id=9631,
            engagement_name="E3",
            organization_id=9631,
            engagement_code="PJ9631",
            engagement_type="b2b",
            assessment_package_id=9631,
            diagnostic_package_id=1,
            city="Pune",
            slot_duration=30,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
            status="active",
            participant_count=0,
        )
    )
    # Flush engagement FK parents before inserting AssessmentInstance.
    await test_db_session.flush()
    inst = AssessmentInstance(
        user_id=9631,
        package_id=9631,
        engagement_id=9631,
        status="active",
        assigned_at=datetime.now(timezone.utc),
        completed_at=None,
    )
    test_db_session.add(inst)
    await test_db_session.flush()

    test_db_session.add(
        QuestionnaireCategory(category_id=9631, category_key="cat3", display_name="Cat 3", status="active")
    )
    # Flush questionnaire_categories before inserting assessment_package_categories.
    await test_db_session.flush()
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=9631,
            question_key="q3",
            question_text="Question three",
            question_type="text",
            status="active",
        )
    )
    # Flush so questionnaire_definitions exists before linking rows.
    await test_db_session.flush()
    test_db_session.add(QuestionnaireCategoryQuestion(category_id=9631, question_id=9631, display_order=1))
    test_db_session.add(AssessmentPackageCategory(package_id=9631, category_id=9631, display_order=1))
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=inst.assessment_instance_id,
            question_id=9631,
            category_id=9631,
            answer="hello",
            submitted_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    url = f"/users/9631/participant-journey/{inst.assessment_instance_id}"
    response = await async_client.get(url, headers=_auth_header(9630))
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["user_id"] == 9631
    assert data["package"]["package_code"] == "PJ3"
    assert len(data["categories"]) == 1
    qs = data["categories"][0]["questions"]
    assert len(qs) == 1
    assert qs[0]["answer_state"] == "submitted"
    assert qs[0]["answer"] == "hello"
