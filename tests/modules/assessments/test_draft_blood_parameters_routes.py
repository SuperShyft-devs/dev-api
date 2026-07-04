"""Tests for POST /assessments/{id}/metsights/draft-blood-parameters."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.reports.blood_parameters_read_service import build_parameter_value_map
from modules.reports.models import IndividualHealthReport
from tests.modules.questionnaire.test_questionnaire_user_routes import _ensure_test_engagement, _seed_user


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_blood_category(
    test_db_session,
    *,
    category_id: int,
    category_key: str,
    question_id: int,
    question_key: str,
    option_id: int,
    mapping_id: int,
    unit_code: str = "0",
    unit_display: str = "mg/dL",
    unitless: bool = False,
):
    test_db_session.add(
        QuestionnaireCategory(
            category_id=category_id,
            category_key=category_key,
            display_name=category_key,
            category_of="metsights",
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=question_id,
            question_key=question_key,
            question_text=question_key,
            question_type="scale",
            is_required=False,
            status="active",
        )
    )
    await test_db_session.flush()
    if not unitless:
        test_db_session.add(
            QuestionnaireOption(
                option_id=option_id,
                question_id=question_id,
                option_value=unit_code,
                display_name=unit_display,
            )
        )
    else:
        test_db_session.add(
            QuestionnaireOption(
                option_id=option_id,
                question_id=question_id,
                option_value="0",
                display_name="ratio",
            )
        )
    test_db_session.add(
        QuestionnaireCategoryQuestion(
            id=mapping_id,
            category_id=category_id,
            question_id=question_id,
            display_order=1,
        )
    )


async def _seed_basic_setup(
    test_db_session,
    *,
    user_id: int,
    package_id: int,
    assessment_id: int,
    category_id: int = 77001,
    question_id: int = 77001,
    option_id: int = 77001,
    mapping_id: int = 77001,
    package_code: str = "METSIGHTS_BASIC",
    metsights_record_id: str | None = "MS-DRAFT-01",
    status: str = "active",
    link_blood_category: bool = True,
    blood_parameters=None,
    report_id: int = 77001,
):
    await _ensure_test_engagement(test_db_session)
    await _seed_user(test_db_session, user_id=user_id)

    test_db_session.add(
        AssessmentPackage(
            package_id=package_id,
            package_code=package_code,
            display_name=package_code,
            assessment_type_code="1" if package_code == "METSIGHTS_BASIC" else "2",
            status="active",
        )
    )
    await test_db_session.flush()

    await _seed_blood_category(
        test_db_session,
        category_id=category_id,
        category_key="blood-parameters",
        question_id=question_id,
        question_key="glucose_fasting",
        option_id=option_id,
        mapping_id=mapping_id,
        unit_code="0",
        unit_display="mg/dL",
    )

    if link_blood_category:
        test_db_session.add(AssessmentPackageCategory(package_id=package_id, category_id=category_id, display_order=1))

    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=assessment_id,
            user_id=user_id,
            package_id=package_id,
            engagement_id=1,
            status=status,
            metsights_record_id=metsights_record_id,
        )
    )
    await test_db_session.flush()

    if blood_parameters is not None:
        test_db_session.add(
            IndividualHealthReport(
                report_id=report_id,
                user_id=user_id,
                engagement_id=1,
                assessment_instance_id=assessment_id,
                blood_parameters=blood_parameters,
            )
        )
    await test_db_session.commit()


def test_build_parameter_value_map_grouped():
    blob = [
        {
            "group_name": "Metabolic",
            "test_count": 1,
            "tests": [
                {
                    "test_id": 1,
                    "parameter_key": "glucose_fasting",
                    "value": 91.0,
                    "unit": "mg/dL",
                }
            ],
        }
    ]
    assert build_parameter_value_map(blob) == {"glucose_fasting": (91.0, "mg/dL")}


def test_build_parameter_value_map_canonical():
    blob = {
        "source": "healthians",
        "ingested_at": "2026-01-01",
        "parameters": {"glucose_fasting": {"value": 88.0, "unit": "mg/dL"}},
    }
    assert build_parameter_value_map(blob) == {"glucose_fasting": (88.0, "mg/dL")}


def test_build_parameter_value_map_legacy_flat():
    blob = {"glucose_fasting": 90.0, "glucose_fasting_unit": "0", "is_complete": True}
    assert build_parameter_value_map(blob) == {"glucose_fasting": (90.0, "0")}


@pytest.mark.asyncio
async def test_draft_blood_parameters_requires_auth(async_client):
    response = await async_client.post("/assessments/1/metsights/draft-blood-parameters")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_draft_blood_parameters_not_found_for_other_user(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66001,
        package_id=66001,
        assessment_id=66001,
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 91.0, "unit": "mg/dL"}],
            }
        ],
    )
    await _seed_user(test_db_session, user_id=66002)

    response = await async_client.post(
        "/assessments/66001/metsights/draft-blood-parameters",
        headers=_auth_header(66002),
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_draft_blood_parameters_rejects_non_metsights_package(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66003,
        package_id=66003,
        assessment_id=66003,
        package_code="MY_FITNESS_PRINT",
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 91.0, "unit": "mg/dL"}],
            }
        ],
    )

    response = await async_client.post(
        "/assessments/66003/metsights/draft-blood-parameters",
        headers=_auth_header(66003),
    )
    assert response.status_code == 422
    assert "not eligible" in response.json()["message"]


@pytest.mark.asyncio
async def test_draft_blood_parameters_requires_package_category_link(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66004,
        package_id=66004,
        assessment_id=66004,
        link_blood_category=False,
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 91.0, "unit": "mg/dL"}],
            }
        ],
    )

    response = await async_client.post(
        "/assessments/66004/metsights/draft-blood-parameters",
        headers=_auth_header(66004),
    )
    assert response.status_code == 422
    assert "not linked" in response.json()["message"]


@pytest.mark.asyncio
async def test_draft_blood_parameters_requires_metsights_record_id(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66005,
        package_id=66005,
        assessment_id=66005,
        metsights_record_id=None,
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 91.0, "unit": "mg/dL"}],
            }
        ],
    )

    response = await async_client.post(
        "/assessments/66005/metsights/draft-blood-parameters",
        headers=_auth_header(66005),
    )
    assert response.status_code == 422
    assert "Metsights record id" in response.json()["message"]


@pytest.mark.asyncio
async def test_draft_blood_parameters_requires_blood_report(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66006,
        package_id=66006,
        assessment_id=66006,
        blood_parameters=None,
    )

    response = await async_client.post(
        "/assessments/66006/metsights/draft-blood-parameters",
        headers=_auth_header(66006),
    )
    assert response.status_code == 422
    assert "Blood parameters report" in response.json()["message"]


@pytest.mark.asyncio
async def test_draft_blood_parameters_rejects_completed_assessment(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66007,
        package_id=66007,
        assessment_id=66007,
        status="completed",
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 91.0, "unit": "mg/dL"}],
            }
        ],
    )

    response = await async_client.post(
        "/assessments/66007/metsights/draft-blood-parameters",
        headers=_auth_header(66007),
    )
    assert response.status_code == 422
    assert "already completed" in response.json()["message"]


@pytest.mark.asyncio
async def test_draft_blood_parameters_basic_writes_draft_responses(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66008,
        package_id=66008,
        assessment_id=66008,
        category_id=77008,
        question_id=77008,
        option_id=77008,
        mapping_id=77008,
        report_id=77008,
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [
                    {
                        "test_id": 1,
                        "parameter_key": "glucose_fasting",
                        "value": 91.5,
                        "unit": "mg/dL",
                    }
                ],
            }
        ],
    )

    response = await async_client.post(
        "/assessments/66008/metsights/draft-blood-parameters",
        headers=_auth_header(66008),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["assessment_instance_id"] == 66008
    assert body["package_code"] == "METSIGHTS_BASIC"
    assert body["responses_drafted"] == 1
    assert body["categories"][0]["category"] == "blood-parameters"
    assert body["categories"][0]["responses_drafted"] == 1

    result = await test_db_session.execute(
        select(QuestionnaireResponse).where(QuestionnaireResponse.assessment_instance_id == 66008)
    )
    rows = list(result.scalars().all())
    assert len(rows) == 1
    assert rows[0].category_id == 77008
    assert rows[0].question_id == 77008
    assert rows[0].answer == {"value": 91.5, "unit": "0"}
    assert rows[0].submitted_at is None


@pytest.mark.asyncio
async def test_draft_blood_parameters_overwrites_existing_as_draft(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66009,
        package_id=66009,
        assessment_id=66009,
        category_id=77009,
        question_id=77009,
        option_id=77009,
        mapping_id=77009,
        report_id=77009,
        blood_parameters=[
            {
                "group_name": "Metabolic",
                "test_count": 1,
                "tests": [{"parameter_key": "glucose_fasting", "value": 100.0, "unit": "mg/dL"}],
            }
        ],
    )
    from datetime import datetime, timezone

    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=66009,
            question_id=77009,
            category_id=77009,
            answer={"value": 50.0, "unit": "0"},
            submitted_at=datetime.now(timezone.utc),
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/assessments/66009/metsights/draft-blood-parameters",
        headers=_auth_header(66009),
    )
    assert response.status_code == 200
    assert response.json()["data"]["responses_drafted"] == 1

    result = await test_db_session.execute(
        select(QuestionnaireResponse).where(QuestionnaireResponse.assessment_instance_id == 66009)
    )
    row = result.scalar_one()
    assert row.answer == {"value": 100.0, "unit": "0"}
    assert row.submitted_at is None


@pytest.mark.asyncio
async def test_draft_blood_parameters_pro_drafts_both_categories(async_client, test_db_session):
    await _ensure_test_engagement(test_db_session)
    uid = 66010
    await _seed_user(test_db_session, user_id=uid)

    pkg_id = 66010
    aid = 66010
    blood_cat_id = 77010
    adv_cat_id = 77011
    blood_qid = 77010
    adv_qid = 77011

    test_db_session.add(
        AssessmentPackage(
            package_id=pkg_id,
            package_code="METSIGHTS_PRO",
            display_name="Metsights Pro",
            assessment_type_code="2",
            status="active",
        )
    )
    await test_db_session.flush()

    await _seed_blood_category(
        test_db_session,
        category_id=blood_cat_id,
        category_key="blood-parameters",
        question_id=blood_qid,
        question_key="glucose_fasting",
        option_id=77010,
        mapping_id=77010,
    )
    await _seed_blood_category(
        test_db_session,
        category_id=adv_cat_id,
        category_key="advanced-blood-parameters",
        question_id=adv_qid,
        question_key="vitamin_d",
        option_id=77011,
        mapping_id=77011,
        unit_display="ng/mL",
    )
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=blood_cat_id, display_order=1))
    test_db_session.add(AssessmentPackageCategory(package_id=pkg_id, category_id=adv_cat_id, display_order=2))
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=aid,
            user_id=uid,
            package_id=pkg_id,
            engagement_id=1,
            status="active",
            metsights_record_id="MS-PRO-DRAFT",
        )
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=77010,
            user_id=uid,
            engagement_id=1,
            assessment_instance_id=aid,
            blood_parameters=[
                {
                    "group_name": "Labs",
                    "test_count": 2,
                    "tests": [
                        {"parameter_key": "glucose_fasting", "value": 95.0, "unit": "mg/dL"},
                        {"parameter_key": "vitamin_d", "value": 32.0, "unit": "ng/mL"},
                    ],
                }
            ],
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        f"/assessments/{aid}/metsights/draft-blood-parameters",
        headers=_auth_header(uid),
    )
    assert response.status_code == 200
    body = response.json()["data"]
    assert body["package_code"] == "METSIGHTS_PRO"
    assert body["responses_drafted"] == 2
    assert {c["category"] for c in body["categories"]} == {
        "blood-parameters",
        "advanced-blood-parameters",
    }

    result = await test_db_session.execute(
        select(QuestionnaireResponse).where(QuestionnaireResponse.assessment_instance_id == aid)
    )
    rows = list(result.scalars().all())
    assert len(rows) == 2
    by_qid = {int(r.question_id): r for r in rows}
    assert by_qid[blood_qid].answer == {"value": 95.0, "unit": "0"}
    assert by_qid[blood_qid].category_id == blood_cat_id
    assert by_qid[blood_qid].submitted_at is None
    assert by_qid[adv_qid].answer == {"value": 32.0, "unit": "0"}
    assert by_qid[adv_qid].category_id == adv_cat_id
    assert by_qid[adv_qid].submitted_at is None


@pytest.mark.asyncio
async def test_draft_blood_parameters_falls_back_to_engagement_report(async_client, test_db_session):
    await _seed_basic_setup(
        test_db_session,
        user_id=66011,
        package_id=66011,
        assessment_id=66011,
        category_id=77012,
        question_id=77012,
        option_id=77012,
        mapping_id=77012,
        blood_parameters=None,
    )
    test_db_session.add(
        IndividualHealthReport(
            report_id=77012,
            user_id=66011,
            engagement_id=1,
            assessment_instance_id=None,
            blood_parameters={
                "source": "healthians",
                "ingested_at": "2026-01-01",
                "parameters": {"glucose_fasting": {"value": 77.0, "unit": "mg/dL"}},
            },
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/assessments/66011/metsights/draft-blood-parameters",
        headers=_auth_header(66011),
    )
    assert response.status_code == 200
    assert response.json()["data"]["responses_drafted"] == 1

    result = await test_db_session.execute(
        select(QuestionnaireResponse).where(QuestionnaireResponse.assessment_instance_id == 66011)
    )
    row = result.scalar_one()
    assert row.answer == {"value": 77.0, "unit": "0"}
