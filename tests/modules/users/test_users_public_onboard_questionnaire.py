"""Tests for optional questionnaire on POST /users/public/onboard."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import text

from modules.assessments.models import AssessmentPackageCategory
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
)


async def _seed_onboard_packages(test_db_session, *, assessment_package_id: int = 9201):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages (package_id, package_code, display_name, status) "
            "VALUES (:pid, :code, 'Onboard Q Package', 'active') "
            "ON CONFLICT (package_id) DO UPDATE SET status = 'active'"
        ),
        {"pid": assessment_package_id, "code": f"ONB_Q_{assessment_package_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    by_type = {
        "bio_ai": {
            "assessment_package_id": assessment_package_id,
            "diagnostic_package_id": 1,
            "blood_collection_type": "home_collection",
            "create_profile_on_metsights": False,
            "enroll_for_fitprint_full": False,
        },
    }
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "b2c_default_engagement_type, b2c_default_blood_collection_type, "
            "b2c_default_create_profile_on_metsights, b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type) "
            "VALUES (1, :pid, 1, 'bio_ai', 'home_collection', false, false, CAST(:by_type AS jsonb)) "
            "ON CONFLICT (settings_id) DO UPDATE SET "
            "b2c_default_assessment_package_id = EXCLUDED.b2c_default_assessment_package_id, "
            "b2c_default_diagnostic_package_id = EXCLUDED.b2c_default_diagnostic_package_id, "
            "b2c_default_create_profile_on_metsights = EXCLUDED.b2c_default_create_profile_on_metsights, "
            "b2c_default_enroll_for_fitprint_full = EXCLUDED.b2c_default_enroll_for_fitprint_full, "
            "b2c_onboarding_by_engagement_type = EXCLUDED.b2c_onboarding_by_engagement_type"
        ),
        {"pid": assessment_package_id, "by_type": json.dumps(by_type)},
    )
    await test_db_session.commit()


async def _seed_category_with_text_question(
    test_db_session,
    *,
    package_id: int,
    category_id: int,
    category_key: str,
    question_id: int,
    question_key: str,
    mapping_id: int,
    is_required: bool = True,
):
    test_db_session.add(
        QuestionnaireCategory(
            category_id=category_id,
            category_key=category_key,
            display_name=category_key,
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=question_id,
            question_key=question_key,
            question_text=f"Question {question_id}",
            question_type="text",
            is_required=is_required,
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(
        AssessmentPackageCategory(package_id=package_id, category_id=category_id)
    )
    test_db_session.add(
        QuestionnaireCategoryQuestion(
            id=mapping_id,
            category_id=category_id,
            question_id=question_id,
        )
    )
    await test_db_session.commit()


def _base_onboard_payload(**overrides):
    payload = {
        "age": 30,
        "first_name": "Q",
        "last_name": "User",
        "email": "q.onboard@example.com",
        "phone": "5555123401",
        "city": "Mumbai",
        "engagement_type": "bio_ai",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_public_onboard_without_questionnaire_unchanged(async_client, test_db_session):
    await _seed_onboard_packages(test_db_session, assessment_package_id=9201)
    await _seed_category_with_text_question(
        test_db_session,
        package_id=9201,
        category_id=9201,
        category_key="onb_cat_a",
        question_id=9201,
        question_key="onb_q_a",
        mapping_id=9201,
    )

    response = await async_client.post("/users/public/onboard", json=_base_onboard_payload())
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["assessment_instance_id"] is not None

    responses = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": data["assessment_instance_id"]},
        )
    ).scalar_one()
    assert int(responses) == 0

    instance = (
        await test_db_session.execute(
            text("SELECT status FROM assessment_instances WHERE assessment_instance_id = :aid"),
            {"aid": data["assessment_instance_id"]},
        )
    ).first()
    assert instance.status == "active"


@pytest.mark.asyncio
async def test_public_onboard_saves_questionnaire_and_completes_when_all_done(
    async_client, test_db_session
):
    await _seed_onboard_packages(test_db_session, assessment_package_id=9202)
    await _seed_category_with_text_question(
        test_db_session,
        package_id=9202,
        category_id=9202,
        category_key="onb_complete_cat",
        question_id=9202,
        question_key="onb_complete_q",
        mapping_id=9202,
    )

    payload = _base_onboard_payload(
        phone="5555123402",
        email="q.complete@example.com",
        questionnaire={
            "onb_complete_cat": {
                "responses": [{"question_id": 9202, "answer": "yes"}],
            }
        },
    )
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    data = response.json()["data"]
    aid = data["assessment_instance_id"]

    row = (
        await test_db_session.execute(
            text(
                "SELECT question_id, answer, submitted_at FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": aid},
        )
    ).first()
    assert row is not None
    assert int(row.question_id) == 9202
    assert row.answer == "yes"
    assert row.submitted_at is not None

    progress = (
        await test_db_session.execute(
            text(
                "SELECT status FROM assessment_category_progress "
                "WHERE assessment_instance_id = :aid AND category_id = 9202"
            ),
            {"aid": aid},
        )
    ).first()
    assert progress.status == "complete"

    instance = (
        await test_db_session.execute(
            text("SELECT status FROM assessment_instances WHERE assessment_instance_id = :aid"),
            {"aid": aid},
        )
    ).first()
    assert instance.status == "completed"


@pytest.mark.asyncio
async def test_public_onboard_skips_unknown_category_key(async_client, test_db_session):
    await _seed_onboard_packages(test_db_session, assessment_package_id=9203)
    await _seed_category_with_text_question(
        test_db_session,
        package_id=9203,
        category_id=9203,
        category_key="onb_known_cat",
        question_id=9203,
        question_key="onb_known_q",
        mapping_id=9203,
    )

    payload = _base_onboard_payload(
        phone="5555123403",
        email="q.skipcat@example.com",
        questionnaire={
            "not-in-package": {
                "responses": [{"question_id": 9203, "answer": "ignored"}],
            }
        },
    )
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    aid = response.json()["data"]["assessment_instance_id"]

    count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": aid},
        )
    ).scalar_one()
    assert int(count) == 0

    instance = (
        await test_db_session.execute(
            text("SELECT status FROM assessment_instances WHERE assessment_instance_id = :aid"),
            {"aid": aid},
        )
    ).first()
    assert instance.status == "active"


@pytest.mark.asyncio
async def test_public_onboard_skips_question_not_in_category(async_client, test_db_session):
    await _seed_onboard_packages(test_db_session, assessment_package_id=9204)
    await _seed_category_with_text_question(
        test_db_session,
        package_id=9204,
        category_id=9204,
        category_key="onb_skip_q_cat",
        question_id=9204,
        question_key="onb_skip_q_good",
        mapping_id=9204,
    )
    # Orphan question not mapped to the category
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=92041,
            question_key="onb_skip_q_orphan",
            question_text="Orphan",
            question_type="text",
            is_required=False,
            status="active",
        )
    )
    await test_db_session.commit()

    payload = _base_onboard_payload(
        phone="5555123404",
        email="q.skipq@example.com",
        questionnaire={
            "onb_skip_q_cat": {
                "responses": [
                    {"question_id": 92041, "answer": "skip-me"},
                    {"question_id": 9204, "answer": "keep-me"},
                ],
            }
        },
    )
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    aid = response.json()["data"]["assessment_instance_id"]

    rows = (
        await test_db_session.execute(
            text(
                "SELECT question_id, answer FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid ORDER BY question_id"
            ),
            {"aid": aid},
        )
    ).all()
    assert len(rows) == 1
    assert int(rows[0].question_id) == 9204
    assert rows[0].answer == "keep-me"


@pytest.mark.asyncio
async def test_public_onboard_partial_categories_keeps_instance_active(
    async_client, test_db_session
):
    await _seed_onboard_packages(test_db_session, assessment_package_id=9205)
    await _seed_category_with_text_question(
        test_db_session,
        package_id=9205,
        category_id=9205,
        category_key="onb_partial_a",
        question_id=9205,
        question_key="onb_partial_qa",
        mapping_id=9205,
    )
    await _seed_category_with_text_question(
        test_db_session,
        package_id=9205,
        category_id=9206,
        category_key="onb_partial_b",
        question_id=9206,
        question_key="onb_partial_qb",
        mapping_id=9206,
    )

    payload = _base_onboard_payload(
        phone="5555123405",
        email="q.partial@example.com",
        questionnaire={
            "onb_partial_a": {
                "responses": [{"question_id": 9205, "answer": "only-a"}],
            }
        },
    )
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    aid = response.json()["data"]["assessment_instance_id"]

    progress_a = (
        await test_db_session.execute(
            text(
                "SELECT status FROM assessment_category_progress "
                "WHERE assessment_instance_id = :aid AND category_id = 9205"
            ),
            {"aid": aid},
        )
    ).first()
    assert progress_a.status == "complete"

    progress_b = (
        await test_db_session.execute(
            text(
                "SELECT status FROM assessment_category_progress "
                "WHERE assessment_instance_id = :aid AND category_id = 9206"
            ),
            {"aid": aid},
        )
    ).first()
    assert progress_b.status == "incomplete"

    instance = (
        await test_db_session.execute(
            text("SELECT status FROM assessment_instances WHERE assessment_instance_id = :aid"),
            {"aid": aid},
        )
    ).first()
    assert instance.status == "active"


@pytest.mark.asyncio
async def test_public_onboard_scale_answer_saved(async_client, test_db_session):
    await _seed_onboard_packages(test_db_session, assessment_package_id=9207)
    test_db_session.add(
        QuestionnaireCategory(
            category_id=9207,
            category_key="onb_scale_cat",
            display_name="Scale",
            status="active",
        )
    )
    test_db_session.add(
        QuestionnaireDefinition(
            question_id=9207,
            question_key="onb_scale_q",
            question_text="Height",
            question_type="scale",
            is_required=True,
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(AssessmentPackageCategory(package_id=9207, category_id=9207))
    test_db_session.add(
        QuestionnaireCategoryQuestion(id=9207, category_id=9207, question_id=9207)
    )
    test_db_session.add(
        QuestionnaireOption(
            question_id=9207,
            option_value="cm",
            display_name="cm",
            tooltip_text=None,
        )
    )
    await test_db_session.commit()

    payload = _base_onboard_payload(
        phone="5555123407",
        email="q.scale@example.com",
        questionnaire={
            "onb_scale_cat": {
                "responses": [
                    {"question_id": 9207, "answer": {"value": 175, "unit": "cm"}},
                ],
            }
        },
    )
    response = await async_client.post("/users/public/onboard", json=payload)
    assert response.status_code == 200
    aid = response.json()["data"]["assessment_instance_id"]

    row = (
        await test_db_session.execute(
            text(
                "SELECT answer FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid AND question_id = 9207"
            ),
            {"aid": aid},
        )
    ).first()
    assert row is not None
    assert row.answer == {"value": 175, "unit": "cm"}

    instance = (
        await test_db_session.execute(
            text("SELECT status FROM assessment_instances WHERE assessment_instance_id = :aid"),
            {"aid": aid},
        )
    ).first()
    assert instance.status == "completed"
