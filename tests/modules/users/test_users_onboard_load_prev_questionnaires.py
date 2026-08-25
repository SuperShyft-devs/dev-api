"""Tests for load_prev_assessment_questionnaires on B2B engagement onboard."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from modules.assessments.models import AssessmentInstance, AssessmentPackageCategory
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireResponse,
)


async def _seed_org_and_packages(test_db_session, *, package_id: int = 9101):
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages "
            "(package_id, package_code, display_name, status, assessment_type_code) "
            "VALUES (:pid, :code, 'Load Prev Package', 'active', '1') "
            "ON CONFLICT (package_id) DO UPDATE SET "
            "assessment_type_code = EXCLUDED.assessment_type_code, status = 'active'"
        ),
        {"pid": package_id, "code": f"LOAD_PREV_{package_id}"},
    )
    await test_db_session.execute(
        text(
            "INSERT INTO diagnostic_package (diagnostic_package_id, reference_id, package_name, status) "
            "VALUES (1, 'REF1', 'Diag Package', 'active') ON CONFLICT (diagnostic_package_id) DO NOTHING"
        )
    )
    await test_db_session.execute(
        text(
            "INSERT INTO organizations (organization_id, name, status, departments) "
            "VALUES (8101, 'Load Prev Org', 'active', "
            "'[{\"department\": \"HR\", \"slug\": \"hr\"}]'::json) "
            "ON CONFLICT (organization_id) DO NOTHING"
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
            is_required=True,
            status="active",
        )
    )
    await test_db_session.flush()
    test_db_session.add(AssessmentPackageCategory(package_id=package_id, category_id=category_id))
    test_db_session.add(
        QuestionnaireCategoryQuestion(
            id=mapping_id,
            category_id=category_id,
            question_id=question_id,
        )
    )
    await test_db_session.commit()


async def _seed_engagement(
    test_db_session,
    *,
    engagement_id: int,
    engagement_code: str,
    package_id: int,
    load_prev: bool,
):
    await test_db_session.execute(
        text(
            "INSERT INTO engagements "
            "(engagement_id, engagement_name, engagement_code, organization_id, engagement_type, "
            "assessment_package_id, diagnostic_package_id, city, slot_duration, start_date, end_date, "
            "status, participant_count, load_prev_assessment_questionnaires) "
            "VALUES (:eid, :name, :code, 8101, 'bio_ai', :pkg, 1, 'BLR', 20, '2026-02-01', '2026-02-01', "
            "'running', 0, :load_prev)"
        ),
        {
            "eid": engagement_id,
            "name": f"Camp {engagement_code}",
            "code": engagement_code,
            "pkg": package_id,
            "load_prev": load_prev,
        },
    )
    await test_db_session.commit()


def _onboard_payload(**overrides):
    payload = {
        "age": 30,
        "first_name": "Load",
        "last_name": "Prev",
        "phone": "7777000101",
        "email": "load.prev@example.com",
        "city": "BLR",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
        "participant_department": "hr",
        "participant_blood_group": "O+",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_engagement_onboard_load_prev_false_does_not_copy(async_client, test_db_session):
    package_id = 9101
    await _seed_org_and_packages(test_db_session, package_id=package_id)
    await _seed_category_question_for_package(
        test_db_session,
        package_id=package_id,
        category_id=9101,
        category_key="load_prev_cat",
        question_id=9101,
        question_key="load_prev_q",
        mapping_id=9101,
    )
    await _seed_engagement(
        test_db_session,
        engagement_id=3101,
        engagement_code="LOADPREV0",
        package_id=package_id,
        load_prev=False,
    )
    await _seed_engagement(
        test_db_session,
        engagement_id=3102,
        engagement_code="LOADPREV1",
        package_id=package_id,
        load_prev=False,
    )

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status, first_name, last_name, email) "
            "VALUES (91001, 30, '7777000101', 'active', 'Load', 'Prev', 'load.prev@example.com') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    test_db_session.add(
        AssessmentInstance(
            user_id=91001,
            package_id=package_id,
            engagement_id=3101,
            status="active",
        )
    )
    await test_db_session.flush()
    old_instance = (
        await test_db_session.execute(
            text(
                "SELECT assessment_instance_id FROM assessment_instances "
                "WHERE user_id = 91001 AND engagement_id = 3101"
            )
        )
    ).scalar_one()

    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=int(old_instance),
            question_id=9101,
            category_ids=[9101],
            answer="prior answer",
        )
    )
    await test_db_session.commit()

    response = await async_client.post("/users/code/LOADPREV1/onboard", json=_onboard_payload())
    assert response.status_code == 200
    data = response.json()["data"]
    assert data.get("preview_available") is False
    new_instance_id = data["assessment_instance_id"]
    assert new_instance_id is not None

    count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": new_instance_id},
        )
    ).scalar_one()
    assert count == 0


@pytest.mark.asyncio
async def test_engagement_onboard_load_prev_true_copies_from_prior_basic(async_client, test_db_session):
    package_id = 9102
    await _seed_org_and_packages(test_db_session, package_id=package_id)
    await _seed_category_question_for_package(
        test_db_session,
        package_id=package_id,
        category_id=9102,
        category_key="load_prev_cat2",
        question_id=9102,
        question_key="load_prev_q2",
        mapping_id=9102,
    )
    await _seed_engagement(
        test_db_session,
        engagement_id=3201,
        engagement_code="LOADPREV2A",
        package_id=package_id,
        load_prev=False,
    )
    await _seed_engagement(
        test_db_session,
        engagement_id=3202,
        engagement_code="LOADPREV2B",
        package_id=package_id,
        load_prev=True,
    )

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status, first_name, last_name, email) "
            "VALUES (91002, 30, '7777000102', 'active', 'Load', 'Prev', 'load.prev2@example.com') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    test_db_session.add(
        AssessmentInstance(
            user_id=91002,
            package_id=package_id,
            engagement_id=3201,
            status="active",
        )
    )
    await test_db_session.flush()
    old_instance_id = (
        await test_db_session.execute(
            text(
                "SELECT assessment_instance_id FROM assessment_instances "
                "WHERE user_id = 91002 AND engagement_id = 3201"
            )
        )
    ).scalar_one()

    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=int(old_instance_id),
            question_id=9102,
            category_ids=[9102],
            answer="copied answer",
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/users/code/LOADPREV2B/onboard",
        json=_onboard_payload(phone="7777000102", email="load.prev2@example.com"),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data.get("preview_available") is True
    new_instance_id = data["assessment_instance_id"]

    row = (
        await test_db_session.execute(
            text(
                "SELECT question_id, answer FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": new_instance_id},
        )
    ).first()
    assert row is not None
    assert int(row.question_id) == 9102
    assert row.answer == "copied answer"


@pytest.mark.asyncio
async def test_engagement_onboard_load_prev_ignores_fitprint_only_prior(async_client, test_db_session):
    basic_package_id = 9103
    fitprint_package_id = 9104
    await _seed_org_and_packages(test_db_session, package_id=basic_package_id)
    await test_db_session.execute(
        text(
            "INSERT INTO assessment_packages "
            "(package_id, package_code, display_name, status, assessment_type_code) "
            "VALUES (:pid, 'FITPRINT_ONLY', 'FitPrint', 'active', '7') "
            "ON CONFLICT (package_id) DO UPDATE SET assessment_type_code = '7'"
        ),
        {"pid": fitprint_package_id},
    )
    await _seed_category_question_for_package(
        test_db_session,
        package_id=basic_package_id,
        category_id=9103,
        category_key="load_prev_cat3",
        question_id=9103,
        question_key="load_prev_q3",
        mapping_id=9103,
    )
    await _seed_engagement(
        test_db_session,
        engagement_id=3301,
        engagement_code="LOADPREV3A",
        package_id=fitprint_package_id,
        load_prev=False,
    )
    await _seed_engagement(
        test_db_session,
        engagement_id=3302,
        engagement_code="LOADPREV3B",
        package_id=basic_package_id,
        load_prev=True,
    )

    await test_db_session.execute(
        text(
            "INSERT INTO users (user_id, age, phone, status, first_name, last_name, email) "
            "VALUES (91003, 30, '7777000103', 'active', 'Load', 'Prev', 'load.prev3@example.com') "
            "ON CONFLICT (user_id) DO NOTHING"
        )
    )
    test_db_session.add(
        AssessmentInstance(
            user_id=91003,
            package_id=fitprint_package_id,
            engagement_id=3301,
            status="active",
        )
    )
    await test_db_session.flush()
    fitprint_instance_id = (
        await test_db_session.execute(
            text(
                "SELECT assessment_instance_id FROM assessment_instances "
                "WHERE user_id = 91003 AND engagement_id = 3301"
            )
        )
    ).scalar_one()
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=int(fitprint_instance_id),
            question_id=9103,
            category_ids=[9103],
            answer="fitprint only",
        )
    )
    await test_db_session.commit()

    response = await async_client.post(
        "/users/code/LOADPREV3B/onboard",
        json=_onboard_payload(phone="7777000103", email="load.prev3@example.com"),
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data.get("preview_available") is False
    new_instance_id = data["assessment_instance_id"]

    count = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) FROM questionnaire_responses "
                "WHERE assessment_instance_id = :aid"
            ),
            {"aid": new_instance_id},
        )
    ).scalar_one()
    assert count == 0


@pytest.mark.asyncio
async def test_create_engagement_persists_load_prev_flag(async_client, test_db_session):
    from tests.modules.engagements.test_engagements_routes import (
        _auth_header,
        _seed_assessment_package,
        _seed_diagnostic_package,
        _seed_employee,
        _seed_organization,
    )

    await _seed_employee(test_db_session, user_id=7901, employee_id=901)
    await _seed_organization(test_db_session, organization_id=1, name="Test Organization 1")
    await _seed_assessment_package(test_db_session, package_id=1, package_code="PKG1")
    await _seed_diagnostic_package(test_db_session, diagnostic_package_id=1)
    await test_db_session.execute(text("DELETE FROM platform_settings"))
    await test_db_session.execute(
        text(
            "INSERT INTO platform_settings "
            "(settings_id, b2c_default_assessment_package_id, b2c_default_diagnostic_package_id, "
            "default_onboarding_assistant_employee_ids) "
            "VALUES (1, 1, 1, '901')"
        )
    )
    await test_db_session.commit()

    payload = {
        "engagement_name": "Load Prev Camp",
        "organization_id": 1,
        "engagement_type": "bio_ai",
        "assessment_package_id": 1,
        "diagnostic_package_id": 1,
        "city": "BLR",
        "slot_duration": 20,
        "start_date": "2026-02-01",
        "end_date": "2026-02-02",
        "load_prev_assessment_questionnaires": True,
    }

    response = await async_client.post("/engagements", headers=_auth_header(7901), json=payload)
    assert response.status_code == 201
    engagement_id = response.json()["data"]["engagement_id"]

    details = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(7901))
    assert details.status_code == 200
    detail_data = details.json().get("data", details.json())
    assert detail_data["load_prev_assessment_questionnaires"] is True

    update_payload = {
        **payload,
        "engagement_code": detail_data["engagement_code"],
        "load_prev_assessment_questionnaires": False,
    }
    updated = await async_client.put(
        f"/engagements/{engagement_id}",
        headers=_auth_header(7901),
        json=update_payload,
    )
    assert updated.status_code == 200

    details2 = await async_client.get(f"/engagements/{engagement_id}", headers=_auth_header(7901))
    detail_data2 = details2.json().get("data", details2.json())
    assert detail_data2["load_prev_assessment_questionnaires"] is False
