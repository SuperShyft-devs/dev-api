"""Integration tests for user-facing questionnaire endpoints."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from core.config import settings
from core.security import create_jwt_token
from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageCategory
from modules.engagements.models import Engagement
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireOption,
    QuestionnaireResponse,
)
from modules.users.models import User, UserPreference


def _auth_header(user_id: int) -> dict[str, str]:
    token = create_jwt_token({"sub": str(user_id)}, timedelta(minutes=5), secret_key=settings.JWT_SECRET_KEY)
    return {"Authorization": f"Bearer {token}"}


async def _seed_user(test_db_session, *, user_id: int):
    test_db_session.add(User(user_id=user_id, age=30, phone=f"{user_id}000000000", status="active"))
    await test_db_session.commit()


async def _map_question_to_category(test_db_session, *, mapping_id: int, category_id: int, question_id: int):
    test_db_session.add(
        QuestionnaireCategoryQuestion(
            id=mapping_id,
            category_id=category_id,
            question_id=question_id,
        )
    )


async def _ensure_test_engagement(test_db_session, *, engagement_id: int = 1):
    """Ensure a test engagement exists for foreign key constraint.
    
    Also creates required packages and diagnostic packages.
    """
    from sqlalchemy import select
    from modules.diagnostics.models import DiagnosticPackage
    
    result = await test_db_session.execute(
        select(Engagement).where(Engagement.engagement_id == engagement_id)
    )
    existing = result.scalar_one_or_none()
    
    if existing is None:
        # Create test assessment package if needed
        pkg_result = await test_db_session.execute(
            select(AssessmentPackage).where(AssessmentPackage.package_id == 1)
        )
        if pkg_result.scalar_one_or_none() is None:
            test_pkg = AssessmentPackage(
                package_id=1,
                package_code="TEST_PKG",
                display_name="Test Package",
                status="active"
            )
            test_db_session.add(test_pkg)
        
        # Create test diagnostic package if needed
        diag_result = await test_db_session.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == 1)
        )
        if diag_result.scalar_one_or_none() is None:
            test_diag = DiagnosticPackage(
                diagnostic_package_id=1,
                reference_id="TEST_DIAG",
                package_name="Test Diagnostic",
                diagnostic_provider="Test Provider",
                no_of_tests=0,
                status="active"
            )
            test_db_session.add(test_diag)
        
        engagement = Engagement(
            engagement_id=engagement_id,
            engagement_name="Test Engagement",
            engagement_code="TEST001",
            engagement_type="test",
            assessment_package_id=1,
            diagnostic_package_id=1,
            slot_duration=20,
            status="active",
            participant_count=0,
        )
        test_db_session.add(engagement)
        await test_db_session.commit()


# ==================== GET /questionnaire/categories/{category_id}/questions Tests ====================


@pytest.mark.asyncio
async def test_list_category_questions_requires_auth(async_client):
    response = await async_client.get("/questionnaire/categories/7001/questions")
    assert response.status_code == 401
    assert response.json()["error_code"] == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_list_category_questions_allows_user_and_returns_only_active(async_client, test_db_session):
    await _seed_user(test_db_session, user_id=5098)
    test_db_session.add(
        QuestionnaireCategory(category_id=7998, category_key="cat_7998", display_name="Category 7998", status="active")
    )
    test_db_session.add_all(
        [
            QuestionnaireDefinition(
                question_id=8991,
                question_key="q8991",
                question_text="Active question",
                question_type="text",
                status="active",
            ),
            QuestionnaireDefinition(
                question_id=8992,
                question_key="q8992",
                question_text="Inactive question",
                question_type="text",
                status="inactive",
            ),
        ]
    )
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9901, category_id=7998, question_id=8991)
    await _map_question_to_category(test_db_session, mapping_id=9902, category_id=7998, question_id=8992)
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/categories/7998/questions", headers=_auth_header(5098))
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 1
    assert data[0]["question_id"] == 8991


# ==================== GET /questionnaire/{category_id} Tests ====================


@pytest.mark.asyncio
async def test_get_questionnaire_requires_auth(async_client):
    """Test that authentication is required."""
    response = await async_client.get("/questionnaire/1")
    assert response.status_code == 401
    assert response.json()["error_code"] == "AUTH_FAILED"


@pytest.mark.asyncio
async def test_get_questionnaire_returns_404_when_assessment_not_found(async_client, test_db_session):
    """Test 404 when assessment instance does not exist."""
    await _seed_user(test_db_session, user_id=5001)

    response = await async_client.get("/questionnaire/99999", headers=_auth_header(5001))
    assert response.status_code == 404
    assert response.json()["error_code"] == "ASSESSMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_get_questionnaire_returns_404_when_category_not_mapped_for_user(async_client, test_db_session):
    """Test 404 when category is not mapped for requesting user."""
    await _seed_user(test_db_session, user_id=5002)
    await _seed_user(test_db_session, user_id=5003)
    await _ensure_test_engagement(test_db_session)

    # Create package
    package = AssessmentPackage(package_id=1001, package_code="PKG001", display_name="Test Package", status="active")
    test_db_session.add(package)
    await test_db_session.commit()

    # Create assessment instance for user 5003
    instance = AssessmentInstance(
        assessment_instance_id=2001,
        user_id=5003,
        package_id=1001,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    category = QuestionnaireCategory(category_id=7001, category_key="cat_7001", display_name="Category 7001", status="active")
    test_db_session.add(category)
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=1001, category_id=7001))
    await test_db_session.commit()

    # User 5002 has no mapped instance for this category
    response = await async_client.get("/questionnaire/7001", headers=_auth_header(5002))
    assert response.status_code == 404
    assert response.json()["error_code"] == "ASSESSMENT_NOT_FOUND"


@pytest.mark.asyncio
async def test_get_questionnaire_returns_empty_questions_when_no_package_questions(async_client, test_db_session):
    """Test empty questions list when package has no questions."""
    await _seed_user(test_db_session, user_id=5004)
    await _ensure_test_engagement(test_db_session)

    package = AssessmentPackage(package_id=1002, package_code="PKG002", display_name="Empty Package", status="active")
    category = QuestionnaireCategory(category_id=7002, category_key="cat_7002", display_name="Category 7002", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()

    instance = AssessmentInstance(
        assessment_instance_id=2002,
        user_id=5004,
        package_id=1002,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    test_db_session.add(AssessmentPackageCategory(package_id=1002, category_id=7002))
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/7002", headers=_auth_header(5004))
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["assessment_instance_id"] == 2002
    assert data["status"] == "active"
    assert data["questions"] == []


@pytest.mark.asyncio
async def test_get_questionnaire_returns_questions_without_answers(async_client, test_db_session):
    """Test getting questions when no answers exist yet."""
    await _seed_user(test_db_session, user_id=5005)
    await _ensure_test_engagement(test_db_session)

    # Create package
    package = AssessmentPackage(package_id=1003, package_code="PKG003", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7003, category_key="cat_7003", display_name="Category 7003", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    # Create questions
    q1 = QuestionnaireDefinition(
        question_id=3001,
        question_key="q3001",
        question_text="What is your age?",
        question_type="number",
        status="active",
    )
    q2 = QuestionnaireDefinition(
        question_id=3002,
        question_key="q3002",
        question_text="Select your gender",
        question_type="single_choice",
        status="active",
    )
    test_db_session.add_all([q1, q2])
    test_db_session.add_all(
        [
            QuestionnaireOption(question_id=3002, option_value="Male", display_name="Male", tooltip_text=None),
            QuestionnaireOption(question_id=3002, option_value="Female", display_name="Female", tooltip_text=None),
            QuestionnaireOption(question_id=3002, option_value="Other", display_name="Other", tooltip_text=None),
        ]
    )
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9903, category_id=7003, question_id=3001)
    await _map_question_to_category(test_db_session, mapping_id=9904, category_id=7003, question_id=3002)
    await test_db_session.commit()
    # Link questions to package
    test_db_session.add(AssessmentPackageCategory(package_id=1003, category_id=7003))

    # Create assessment instance
    instance = AssessmentInstance(
        assessment_instance_id=2003,
        user_id=5005,
        package_id=1003,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/7003", headers=_auth_header(5005))
    assert response.status_code == 200

    data = response.json()["data"]
    assert data["assessment_instance_id"] == 2003
    assert data["status"] == "active"
    assert len(data["questions"]) == 2

    # Check first question
    assert data["questions"][0]["question_id"] == 3001
    assert data["questions"][0]["question_text"] == "What is your age?"
    assert data["questions"][0]["question_type"] == "number"
    assert data["questions"][0]["options"] is None
    assert data["questions"][0]["answer"] is None

    # Check second question
    assert data["questions"][1]["question_id"] == 3002
    assert data["questions"][1]["question_text"] == "Select your gender"
    assert data["questions"][1]["question_type"] == "single_choice"
    assert data["questions"][1]["options"] == [
        {"option_value": "Male", "display_name": "Male", "tooltip_text": None},
        {"option_value": "Female", "display_name": "Female", "tooltip_text": None},
        {"option_value": "Other", "display_name": "Other", "tooltip_text": None},
    ]
    assert data["questions"][1]["answer"] is None


@pytest.mark.asyncio
async def test_get_questionnaire_returns_questions_with_existing_answers(async_client, test_db_session):
    """Test getting questions with existing draft answers."""
    await _seed_user(test_db_session, user_id=5006)
    await _ensure_test_engagement(test_db_session)

    # Create package
    package = AssessmentPackage(package_id=1004, package_code="PKG004", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7004, category_key="cat_7004", display_name="Category 7004", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    # Create questions
    q1 = QuestionnaireDefinition(
        question_id=3003,
        question_key="q3003",
        question_text="What is your age?",
        question_type="number",
        status="active",
    )
    test_db_session.add(q1)
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9905, category_id=7004, question_id=3003)
    await test_db_session.commit()

    # Link question to package
    test_db_session.add(AssessmentPackageCategory(package_id=1004, category_id=7004))

    # Create assessment instance
    instance = AssessmentInstance(
        assessment_instance_id=2004,
        user_id=5006,
        package_id=1004,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)

    # Create existing response
    response_row = QuestionnaireResponse(
        assessment_instance_id=2004,
        question_id=3003,
        category_id=7004,
        answer=25,
        submitted_at=None,
    )
    test_db_session.add(response_row)
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/7004", headers=_auth_header(5006))
    assert response.status_code == 200

    data = response.json()["data"]
    assert len(data["questions"]) == 1
    assert data["questions"][0]["question_id"] == 3003
    assert data["questions"][0]["answer"] == 25


@pytest.mark.asyncio
async def test_get_questionnaire_skips_inactive_questions(async_client, test_db_session):
    """Test that inactive questions are not returned."""
    await _seed_user(test_db_session, user_id=5007)
    await _ensure_test_engagement(test_db_session)

    # Create package
    package = AssessmentPackage(package_id=1005, package_code="PKG005", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7005, category_key="cat_7005", display_name="Category 7005", status="active")
    test_db_session.add(package)
    test_db_session.add(category)

    # Create questions (one active, one inactive)
    q1 = QuestionnaireDefinition(
        question_id=3004,
        question_key="q3004",
        question_text="Active question",
        question_type="text",
        status="active",
    )
    q2 = QuestionnaireDefinition(
        question_id=3005,
        question_key="q3005",
        question_text="Inactive question",
        question_type="text",
        status="inactive",
    )
    test_db_session.add_all([q1, q2])
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9906, category_id=7005, question_id=3004)
    await _map_question_to_category(test_db_session, mapping_id=9907, category_id=7005, question_id=3005)
    await test_db_session.commit()
    # Link both questions to package
    test_db_session.add(AssessmentPackageCategory(package_id=1005, category_id=7005))

    # Create assessment instance
    instance = AssessmentInstance(
        assessment_instance_id=2005,
        user_id=5007,
        package_id=1005,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/7005", headers=_auth_header(5007))
    assert response.status_code == 200

    data = response.json()["data"]
    assert len(data["questions"]) == 1
    assert data["questions"][0]["question_id"] == 3004


@pytest.mark.asyncio
async def test_get_questionnaire_applies_parent_answer_visibility(async_client, test_db_session):
    """Child question is hidden when parent answer does not match rule."""
    await _seed_user(test_db_session, user_id=5071)
    await _ensure_test_engagement(test_db_session)

    package = AssessmentPackage(package_id=1701, package_code="PKG1701", display_name="Rule Package", status="active")
    category = QuestionnaireCategory(category_id=7701, category_key="cat_7701", display_name="Diet", status="active")
    test_db_session.add_all([package, category])
    await test_db_session.commit()

    q_parent = QuestionnaireDefinition(
        question_id=3701,
        question_key="consume_coffee_or_tea",
        question_text="Do you consume coffee or tea?",
        question_type="single_choice",
        status="active",
    )
    q_child = QuestionnaireDefinition(
        question_id=3702,
        question_key="coffee_or_tea_cups",
        question_text="How many cups do you consume?",
        question_type="number",
        visibility_rules={
            "match": "all",
            "conditions": [
                {
                    "type": "question_answer",
                    "operator": "equals",
                    "question_key": "consume_coffee_or_tea",
                    "value": "yes",
                }
            ],
        },
        status="active",
    )
    test_db_session.add_all([q_parent, q_child])
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9971, category_id=7701, question_id=3701)
    await _map_question_to_category(test_db_session, mapping_id=9972, category_id=7701, question_id=3702)
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageCategory(package_id=1701, category_id=7701))
    instance = AssessmentInstance(
        assessment_instance_id=2701,
        user_id=5071,
        package_id=1701,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    test_db_session.add(
        QuestionnaireResponse(
            assessment_instance_id=2701,
            question_id=3701,
            category_id=7701,
            answer="no",
            submitted_at=None,
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/7701", headers=_auth_header(5071))
    assert response.status_code == 200
    questions = response.json()["data"]["questions"]
    by_id = {row["question_id"]: row for row in questions}
    assert by_id[3701]["is_visible"] is True
    assert by_id[3702]["is_visible"] is False
    assert by_id[3702]["visibility_reason"] == "visibility_rules_not_matched"


@pytest.mark.asyncio
async def test_get_questionnaire_prefills_and_uses_preferences_for_visibility(async_client, test_db_session):
    """Preference data should prefill and hide non-applicable questions."""
    await _seed_user(test_db_session, user_id=5072)
    await _ensure_test_engagement(test_db_session)

    package = AssessmentPackage(package_id=1702, package_code="PKG1702", display_name="Rule Package", status="active")
    category = QuestionnaireCategory(category_id=7702, category_key="cat_7702", display_name="Diet", status="active")
    test_db_session.add_all([package, category])
    await test_db_session.commit()

    test_db_session.add(
        UserPreference(
            user_id=5072,
            diet_preference="veg",
            allergies=["dairy"],
        )
    )

    q_diet = QuestionnaireDefinition(
        question_id=3703,
        question_key="diet_preference",
        question_text="What is your diet preference?",
        question_type="single_choice",
        prefill_from={"source": "user_preference", "preference_key": "diet_preference"},
        status="active",
    )
    q_non_veg = QuestionnaireDefinition(
        question_id=3704,
        question_key="consume_non_veg",
        question_text="Do you consume non-veg?",
        question_type="single_choice",
        visibility_rules={
            "match": "all",
            "conditions": [
                {
                    "type": "user_preference",
                    "operator": "equals",
                    "preference_key": "diet_preference",
                    "value": "non_veg",
                }
            ],
        },
        status="active",
    )
    test_db_session.add_all([q_diet, q_non_veg])
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9973, category_id=7702, question_id=3703)
    await _map_question_to_category(test_db_session, mapping_id=9974, category_id=7702, question_id=3704)
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageCategory(package_id=1702, category_id=7702))
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=2702,
            user_id=5072,
            package_id=1702,
            engagement_id=1,
            status="active",
        )
    )
    await test_db_session.commit()

    response = await async_client.get("/questionnaire/7702", headers=_auth_header(5072))
    assert response.status_code == 200
    questions = response.json()["data"]["questions"]
    by_id = {row["question_id"]: row for row in questions}
    assert by_id[3703]["answer"] == "veg"
    assert by_id[3703]["answer_source"] == "prefill"
    assert by_id[3704]["is_visible"] is False

    await test_db_session.delete(
        (
            await test_db_session.execute(
                select(UserPreference).where(UserPreference.user_id == 5072)
            )
        ).scalar_one()
    )
    await test_db_session.commit()


# ==================== PUT /questionnaire/{category_id}/responses Tests ====================


@pytest.mark.asyncio
async def test_upsert_responses_requires_auth(async_client):
    """Test that authentication is required."""
    response = await async_client.put("/questionnaire/1/responses", json={"responses": []})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_upsert_responses_validates_payload(async_client, test_db_session):
    """Test payload validation."""
    await _seed_user(test_db_session, user_id=5008)

    # Empty responses array
    response = await async_client.put("/questionnaire/1/responses", headers=_auth_header(5008), json={"responses": []})
    assert response.status_code == 400

    # Missing responses field
    response = await async_client.put("/questionnaire/1/responses", headers=_auth_header(5008), json={})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_upsert_responses_returns_404_when_assessment_not_found(async_client, test_db_session):
    """Test 404 when assessment instance does not exist."""
    await _seed_user(test_db_session, user_id=5009)

    payload = {"responses": [{"question_id": 1, "answer": "test"}]}
    response = await async_client.put("/questionnaire/99999/responses", headers=_auth_header(5009), json=payload)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_upsert_responses_returns_404_when_category_not_mapped_for_user(async_client, test_db_session):
    """Test 404 when user has no assessment mapped to category."""
    await _seed_user(test_db_session, user_id=5010)
    await _seed_user(test_db_session, user_id=5011)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1006, package_code="PKG006", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7006, category_key="cat_7006", display_name="Category 7006", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2006,
        user_id=5011,
        package_id=1006,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    test_db_session.add(AssessmentPackageCategory(package_id=1006, category_id=7006))
    await test_db_session.commit()

    payload = {"responses": [{"question_id": 1, "answer": "test"}]}
    response = await async_client.put("/questionnaire/7006/responses", headers=_auth_header(5010), json=payload)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_upsert_responses_returns_422_when_assessment_completed(async_client, test_db_session):
    """Test 422 when trying to update completed assessment."""
    await _seed_user(test_db_session, user_id=5012)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1007, package_code="PKG007", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7007, category_key="cat_7007", display_name="Category 7007", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2007,
        user_id=5012,
        package_id=1007,
        engagement_id=1,
        status="completed",
    )
    test_db_session.add(instance)
    test_db_session.add(AssessmentPackageCategory(package_id=1007, category_id=7007))
    await test_db_session.commit()

    payload = {"responses": [{"question_id": 1, "answer": "test"}]}
    response = await async_client.put("/questionnaire/7007/responses", headers=_auth_header(5012), json=payload)
    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_STATE"


@pytest.mark.asyncio
async def test_upsert_responses_returns_422_when_question_not_in_package(async_client, test_db_session):
    """Test 422 when question doesn't belong to assessment package."""
    await _seed_user(test_db_session, user_id=5013)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1008, package_code="PKG008", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7008, category_key="cat_7008", display_name="Category 7008", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2008,
        user_id=5013,
        package_id=1008,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    test_db_session.add(AssessmentPackageCategory(package_id=1008, category_id=7008))
    await test_db_session.commit()

    # Try to answer a question that's not in the package
    payload = {"responses": [{"question_id": 99999, "answer": "test"}]}
    response = await async_client.put("/questionnaire/7008/responses", headers=_auth_header(5013), json=payload)
    assert response.status_code == 422
    assert "does not belong" in response.json()["message"]


@pytest.mark.asyncio
async def test_upsert_responses_returns_422_when_question_inactive(async_client, test_db_session):
    """Test 422 when question is inactive."""
    await _seed_user(test_db_session, user_id=5014)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1009, package_code="PKG009", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7009, category_key="cat_7009", display_name="Category 7009", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    q1 = QuestionnaireDefinition(
        question_id=3006,
        question_key="q3006",
        question_text="Inactive question",
        question_type="text",
        status="inactive",
    )
    test_db_session.add(q1)
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9908, category_id=7009, question_id=3006)
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=1009, category_id=7009))

    instance = AssessmentInstance(
        assessment_instance_id=2009,
        user_id=5014,
        package_id=1009,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    payload = {"responses": [{"question_id": 3006, "answer": "test"}]}
    response = await async_client.put("/questionnaire/7009/responses", headers=_auth_header(5014), json=payload)
    assert response.status_code == 422
    assert "not available" in response.json()["message"]


@pytest.mark.asyncio
async def test_upsert_responses_rejects_hidden_question(async_client, test_db_session):
    """Submitting hidden question answers should be rejected."""
    await _seed_user(test_db_session, user_id=5073)
    await _ensure_test_engagement(test_db_session)

    package = AssessmentPackage(package_id=1703, package_code="PKG1703", display_name="Rule Package", status="active")
    category = QuestionnaireCategory(category_id=7703, category_key="cat_7703", display_name="Diet", status="active")
    test_db_session.add_all([package, category])
    await test_db_session.commit()

    q_parent = QuestionnaireDefinition(
        question_id=3705,
        question_key="consume_coffee_or_tea",
        question_text="Do you consume coffee or tea?",
        question_type="single_choice",
        status="active",
    )
    q_child = QuestionnaireDefinition(
        question_id=3706,
        question_key="coffee_or_tea_cups",
        question_text="How many cups?",
        question_type="number",
        visibility_rules={
            "match": "all",
            "conditions": [
                {
                    "type": "question_answer",
                    "operator": "equals",
                    "question_key": "consume_coffee_or_tea",
                    "value": "yes",
                }
            ],
        },
        status="active",
    )
    test_db_session.add_all([q_parent, q_child])
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9975, category_id=7703, question_id=3705)
    await _map_question_to_category(test_db_session, mapping_id=9976, category_id=7703, question_id=3706)
    await test_db_session.commit()

    test_db_session.add(AssessmentPackageCategory(package_id=1703, category_id=7703))
    test_db_session.add(
        AssessmentInstance(
            assessment_instance_id=2703,
            user_id=5073,
            package_id=1703,
            engagement_id=1,
            status="active",
        )
    )
    await test_db_session.commit()

    payload = {
        "responses": [
            {"question_id": 3705, "answer": "no"},
            {"question_id": 3706, "answer": 3},
        ]
    }
    response = await async_client.put("/questionnaire/7703/responses", headers=_auth_header(5073), json=payload)
    assert response.status_code == 422
    assert response.json()["message"] == "Question is not currently visible"


@pytest.mark.asyncio
async def test_upsert_responses_creates_new_responses(async_client, test_db_session):
    """Test creating new responses."""
    await _seed_user(test_db_session, user_id=5015)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1010, package_code="PKG010", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7010, category_key="cat_7010", display_name="Category 7010", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    q1 = QuestionnaireDefinition(
        question_id=3007,
        question_key="q3007",
        question_text="Question 1",
        question_type="text",
        status="active",
    )
    q2 = QuestionnaireDefinition(
        question_id=3008,
        question_key="q3008",
        question_text="Question 2",
        question_type="number",
        status="active",
    )
    test_db_session.add_all([q1, q2])
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9909, category_id=7010, question_id=3007)
    await _map_question_to_category(test_db_session, mapping_id=9910, category_id=7010, question_id=3008)
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=1010, category_id=7010))

    instance = AssessmentInstance(
        assessment_instance_id=2010,
        user_id=5015,
        package_id=1010,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    payload = {
        "responses": [
            {"question_id": 3007, "answer": "My answer"},
            {"question_id": 3008, "answer": 42},
        ]
    }
    response = await async_client.put("/questionnaire/7010/responses", headers=_auth_header(5015), json=payload)
    assert response.status_code == 200
    assert "saved successfully" in response.json()["data"]["message"]

    # Verify responses were created
    from sqlalchemy import select
    result = await test_db_session.execute(
        select(QuestionnaireResponse).where(QuestionnaireResponse.assessment_instance_id == 2010)
    )
    responses = list(result.scalars().all())
    assert len(responses) == 2
    
    # Verify no submission timestamp (draft mode)
    for resp in responses:
        assert resp.submitted_at is None


@pytest.mark.asyncio
async def test_upsert_responses_updates_existing_responses(async_client, test_db_session):
    """Test updating existing draft responses."""
    await _seed_user(test_db_session, user_id=5016)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1011, package_code="PKG011", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7011, category_key="cat_7011", display_name="Category 7011", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    q1 = QuestionnaireDefinition(
        question_id=3009,
        question_key="q3009",
        question_text="Question 1",
        question_type="text",
        status="active",
    )
    test_db_session.add(q1)
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9911, category_id=7011, question_id=3009)
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=1011, category_id=7011))

    instance = AssessmentInstance(
        assessment_instance_id=2011,
        user_id=5016,
        package_id=1011,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)

    # Create existing response
    existing_response = QuestionnaireResponse(
        assessment_instance_id=2011,
        question_id=3009,
        category_id=7011,
        answer="Old answer",
        submitted_at=None,
    )
    test_db_session.add(existing_response)
    await test_db_session.commit()

    # Update the response
    payload = {"responses": [{"question_id": 3009, "answer": "New answer"}]}
    response = await async_client.put("/questionnaire/7011/responses", headers=_auth_header(5016), json=payload)
    assert response.status_code == 200

    # Verify response was updated
    from sqlalchemy import select
    result = await test_db_session.execute(
        select(QuestionnaireResponse)
        .where(QuestionnaireResponse.assessment_instance_id == 2011)
        .where(QuestionnaireResponse.question_id == 3009)
    )
    updated = result.scalar_one()
    assert updated.answer == "New answer"
    assert updated.submitted_at is None


# ==================== POST /questionnaire/{assessment_instance_id}/submit Tests ====================


@pytest.mark.asyncio
async def test_submit_questionnaire_requires_auth(async_client):
    """Test that authentication is required."""
    response = await async_client.post("/questionnaire/1/submit")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_submit_questionnaire_returns_404_when_assessment_not_found(async_client, test_db_session):
    """Test 404 when assessment instance does not exist."""
    await _seed_user(test_db_session, user_id=5017)

    response = await async_client.post("/questionnaire/99999/submit", headers=_auth_header(5017))
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_submit_questionnaire_returns_403_when_not_owner(async_client, test_db_session):
    """Test 403 when user tries to submit another user's assessment."""
    await _seed_user(test_db_session, user_id=5018)
    await _seed_user(test_db_session, user_id=5019)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1012, package_code="PKG012", display_name="Test Package", status="active")
    test_db_session.add(package)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2012,
        user_id=5019,
        package_id=1012,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    response = await async_client.post("/questionnaire/2012/submit", headers=_auth_header(5018))
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_submit_questionnaire_returns_422_when_already_completed(async_client, test_db_session):
    """Test 422 when assessment is already completed."""
    await _seed_user(test_db_session, user_id=5020)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1013, package_code="PKG013", display_name="Test Package", status="active")
    test_db_session.add(package)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2013,
        user_id=5020,
        package_id=1013,
        engagement_id=1,
        status="completed",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    response = await async_client.post("/questionnaire/2013/submit", headers=_auth_header(5020))
    assert response.status_code == 422
    assert "already completed" in response.json()["message"]


@pytest.mark.asyncio
async def test_submit_questionnaire_returns_422_when_not_active(async_client, test_db_session):
    """Test 422 when assessment is not active."""
    await _seed_user(test_db_session, user_id=5021)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1014, package_code="PKG014", display_name="Test Package", status="active")
    test_db_session.add(package)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2014,
        user_id=5021,
        package_id=1014,
        engagement_id=1,
        status="inactive",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    response = await async_client.post("/questionnaire/2014/submit", headers=_auth_header(5021))
    assert response.status_code == 422
    assert "not active" in response.json()["message"]


@pytest.mark.asyncio
async def test_submit_questionnaire_marks_assessment_completed(async_client, test_db_session):
    """Test successful submission marks assessment as completed."""
    await _seed_user(test_db_session, user_id=5022)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1015, package_code="PKG015", display_name="Test Package", status="active")
    category = QuestionnaireCategory(category_id=7015, category_key="cat_7015", display_name="Category 7015", status="active")
    test_db_session.add(package)
    test_db_session.add(category)
    await test_db_session.commit()
    q1 = QuestionnaireDefinition(
        question_id=3010,
        question_key="q3010",
        question_text="Question 1",
        question_type="text",
        status="active",
    )
    test_db_session.add(q1)
    await test_db_session.commit()
    await _map_question_to_category(test_db_session, mapping_id=9912, category_id=7015, question_id=3010)
    await test_db_session.commit()
    test_db_session.add(AssessmentPackageCategory(package_id=1015, category_id=7015))

    instance = AssessmentInstance(
        assessment_instance_id=2015,
        user_id=5022,
        package_id=1015,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)

    # Add a response
    response_row = QuestionnaireResponse(
        assessment_instance_id=2015,
        question_id=3010,
        category_id=7015,
        answer="My answer",
        submitted_at=None,
    )
    test_db_session.add(response_row)
    await test_db_session.commit()

    response = await async_client.post("/questionnaire/2015/submit", headers=_auth_header(5022))
    assert response.status_code == 200
    assert "submitted successfully" in response.json()["data"]["message"]

    # Verify assessment is completed
    await test_db_session.refresh(instance)
    assert instance.status == "completed"
    assert instance.completed_at is not None

    # Verify response has submission timestamp
    await test_db_session.refresh(response_row)
    assert response_row.submitted_at is not None


@pytest.mark.asyncio
async def test_submit_questionnaire_with_no_responses(async_client, test_db_session):
    """Test submission is allowed even with no responses."""
    await _seed_user(test_db_session, user_id=5023)
    await _ensure_test_engagement(test_db_session)
    package = AssessmentPackage(package_id=1016, package_code="PKG016", display_name="Test Package", status="active")
    test_db_session.add(package)
    await test_db_session.commit()
    instance = AssessmentInstance(
        assessment_instance_id=2016,
        user_id=5023,
        package_id=1016,
        engagement_id=1,
        status="active",
    )
    test_db_session.add(instance)
    await test_db_session.commit()

    response = await async_client.post("/questionnaire/2016/submit", headers=_auth_header(5023))
    assert response.status_code == 200

    # Verify assessment is completed
    await test_db_session.refresh(instance)
    assert instance.status == "completed"
