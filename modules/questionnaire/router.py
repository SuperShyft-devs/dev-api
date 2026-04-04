"""Questionnaire HTTP routes.

Employee endpoints manage question/category definitions; authenticated users
fill and submit questionnaires under the same `/questionnaire` prefix.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.questionnaire.dependencies import (
    get_questionnaire_management_service,
    get_questionnaire_user_service,
)
from modules.questionnaire.schemas import (
    HealthyHabitRuleCreateRequest,
    HealthyHabitRuleUpdateRequest,
    QuestionnaireCategoryCreateRequest,
    QuestionnaireCategoryQuestionsAssignRequest,
    QuestionnaireCategoryQuestionsReorderRequest,
    QuestionnaireCategoryStatusUpdateRequest,
    QuestionnaireCategoryUpdateRequest,
    QuestionnaireQuestionCreateRequest,
    QuestionnaireQuestionStatusUpdateRequest,
    QuestionnaireQuestionUpdateRequest,
    QuestionnaireResponsesUpsertRequest,
)
from modules.questionnaire.service import QuestionnaireService
from core.dependencies import get_current_user


router = APIRouter(tags=["questionnaire"])
management_router = APIRouter(prefix="/questionnaire", tags=["questionnaire"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@management_router.post("/questions", status_code=201)
async def create_question_definition(
    payload: QuestionnaireQuestionCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    created = await service.create_question_definition(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"question_id": created.question_id})


@management_router.get("/questions")
async def list_questions(
    request: Request,
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    type: str | None = None,  # noqa: A002
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    rows, total = await service.list_question_definitions(
        db,
        employee=employee,
        page=page,
        limit=limit,
        status=status,
        question_type=type,
    )

    data = [await service.serialize_question_definition(db, row) for row in rows]

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@management_router.get("/questions/{question_id}")
async def get_question(
    question_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    row = await service.get_question_definition(db, employee=employee, question_id=question_id)

    return success_response(await service.serialize_question_definition(db, row))


@management_router.put("/questions/{question_id}")
async def update_question(
    question_id: int,
    payload: QuestionnaireQuestionUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    updated = await service.update_question_definition(
        db,
        employee=employee,
        question_id=question_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"question_id": updated.question_id})


@management_router.patch("/questions/{question_id}/status")
async def update_question_status(
    question_id: int,
    payload: QuestionnaireQuestionStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    updated = await service.change_question_status(
        db,
        employee=employee,
        question_id=question_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"question_id": updated.question_id, "status": updated.status})


@management_router.get("/questions/{question_id}/healthy-habit-rules")
async def list_healthy_habit_rules(
    question_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    data = await service.list_healthy_habit_rules(db, employee=employee, question_id=question_id)
    return success_response(data)


@management_router.post("/questions/{question_id}/healthy-habit-rules", status_code=201)
async def create_healthy_habit_rule(
    question_id: int,
    payload: HealthyHabitRuleCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    data = await service.create_healthy_habit_rule(
        db,
        employee=employee,
        question_id=question_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@management_router.put("/questions/{question_id}/healthy-habit-rules/{rule_id}")
async def update_healthy_habit_rule(
    question_id: int,
    rule_id: int,
    payload: HealthyHabitRuleUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    data = await service.update_healthy_habit_rule(
        db,
        employee=employee,
        question_id=question_id,
        rule_id=rule_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@management_router.delete("/questions/{question_id}/healthy-habit-rules/{rule_id}")
async def delete_healthy_habit_rule(
    question_id: int,
    rule_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    await service.delete_healthy_habit_rule(
        db,
        employee=employee,
        question_id=question_id,
        rule_id=rule_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"deleted": True})


# Category CRUD
@management_router.post("/categories", status_code=201)
async def create_category(
    payload: QuestionnaireCategoryCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    row = await service.create_category(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"category_id": row.category_id})


@management_router.get("/categories")
async def list_categories(
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    rows, total = await service.list_categories(db, employee=employee, page=page, limit=limit)
    data = [
        {
            "category_id": row.category_id,
            "category_key": row.category_key,
            "display_name": row.display_name,
            "status": row.status,
        }
        for row in rows
    ]
    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@management_router.get("/categories/{category_id}")
async def get_category(
    category_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    row = await service.get_category(db, employee=employee, category_id=category_id)
    return success_response(
        {
            "category_id": row.category_id,
            "category_key": row.category_key,
            "display_name": row.display_name,
            "status": row.status,
        }
    )


@management_router.put("/categories/{category_id}")
async def update_category(
    category_id: int,
    payload: QuestionnaireCategoryUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    row = await service.update_category(
        db,
        employee=employee,
        category_id=category_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"category_id": row.category_id})


@management_router.patch("/categories/{category_id}/status")
async def update_category_status(
    category_id: int,
    payload: QuestionnaireCategoryStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    row = await service.change_category_status(
        db,
        employee=employee,
        category_id=category_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"category_id": row.category_id, "status": row.status})


@management_router.get("/categories/{category_id}/questions")
async def list_category_questions(
    category_id: int,
    db: AsyncSession = Depends(get_db),
    _current_user=Depends(get_current_user),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    rows = await service.list_category_questions_for_user(db, category_id=category_id)
    return success_response(rows)


@management_router.post("/categories/{category_id}/questions", status_code=201)
async def assign_category_questions(
    category_id: int,
    payload: QuestionnaireCategoryQuestionsAssignRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    data = await service.assign_category_questions(
        db,
        employee=employee,
        category_id=category_id,
        question_ids=payload.question_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@management_router.delete("/categories/{category_id}/questions/{question_id}")
async def remove_category_question(
    category_id: int,
    question_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    data = await service.remove_category_question(
        db,
        employee=employee,
        category_id=category_id,
        question_id=question_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@management_router.patch("/categories/{category_id}/questions/order")
async def reorder_category_questions(
    category_id: int,
    payload: QuestionnaireCategoryQuestionsReorderRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    data = await service.reorder_category_questions(
        db,
        employee=employee,
        category_id=category_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


# User-facing endpoints for questionnaire responses

@management_router.get("/{category_id}")
async def get_questionnaire(
    category_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: QuestionnaireService = Depends(get_questionnaire_user_service),
):
    """Get category questionnaire questions and existing draft answers for a user."""
    result = await service.get_questionnaire_for_user(
        db,
        user_id=current_user.user_id,
        category_id=category_id,
    )
    
    return success_response(result)


@management_router.put("/{category_id}/responses")
async def upsert_questionnaire_responses(
    category_id: int,
    payload: QuestionnaireResponsesUpsertRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: QuestionnaireService = Depends(get_questionnaire_user_service),
):
    """Create or update draft answers for a category questionnaire."""
    # Convert schema to dict for service layer
    responses_data = [
        {"question_id": item.question_id, "answer": item.normalized_answer()}
        for item in payload.responses
    ]
    
    await service.upsert_responses_for_user(
        db,
        user_id=current_user.user_id,
        category_id=category_id,
        responses=responses_data,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    
    await db.commit()
    
    return success_response({"message": "Responses saved successfully"})


@management_router.post("/{assessment_instance_id}/submit")
async def submit_questionnaire(
    assessment_instance_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: QuestionnaireService = Depends(get_questionnaire_user_service),
):
    """Submit questionnaire and mark assessment as completed.
    
    Security: User authentication required. Access control enforced in service layer.
    This action is final and triggers Metsights integration.
    """
    await service.submit_questionnaire_for_user(
        db,
        user_id=current_user.user_id,
        assessment_instance_id=assessment_instance_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    
    await db.commit()
    
    return success_response({"message": "Questionnaire submitted successfully"})


router.include_router(management_router)
