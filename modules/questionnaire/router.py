"""Questionnaire HTTP routes.

These endpoints are employee-only.
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
    QuestionnaireQuestionCreateRequest,
    QuestionnaireQuestionStatusUpdateRequest,
    QuestionnaireQuestionUpdateRequest,
    QuestionnaireResponsesUpsertRequest,
)
from modules.questionnaire.service import QuestionnaireService
from core.dependencies import get_current_user


router = APIRouter(prefix="/questionnaire", tags=["questionnaire"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("/questions", status_code=201)
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


@router.get("/questions")
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

    data = []
    for row in rows:
        data.append(
            {
                "question_id": row.question_id,
                "question_text": row.question_text,
                "question_type": row.question_type,
                "options": row.options,
                "status": row.status,
                "created_at": row.created_at,
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.get("/questions/{question_id}")
async def get_question(
    question_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: QuestionnaireService = Depends(get_questionnaire_management_service),
):
    row = await service.get_question_definition(db, employee=employee, question_id=question_id)

    return success_response(
        {
            "question_id": row.question_id,
            "question_text": row.question_text,
            "question_type": row.question_type,
            "options": row.options,
            "status": row.status,
            "created_at": row.created_at,
        }
    )


@router.put("/questions/{question_id}")
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


@router.patch("/questions/{question_id}/status")
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


# User-facing endpoints for questionnaire responses

@router.get("/{assessment_instance_id}")
async def get_questionnaire(
    assessment_instance_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: QuestionnaireService = Depends(get_questionnaire_user_service),
):
    """Get questionnaire questions and existing draft answers for a user.
    
    Security: User authentication required. Access control enforced in service layer.
    """
    result = await service.get_questionnaire_for_user(
        db,
        user_id=current_user.user_id,
        assessment_instance_id=assessment_instance_id,
    )
    
    return success_response(result)


@router.put("/{assessment_instance_id}/responses")
async def upsert_questionnaire_responses(
    assessment_instance_id: int,
    payload: QuestionnaireResponsesUpsertRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: QuestionnaireService = Depends(get_questionnaire_user_service),
):
    """Create or update draft answers for a questionnaire.
    
    Security: User authentication required. Access control enforced in service layer.
    Responses are editable until submit is called.
    """
    # Convert schema to dict for service layer
    responses_data = [
        {"question_id": item.question_id, "answer": item.normalized_answer()}
        for item in payload.responses
    ]
    
    await service.upsert_responses_for_user(
        db,
        user_id=current_user.user_id,
        assessment_instance_id=assessment_instance_id,
        responses=responses_data,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    
    await db.commit()
    
    return success_response({"message": "Responses saved successfully"})


@router.post("/{assessment_instance_id}/submit")
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
