"""Engagement console HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.assessments.schemas import AssessmentSubmitRequest
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.engagements.console.schemas import ConsoleParticipantBookRequest
from modules.engagements.console.service import ConsoleService
from modules.engagements.dependencies import get_console_service
from modules.questionnaire.schemas import QuestionnaireResponsesUpsertRequest

router = APIRouter(prefix="/engagements", tags=["engagement-console"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.get("/console/engagements")
async def list_console_engagements(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.list_console_engagements(db, employee=employee)
    return success_response(data)


@router.get("/{engagement_id}/console")
async def get_engagement_for_console(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.get_engagement_for_console(
        db,
        employee=employee,
        engagement_id=engagement_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/console/participants")
async def get_console_participants(
    engagement_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await console_service.list_participants_for_console(
        db,
        employee=employee,
        engagement_id=engagement_id,
        page=page,
        limit=limit,
    )

    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.post("/{engagement_id}/console/participants/{user_id}/book")
async def book_console_participant(
    engagement_id: int,
    user_id: int,
    payload: ConsoleParticipantBookRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.book_participant(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        barcode=payload.barcode,
    )
    await db.commit()
    return success_response(data)


@router.delete("/{engagement_id}/console/participants/{user_id}/book")
async def cancel_console_participant_booking(
    engagement_id: int,
    user_id: int,
    remarks: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    if not remarks.strip():
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="remarks is required")
    data = await console_service.cancel_participant_booking(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        remarks=remarks.strip(),
    )
    await db.commit()
    return success_response(data)


@router.get("/{engagement_id}/console/participants/{user_id}/assessments")
async def list_console_participant_assessments(
    engagement_id: int,
    user_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.list_participant_assessments(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
    )
    return success_response(data)


@router.get("/{engagement_id}/console/participants/{user_id}/assessments/{assessment_instance_id}/status")
async def get_console_participant_assessment_status(
    engagement_id: int,
    user_id: int,
    assessment_instance_id: int,
    category_of: str = "supershyft",
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.get_participant_assessment_status(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        assessment_instance_id=assessment_instance_id,
        category_of=category_of,
    )
    return success_response(data)


@router.get(
    "/{engagement_id}/console/participants/{user_id}/questionnaire/{assessment_instance_id}/category/{category_id}"
)
async def get_console_participant_questionnaire(
    engagement_id: int,
    user_id: int,
    assessment_instance_id: int,
    category_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.get_participant_questionnaire(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        assessment_instance_id=assessment_instance_id,
        category_id=category_id,
    )
    return success_response(data)


@router.put(
    "/{engagement_id}/console/participants/{user_id}/questionnaire/{assessment_instance_id}/category/{category_id}/responses"
)
async def upsert_console_participant_questionnaire_responses(
    engagement_id: int,
    user_id: int,
    assessment_instance_id: int,
    category_id: int,
    payload: QuestionnaireResponsesUpsertRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    responses_data = [
        {"question_id": item.question_id, "answer": item.normalized_answer()}
        for item in payload.responses
    ]

    await console_service.upsert_participant_questionnaire_responses(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        assessment_instance_id=assessment_instance_id,
        category_id=category_id,
        responses=responses_data,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"message": "Responses saved successfully"})


@router.post("/{engagement_id}/console/participants/{user_id}/assessments/{assessment_instance_id}/submit")
async def submit_console_participant_assessment(
    engagement_id: int,
    user_id: int,
    assessment_instance_id: int,
    body: AssessmentSubmitRequest,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    console_service: ConsoleService = Depends(get_console_service),
):
    data = await console_service.submit_participant_assessment_category(
        db,
        employee=employee,
        engagement_id=engagement_id,
        user_id=user_id,
        assessment_instance_id=assessment_instance_id,
        category=body.category,
        category_of=body.category_of,
    )
    await db.commit()
    return success_response(data)
