"""Experts HTTP routes."""

from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee, get_optional_employee_if_authenticated
from modules.employee.service import EmployeeContext
from modules.experts.dependencies import get_experts_service
from modules.experts.schemas import (
    ExpertCreateRequest,
    ExpertReviewCreateRequest,
    ExpertStatusUpdateRequest,
    ExpertTagCreateRequest,
    ExpertUpdateRequest,
)
from modules.experts.service import ExpertsService
from modules.users.models import User


router = APIRouter(prefix="/experts", tags=["experts"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


def _decimal_to_float(value: Decimal | None) -> float:
    if value is None:
        return 0.0
    return float(value)


def _expert_dict(expert) -> dict:
    return {
        "expert_id": expert.expert_id,
        "user_id": expert.user_id,
        "expert_type": expert.expert_type,
        "display_name": expert.display_name,
        "profile_photo": expert.profile_photo,
        "rating": _decimal_to_float(expert.rating),
        "review_count": expert.review_count,
        "patient_count": expert.patient_count,
        "experience_years": expert.experience_years,
        "qualifications": expert.qualifications,
        "about_text": expert.about_text,
        "consultation_modes": expert.consultation_modes,
        "languages": expert.languages,
        "session_duration_mins": expert.session_duration_mins,
        "appointment_fee_paise": expert.appointment_fee_paise,
        "original_fee_paise": expert.original_fee_paise,
        "status": expert.status,
        "created_at": expert.created_at,
        "updated_at": expert.updated_at,
    }


def _tag_dict(tag) -> dict:
    return {
        "tag_id": tag.tag_id,
        "expert_id": tag.expert_id,
        "tag_name": tag.tag_name,
        "display_order": tag.display_order,
    }


def _review_dict(review) -> dict:
    return {
        "review_id": review.review_id,
        "expert_id": review.expert_id,
        "user_id": review.user_id,
        "rating": _decimal_to_float(review.rating),
        "review_text": review.review_text,
        "created_at": review.created_at,
    }


@router.get("")
async def list_experts(
    request: Request,
    page: int = 1,
    limit: int = 20,
    expert_type: str | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext | None = Depends(get_optional_employee_if_authenticated),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    _ = request
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    status_param = status if employee is not None else None
    experts, total = await experts_service.list_experts(
        db,
        employee=employee,
        page=page,
        limit=limit,
        expert_type=expert_type,
        status_query=status_param,
    )
    return success_response([_expert_dict(e) for e in experts], meta={"page": page, "limit": limit, "total": total})


@router.get("/{expert_id}")
async def get_expert(
    expert_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext | None = Depends(get_optional_employee_if_authenticated),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    expert, tags = await experts_service.get_expert_detail(db, employee=employee, expert_id=expert_id)
    data = _expert_dict(expert)
    data["expertise_tags"] = [_tag_dict(t) for t in tags]
    return success_response(data)


@router.post("", status_code=201)
async def create_expert(
    payload: ExpertCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    expert = await experts_service.create_expert_for_employee(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"expert_id": expert.expert_id})


@router.put("/{expert_id}")
async def update_expert(
    expert_id: int,
    payload: ExpertUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    expert = await experts_service.update_expert_for_employee(
        db,
        employee=employee,
        expert_id=expert_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"expert_id": expert.expert_id})


@router.patch("/{expert_id}/status")
async def patch_expert_status(
    expert_id: int,
    payload: ExpertStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    expert = await experts_service.patch_expert_status_for_employee(
        db,
        employee=employee,
        expert_id=expert_id,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"expert_id": expert.expert_id, "status": expert.status})


@router.post("/{expert_id}/tags", status_code=201)
async def add_expert_tag(
    expert_id: int,
    payload: ExpertTagCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    tag = await experts_service.add_tag_for_employee(
        db,
        employee=employee,
        expert_id=expert_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(_tag_dict(tag))


@router.delete("/{expert_id}/tags/{tag_id}")
async def delete_expert_tag(
    expert_id: int,
    tag_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    await experts_service.delete_tag_for_employee(
        db,
        employee=employee,
        expert_id=expert_id,
        tag_id=tag_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"tag_id": tag_id})


@router.get("/{expert_id}/reviews")
async def list_expert_reviews(
    expert_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext | None = Depends(get_optional_employee_if_authenticated),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
    reviews, total = await experts_service.list_reviews(
        db, employee=employee, expert_id=expert_id, page=page, limit=limit
    )
    return success_response([_review_dict(r) for r in reviews], meta={"page": page, "limit": limit, "total": total})


@router.post("/{expert_id}/reviews", status_code=201)
async def create_expert_review(
    expert_id: int,
    payload: ExpertReviewCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    review = await experts_service.create_review_for_user(
        db,
        user_id=current_user.user_id,
        expert_id=expert_id,
        payload=payload,
    )
    await db.commit()
    return success_response(_review_dict(review))
