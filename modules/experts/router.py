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
from modules.experts.dependencies import get_expert_types_service, get_experts_service, get_availability_service
from modules.experts.schemas import (
    AvailabilityBlockCreate,
    AvailabilityBulkSave,
    ExpertCreateRequest,
    ExpertReviewCreateRequest,
    ExpertStatusUpdateRequest,
    ExpertTagCreateRequest,
    ExpertTypeCreateRequest,
    ExpertTypeUpdateRequest,
    ExpertUpdateRequest,
    OverrideCreate,
)
from modules.experts.service import ExpertAvailabilityService, ExpertsService, ExpertTypesService
from modules.users.models import User


router = APIRouter(prefix="/experts", tags=["experts"])
portal_router = APIRouter(prefix="/experts/portal", tags=["experts-portal"])
expert_types_router = APIRouter(prefix="/expert-types", tags=["expert-types"])


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
        "specialization": expert.specialization,
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
        "effective_from": expert.effective_from.isoformat() if expert.effective_from else None,
        "effective_until": expert.effective_until.isoformat() if expert.effective_until else None,
        "status": expert.status,
        "created_at": expert.created_at,
        "updated_at": expert.updated_at,
    }


def _availability_block_dict(block) -> dict:
    return {
        "id": block.id,
        "expert_id": block.expert_id,
        "day_of_week": block.day_of_week,
        "start_time": block.start_time.strftime("%H:%M") if block.start_time else None,
        "end_time": block.end_time.strftime("%H:%M") if block.end_time else None,
        "slot_duration": block.slot_duration,
        "buffer_time": block.buffer_time,
    }


def _override_dict(override) -> dict:
    return {
        "id": override.id,
        "expert_id": override.expert_id,
        "override_date": override.override_date.isoformat() if override.override_date else None,
        "availability": override.availability,
        "start_time": override.start_time.strftime("%H:%M") if override.start_time else None,
        "end_time": override.end_time.strftime("%H:%M") if override.end_time else None,
        "buffer_time": override.buffer_time,
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


@portal_router.get("/me")
async def get_experts_portal_me(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
):
    expert, tags = await experts_service.get_portal_me(db, employee=employee)
    data = _expert_dict(expert)
    data["expertise_tags"] = [_tag_dict(t) for t in tags]
    return success_response(data)


@router.get("")
async def list_experts(
    request: Request,
    page: int = 1,
    limit: int = 20,
    expert_type: str | None = None,
    status: str | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
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
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
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


# ─── Admin Availability endpoints ─────────────────────────────────────────────


@router.get("/{expert_id}/availability")
async def list_expert_availability(
    expert_id: int,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    blocks = await availability_service.list_blocks(db, expert_id=expert_id)
    return success_response([_availability_block_dict(b) for b in blocks])


@router.put("/{expert_id}/availability")
async def bulk_save_expert_availability(
    expert_id: int,
    payload: AvailabilityBulkSave,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    blocks = await availability_service.bulk_save_blocks(db, expert_id=expert_id, payload=payload)
    await db.commit()
    return success_response([_availability_block_dict(b) for b in blocks])


@router.get("/{expert_id}/overrides")
async def list_expert_overrides(
    expert_id: int,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    overrides = await availability_service.list_overrides(db, expert_id=expert_id)
    return success_response([_override_dict(o) for o in overrides])


@router.post("/{expert_id}/overrides", status_code=201)
async def create_expert_override(
    expert_id: int,
    payload: OverrideCreate,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    override = await availability_service.create_override(db, expert_id=expert_id, payload=payload)
    await db.commit()
    return success_response(_override_dict(override))


@router.delete("/{expert_id}/overrides/{override_id}")
async def delete_expert_override(
    expert_id: int,
    override_id: int,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    await availability_service.delete_override(db, expert_id=expert_id, override_id=override_id)
    await db.commit()
    return success_response({"id": override_id})


# ─── Portal Availability endpoints ────────────────────────────────────────────


@portal_router.get("/availability")
async def portal_list_availability(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    blocks = await availability_service.list_blocks(db, expert_id=expert.expert_id)
    return success_response([_availability_block_dict(b) for b in blocks])


@portal_router.post("/availability", status_code=201)
async def portal_create_availability(
    payload: AvailabilityBlockCreate,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    block = await availability_service.create_block(db, expert_id=expert.expert_id, payload=payload)
    await db.commit()
    return success_response(_availability_block_dict(block))


@portal_router.put("/availability/{block_id}")
async def portal_update_availability(
    block_id: int,
    payload: AvailabilityBlockCreate,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    block = await availability_service.update_block(db, expert_id=expert.expert_id, block_id=block_id, payload=payload)
    await db.commit()
    return success_response(_availability_block_dict(block))


@portal_router.delete("/availability/{block_id}")
async def portal_delete_availability(
    block_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    await availability_service.delete_block(db, expert_id=expert.expert_id, block_id=block_id)
    await db.commit()
    return success_response({"id": block_id})


@portal_router.put("/availability")
async def portal_bulk_save_availability(
    payload: AvailabilityBulkSave,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    blocks = await availability_service.bulk_save_blocks(db, expert_id=expert.expert_id, payload=payload)
    await db.commit()
    return success_response([_availability_block_dict(b) for b in blocks])


@portal_router.get("/overrides")
async def portal_list_overrides(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    overrides = await availability_service.list_overrides(db, expert_id=expert.expert_id)
    return success_response([_override_dict(o) for o in overrides])


@portal_router.post("/overrides", status_code=201)
async def portal_create_override(
    payload: OverrideCreate,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    override = await availability_service.create_override(db, expert_id=expert.expert_id, payload=payload)
    await db.commit()
    return success_response(_override_dict(override))


@portal_router.delete("/overrides/{override_id}")
async def portal_delete_override(
    override_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    experts_service: ExpertsService = Depends(get_experts_service),
    availability_service: ExpertAvailabilityService = Depends(get_availability_service),
):
    expert, _ = await experts_service.get_portal_me(db, employee=employee)
    await availability_service.delete_override(db, expert_id=expert.expert_id, override_id=override_id)
    await db.commit()
    return success_response({"id": override_id})


# ─── Expert Types CRUD ────────────────────────────────────────────────────────


def _expert_type_dict(et) -> dict:
    return {
        "id": et.id,
        "type_key": et.type_key,
        "type": et.type,
    }


@expert_types_router.get("")
async def list_expert_types(
    db: AsyncSession = Depends(get_db),
    expert_types_service: ExpertTypesService = Depends(get_expert_types_service),
):
    items = await expert_types_service.list_expert_types(db)
    return success_response([_expert_type_dict(et) for et in items])


@expert_types_router.post("", status_code=201)
async def create_expert_type(
    payload: ExpertTypeCreateRequest,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    expert_types_service: ExpertTypesService = Depends(get_expert_types_service),
):
    et = await expert_types_service.create_expert_type(db, payload=payload)
    await db.commit()
    return success_response(_expert_type_dict(et))


@expert_types_router.put("/{expert_type_id}")
async def update_expert_type(
    expert_type_id: int,
    payload: ExpertTypeUpdateRequest,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    expert_types_service: ExpertTypesService = Depends(get_expert_types_service),
):
    et = await expert_types_service.update_expert_type(db, expert_type_id=expert_type_id, payload=payload)
    await db.commit()
    return success_response(_expert_type_dict(et))


@expert_types_router.delete("/{expert_type_id}")
async def delete_expert_type(
    expert_type_id: int,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    expert_types_service: ExpertTypesService = Depends(get_expert_types_service),
):
    await expert_types_service.delete_expert_type(db, expert_type_id=expert_type_id)
    await db.commit()
    return success_response({"id": expert_type_id})
