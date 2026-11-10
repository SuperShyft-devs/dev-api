"""Partners HTTP routes — CRUD + OTP auth."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import PageParamsDep
from common.responses import success_response
from core.exceptions import AppError
from core.network import get_client_ip
from core.rate_limit import limiter
from db.session import get_db
from core.dependencies import get_current_partner
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.partners.auth_service import PartnerAuthService
from modules.partners.dependencies import get_partner_auth_service, get_partners_service
from modules.partners.models import Partner
from modules.partners.schemas import (
    PartnerCreateRequest,
    PartnerLogoutRequest,
    PartnerRefreshTokenRequest,
    PartnerSendOtpRequest,
    PartnerStatusUpdateRequest,
    PartnerUpdateRequest,
    PartnerVerifyOtpRequest,
)
from modules.partners.service import PartnersService

router = APIRouter(prefix="/partners", tags=["partners"])


def _partner_to_dict(row) -> dict:
    return {
        "partner_id": row.partner_id,
        "name": row.name,
        "phone": row.phone,
        "email": row.email,
        "role": row.role,
        "status": row.status,
        "created_at": getattr(row, "created_at", None),
        "updated_at": getattr(row, "updated_at", None),
    }


def _tokens_payload(tokens) -> dict:
    return {
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "token_type": "bearer",
    }


# ── Auth (registered before /{partner_id} routes) ───────────────────────


@router.post("/auth/send-otp")
@limiter.limit("5/minute")
async def partner_send_otp(
    payload: PartnerSendOtpRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: PartnerAuthService = Depends(get_partner_auth_service),
):
    session_id, delivery = await auth_service.send_otp(
        db,
        phone=payload.phone,
        email=str(payload.email) if payload.email else None,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    if delivery is not None:
        await auth_service.deliver_otp(
            db,
            delivery=delivery,
            request_phone=str(payload.phone) if payload.phone else None,
            request_email=str(payload.email) if payload.email else None,
        )
        await db.commit()
    return success_response({"session_id": session_id})


@router.post("/auth/verify-otp")
@limiter.limit("10/minute")
async def partner_verify_otp(
    payload: PartnerVerifyOtpRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: PartnerAuthService = Depends(get_partner_auth_service),
):
    partner, tokens = await auth_service.verify_otp(
        db,
        phone=payload.phone,
        email=str(payload.email) if payload.email else None,
        otp=payload.otp,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        {
            "partner_id": partner.partner_id,
            "name": partner.name,
            "role": partner.role,
            "tokens": _tokens_payload(tokens),
        }
    )


@router.get("/auth/me")
async def partner_auth_me(
    partner: Partner = Depends(get_current_partner),
):
    return success_response(_partner_to_dict(partner))


@router.post("/auth/refresh-token")
async def partner_refresh_token(
    payload: PartnerRefreshTokenRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: PartnerAuthService = Depends(get_partner_auth_service),
):
    partner, tokens = await auth_service.refresh_tokens(
        db,
        refresh_token=payload.refresh_token,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        {
            "partner_id": partner.partner_id,
            "role": partner.role,
            "tokens": _tokens_payload(tokens),
        }
    )


@router.post("/auth/logout")
async def partner_logout(
    payload: PartnerLogoutRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: PartnerAuthService = Depends(get_partner_auth_service),
):
    await auth_service.logout(
        db,
        refresh_token=payload.refresh_token,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"success": True})


# ── CRUD ────────────────────────────────────────────────────────────────


@router.post("", status_code=201)
async def create_partner(
    payload: PartnerCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    partners_service: PartnersService = Depends(get_partners_service),
):
    created, expert_id = await partners_service.create_partner(
        db,
        employee=employee,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    data: dict = {"partner_id": created.partner_id}
    if expert_id is not None:
        data["expert_id"] = expert_id
    return success_response(data)


@router.get("")
async def list_partners(
    pagination: PageParamsDep,
    status: str | None = None,
    role: str | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    partners_service: PartnersService = Depends(get_partners_service),
):
    partners, total = await partners_service.list_partners(
        db,
        employee=employee,
        page=pagination.page,
        limit=pagination.limit,
        status=status,
        role=role,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return success_response(
        [_partner_to_dict(row) for row in partners],
        meta={"page": pagination.page, "limit": pagination.limit, "total": total},
    )


@router.get("/{partner_id}")
async def get_partner(
    partner_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    partners_service: PartnersService = Depends(get_partners_service),
):
    row, expert_id = await partners_service.get_partner_details(db, employee=employee, partner_id=partner_id)
    payload = _partner_to_dict(row)
    payload["has_expert_profile"] = expert_id is not None
    payload["expert_id"] = expert_id
    return success_response(payload)


@router.put("/{partner_id}")
async def update_partner(
    partner_id: int,
    payload: PartnerUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    partners_service: PartnersService = Depends(get_partners_service),
):
    updated, expert_id = await partners_service.update_partner(
        db,
        employee=employee,
        partner_id=partner_id,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    data: dict = {"partner_id": updated.partner_id}
    if expert_id is not None:
        data["expert_id"] = expert_id
    return success_response(data)


@router.patch("/{partner_id}/status")
async def update_partner_status(
    partner_id: int,
    payload: PartnerStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    partners_service: PartnersService = Depends(get_partners_service),
):
    updated = await partners_service.change_partner_status(
        db,
        employee=employee,
        partner_id=partner_id,
        status=payload.status,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"partner_id": updated.partner_id, "status": updated.status})
