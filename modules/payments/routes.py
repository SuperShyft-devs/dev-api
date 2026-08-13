"""Payments HTTP routes (FastAPI)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user, get_optional_user
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee, get_employee_service
from modules.employee.service import EmployeeContext, EmployeeService
from modules.payments.services import PaymentsService

# Prefix /payments (not /api/payments): dev-admin Vite proxy strips /api from the request path.
router = APIRouter(prefix="/payments", tags=["payments"])


def get_payments_service() -> PaymentsService:
    return PaymentsService()


class CreateOrderLineItem(BaseModel):
    user_id: int = Field(..., ge=1)
    entity_type: str = Field(..., min_length=1)
    entity_id: int = Field(..., ge=1)


class CreateOrderRequest(BaseModel):
    """Payer user_id plus at least one line (member user_id + entity)."""

    user_id: int = Field(..., ge=1)
    items: list[CreateOrderLineItem] = Field(..., min_length=1, max_length=10)
    discount_code: str | None = None
    organization_id: int | None = None
    camp_no: str | None = None
    engagement_id: int | None = None
    city: str | None = None

    @model_validator(mode="after")
    def unique_line_user_ids(self) -> "CreateOrderRequest":
        ids = [line.user_id for line in self.items]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate user_id in items")
        return self


class VerifyPaymentRequest(BaseModel):
    razorpay_payment_id: str = Field(..., min_length=1)
    razorpay_order_id: str = Field(..., min_length=1)
    razorpay_signature: str = Field(..., min_length=1)


class FailedPaymentRequest(BaseModel):
    razorpay_order_id: str = Field(..., min_length=1)
    failure_reason: str | None = None


@router.post("/create-order")
async def create_order(
    body: CreateOrderRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
    current_user=Depends(get_current_user),
):
    try:
        from core.network import get_client_ip

        discount_context = None
        if body.discount_code or body.organization_id or body.camp_no or body.engagement_id:
            discount_context = {
                "organization_id": body.organization_id,
                "camp_no": body.camp_no,
                "engagement_id": body.engagement_id,
                "city": body.city,
            }
        result = await service.create_order(
            db,
            payer_user_id=body.user_id,
            items=[
                (line.user_id, line.entity_type.strip(), line.entity_id) for line in body.items
            ],
            authenticated_user_id=current_user.user_id,
            discount_code=body.discount_code.strip() if body.discount_code else None,
            discount_context=discount_context,
            client_ip=get_client_ip(request),
        )
        err = result.get("_error")
        if err:
            await db.rollback()
            code, msg = err
            payload = {"success": False, "message": msg}
            return JSONResponse(status_code=code, content=payload)
        await db.commit()
        return result
    except Exception:
        await db.rollback()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )


@router.post("/verify")
async def verify_payment(
    body: VerifyPaymentRequest,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
    current_user=Depends(get_current_user),
):
    try:
        result = await service.verify_payment(
            db,
            razorpay_payment_id=body.razorpay_payment_id.strip(),
            razorpay_order_id=body.razorpay_order_id.strip(),
            razorpay_signature=body.razorpay_signature.strip(),
            authenticated_user_id=current_user.user_id,
        )
        if result.get("_signature_invalid"):
            await db.rollback()
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": "Invalid payment signature"},
            )
        err = result.get("_error")
        if err:
            await db.rollback()
            code, msg = err
            return JSONResponse(
                status_code=code,
                content={"success": False, "message": msg},
            )
        await db.commit()
        return result
    except Exception:
        await db.rollback()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )


@router.post("/failed")
async def payment_failed(
    body: FailedPaymentRequest,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
    current_user=Depends(get_current_user),
):
    try:
        result = await service.record_failure(
            db,
            razorpay_order_id=body.razorpay_order_id.strip(),
            failure_reason=body.failure_reason,
            authenticated_user_id=current_user.user_id,
        )
        err = result.get("_error")
        if err:
            await db.rollback()
            code, msg = err
            return JSONResponse(
                status_code=code,
                content={"success": False, "message": msg},
            )
        await db.commit()
        return result
    except Exception:
        await db.rollback()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )


@router.get("/bookings")
async def list_bookings(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    search: str | None = None,
    status: str | None = None,
    sort_key: str = Query("booking_id"),
    sort_dir: str = Query("desc"),
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
    _employee: EmployeeContext = Depends(get_current_employee),
):
    if sort_key != "booking_id":
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error_code": "INVALID_INPUT", "message": "Invalid sort_key"},
        )
    if sort_dir.lower() not in ("asc", "desc"):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error_code": "INVALID_INPUT", "message": "Invalid sort_dir"},
        )
    payload = await service.list_bookings_admin(
        db,
        page=page,
        limit=limit,
        search=search,
        status=status,
        sort_key=sort_key,
        sort_dir=sort_dir,
    )
    return success_response(payload)


@router.get("/booking/{booking_id}/status")
async def booking_status(
    booking_id: int,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
    user=Depends(get_current_user),
    employee_service: EmployeeService = Depends(get_employee_service),
):
    is_employee = False
    try:
        await employee_service.get_active_employee_by_user_id(db, user.user_id)
        is_employee = True
    except AppError:
        pass

    try:
        data = await service.get_booking_status(
            db,
            booking_id=booking_id,
            include_user_detail=is_employee,
            requesting_user_id=user.user_id,
            is_employee=is_employee,
        )
        if data is None:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={"success": False, "message": "Booking not found"},
            )
        return data
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )
