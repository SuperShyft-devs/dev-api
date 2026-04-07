"""Payments HTTP routes (FastAPI)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from db.session import get_db
from modules.payments.services import PaymentsService

router = APIRouter(prefix="/api/payments", tags=["payments"])


def get_payments_service() -> PaymentsService:
    return PaymentsService()


class CreateOrderRequest(BaseModel):
    user_id: int = Field(..., ge=1)
    entity_type: str = Field(..., min_length=1)
    entity_id: int = Field(..., ge=1)


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
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
):
    try:
        result = await service.create_order(
            db,
            user_id=body.user_id,
            entity_type=body.entity_type.strip(),
            entity_id=body.entity_id,
        )
        err = result.get("_error")
        if err:
            code, msg = err
            payload = {"success": False, "message": msg}
            return JSONResponse(status_code=code, content=payload)
        return result
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )


@router.post("/verify")
async def verify_payment(
    body: VerifyPaymentRequest,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
):
    try:
        result = await service.verify_payment(
            db,
            razorpay_payment_id=body.razorpay_payment_id.strip(),
            razorpay_order_id=body.razorpay_order_id.strip(),
            razorpay_signature=body.razorpay_signature.strip(),
        )
        if result.get("_signature_invalid"):
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"success": False, "message": "Invalid payment signature"},
            )
        err = result.get("_error")
        if err:
            code, msg = err
            return JSONResponse(
                status_code=code,
                content={"success": False, "message": msg},
            )
        return result
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )


@router.post("/failed")
async def payment_failed(
    body: FailedPaymentRequest,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
):
    try:
        result = await service.record_failure(
            db,
            razorpay_order_id=body.razorpay_order_id.strip(),
            failure_reason=body.failure_reason,
        )
        err = result.get("_error")
        if err:
            code, msg = err
            return JSONResponse(
                status_code=code,
                content={"success": False, "message": msg},
            )
        return result
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "message": "Internal server error"},
        )


@router.get("/booking/{booking_id}/status")
async def booking_status(
    booking_id: int,
    db: AsyncSession = Depends(get_db),
    service: PaymentsService = Depends(get_payments_service),
):
    try:
        data = await service.get_booking_status(db, booking_id=booking_id)
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
