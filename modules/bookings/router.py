"""Authenticated batch booking endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.rate_limit import limiter
from db.session import get_db
from modules.bookings.schemas import (
    AvailableSlotsRequest,
    BookPayRequest,
    CancelBookingRequest,
    CheckServiceabilityRequest,
    LockSlotRequest,
    VerifyAndBookRequest,
)
from modules.bookings import service as booking_service
from modules.engagements.dependencies import get_engagements_repository, get_engagements_service
from modules.engagements.models import EngagementKind
from modules.engagements.repository import EngagementsRepository
from modules.engagements.service import EngagementsService


router = APIRouter(prefix="/book", tags=["bookings"])


@router.post("/pay")
@limiter.limit("5/minute")
async def book_pay(
    payload: BookPayRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    members = [{"user_id": m.user_id, "engagement_id": m.engagement_id} for m in payload.members]
    result = await booking_service.create_pay_order_for_draft_engagements(
        db, members=members, payer_user_id=current_user.user_id
    )
    await db.commit()
    return success_response(result)


@router.post("/bio-ai")
@limiter.limit("5/minute")
async def book_bio_ai_batch(
    payload: VerifyAndBookRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    result = await booking_service.verify_and_finalize_draft_bookings(
        db,
        razorpay_payment_id=payload.razorpay_payment_id,
        razorpay_order_id=payload.razorpay_order_id,
        razorpay_signature=payload.razorpay_signature,
        caller_user_id=current_user.user_id,
        engagement_type=EngagementKind.bio_ai,
        engagements_service=engagements_service,
    )
    await db.commit()
    return success_response(result)


@router.post("/blood-test")
@limiter.limit("5/minute")
async def book_blood_test_batch(
    payload: VerifyAndBookRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    result = await booking_service.verify_and_finalize_draft_bookings(
        db,
        razorpay_payment_id=payload.razorpay_payment_id,
        razorpay_order_id=payload.razorpay_order_id,
        razorpay_signature=payload.razorpay_signature,
        caller_user_id=current_user.user_id,
        engagement_type=EngagementKind.blood_test,
        engagements_service=engagements_service,
    )
    await db.commit()
    return success_response(result)


@router.post("/cancel/bio-ai")
@limiter.limit("5/minute")
async def cancel_bio_ai_bookings(
    payload: CancelBookingRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    repository: EngagementsRepository = Depends(get_engagements_repository),
):
    members = [
        {"user_id": m.user_id, "engagement_id": m.engagement_id, "remarks": m.remarks}
        for m in payload.members
    ]
    result = await booking_service.cancel_healthians_bookings_batch(
        db,
        members=members,
        caller_user_id=current_user.user_id,
        repository=repository,
    )
    await db.commit()
    return success_response({"members": result})


@router.post("/cancel/blood-test")
@limiter.limit("5/minute")
async def cancel_blood_test_bookings(
    payload: CancelBookingRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    repository: EngagementsRepository = Depends(get_engagements_repository),
):
    members = [
        {"user_id": m.user_id, "engagement_id": m.engagement_id, "remarks": m.remarks}
        for m in payload.members
    ]
    result = await booking_service.cancel_healthians_bookings_batch(
        db,
        members=members,
        caller_user_id=current_user.user_id,
        repository=repository,
    )
    await db.commit()
    return success_response({"members": result})


@router.get("/me/drafts")
async def get_my_drafts(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    engagements = await booking_service.get_user_draft_engagements(
        db, user_id=current_user.user_id
    )
    return success_response({"engagements": engagements})


@router.post("/check-service-availability")
@limiter.limit("5/minute")
async def check_service_availability(
    payload: CheckServiceabilityRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    engagements_service: EngagementsService = Depends(get_engagements_service),
):
    members = [
        {
            "user_id": m.user_id,
            "house_flat_no": m.house_flat_no,
            "building_area": m.building_area,
            "landmark": m.landmark,
            "city": m.city,
            "pincode": m.pincode,
            "diagnostic_package_id": m.diagnostic_package_id,
        }
        for m in payload.members
    ]
    result = await booking_service.check_service_availability(
        db, members=members, engagements_service=engagements_service,
        booked_by_user_id=current_user.user_id,
    )
    await db.commit()
    return success_response({"members": result})


@router.post("/available-slots")
@limiter.limit("10/minute")
async def get_available_slots(
    payload: AvailableSlotsRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    members = [
        {
            "user_id": m.user_id,
            "engagement_id": m.engagement_id,
            "blood_collection_date": m.blood_collection_date,
        }
        for m in payload.members
    ]
    result = await booking_service.get_available_slots(db, members=members)
    await db.commit()
    return success_response({"members": result})


@router.post("/lock")
@limiter.limit("5/minute")
async def lock_slots(
    payload: LockSlotRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    members = [
        {
            "user_id": m.user_id,
            "engagement_id": m.engagement_id,
            "blood_collection_date": m.blood_collection_date,
            "blood_collection_time_slot_id": m.blood_collection_time_slot_id,
            "blood_collection_time_slot": m.blood_collection_time_slot,
        }
        for m in payload.members
    ]
    result = await booking_service.lock_slots(db, members=members)
    await db.commit()
    return success_response({"members": result})
