"""Authenticated batch booking endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.network import get_client_ip
from core.rate_limit import limiter
from db.session import get_db
from modules.bookings.schemas import (
    AvailableSlotsRequest,
    BookFromDraftRequest,
    CheckServiceabilityRequest,
    LockSlotRequest,
)
from modules.bookings import service as booking_service
from modules.engagements.dependencies import get_engagements_service
from modules.engagements.service import EngagementsService
from modules.users.dependencies import get_users_service
from modules.users.schemas import BookBioAiBatchRequest, BookBloodTestBatchRequest
from modules.users.service import UsersService


router = APIRouter(prefix="/book", tags=["bookings"])


@router.post("/bio-ai")
@limiter.limit("5/minute")
async def book_bio_ai_batch(
    payload: BookFromDraftRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    members = [{"user_id": m.user_id, "engagement_id": m.engagement_id} for m in payload.members]
    result = await booking_service.create_healthians_booking_after_payment(db, members=members)
    await db.commit()
    return success_response({"members": result})


@router.post("/blood-test")
@limiter.limit("5/minute")
async def book_blood_test_batch(
    payload: BookFromDraftRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    members = [{"user_id": m.user_id, "engagement_id": m.engagement_id} for m in payload.members]
    result = await booking_service.create_healthians_booking_after_payment(db, members=members)
    await db.commit()
    return success_response({"members": result})


@router.get("/me/drafts")
async def get_my_drafts(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
):
    engagement_ids = await booking_service.get_user_draft_engagement_ids(
        db, user_id=current_user.user_id
    )
    return success_response({"engagement_ids": engagement_ids})


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
            "address": m.address,
            "sub_locality": m.sub_locality,
            "landmark": m.landmark,
            "city": m.city,
            "state": m.state,
            "country": m.country,
            "latitude": m.latitude,
            "longitude": m.longitude,
            "diagnostic_package_id": m.diagnostic_package_id,
        }
        for m in payload.members
    ]
    result = await booking_service.check_service_availability(
        db, members=members, engagements_service=engagements_service
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
        }
        for m in payload.members
    ]
    result = await booking_service.lock_slots(db, members=members)
    await db.commit()
    return success_response({"members": result})
