"""Authenticated batch booking endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.network import get_client_ip
from core.rate_limit import limiter
from db.session import get_db
from modules.users.dependencies import get_users_service
from modules.users.schemas import BookBioAiBatchRequest, BookBloodTestBatchRequest
from modules.users.service import UsersService


router = APIRouter(prefix="/book", tags=["bookings"])


@router.post("/bio-ai")
@limiter.limit("5/minute")
async def book_bio_ai_batch(
    payload: BookBioAiBatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.book_bio_ai_batch_for_primary(
        db,
        actor=current_user,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result.model_dump())


@router.post("/blood-test")
@limiter.limit("5/minute")
async def book_blood_test_batch(
    payload: BookBloodTestBatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    result = await users_service.book_blood_test_batch_for_primary(
        db,
        actor=current_user,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result.model_dump())
