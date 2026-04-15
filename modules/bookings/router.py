"""Authenticated batch booking endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.users.dependencies import get_users_service
from modules.users.schemas import BookBioAiBatchRequest, BookBloodTestBatchRequest
from modules.users.service import UsersService


router = APIRouter(prefix="/book", tags=["bookings"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.post("/bio-ai")
async def book_bio_ai_batch(
    payload: BookBioAiBatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    results = await users_service.book_bio_ai_batch_for_primary(
        db,
        actor=current_user,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"results": [r.model_dump(exclude_none=True) for r in results]})


@router.post("/blood-test")
async def book_blood_test_batch(
    payload: BookBloodTestBatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    users_service: UsersService = Depends(get_users_service),
):
    results = await users_service.book_blood_test_batch_for_primary(
        db,
        actor=current_user,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"results": [r.model_dump(exclude_none=True) for r in results]})
