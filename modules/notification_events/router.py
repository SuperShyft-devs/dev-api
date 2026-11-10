"""Deprecated auto notification events CRUD — replaced by hardcoded catalog."""

from __future__ import annotations

from fastapi import APIRouter

from core.exceptions import AppError

router = APIRouter(prefix="/notification-events", tags=["notification-events"])


@router.get("")
async def list_notification_events_deprecated():
    raise AppError(
        status_code=410,
        error_code="GONE",
        message="Use GET /notifications/catalog/events and /notifications/bindings",
    )
