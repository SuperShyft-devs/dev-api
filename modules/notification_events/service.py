"""Auto notification events service."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.engagements.models import AutoNotificationEvent, EngagementType
from modules.notification_events.repository import NotificationEventsRepository
from modules.notification_events.schemas import (
    EngagementTypeRef,
    NotificationEventCreateRequest,
    NotificationEventResponse,
    NotificationEventUpdateRequest,
)


class NotificationEventsService:
    def __init__(self, repository: NotificationEventsRepository | None = None):
        self._repository = repository or NotificationEventsRepository()

    async def _build_type_map(self, db: AsyncSession) -> dict[int, str]:
        result = await db.execute(select(EngagementType.id, EngagementType.display_name))
        return {row.id: row.display_name for row in result.all()}

    def _to_response(
        self, obj: AutoNotificationEvent, type_map: dict[int, str]
    ) -> NotificationEventResponse:
        refs = [
            EngagementTypeRef(engagement_type_id=tid, display_name=type_map.get(tid, f"Type #{tid}"))
            for tid in (obj.engagement_type_ids or [])
        ]
        return NotificationEventResponse(
            notification_event_id=obj.id,
            engagement_type_ids=obj.engagement_type_ids or [],
            engagement_types=refs,
            event_code=obj.event_code,
            display_name=obj.display_name,
            description=obj.description,
            created_at=getattr(obj, "created_at", None),
        )

    async def list_all(
        self,
        db: AsyncSession,
        *,
        engagement_type_id: int | None = None,
    ) -> list[NotificationEventResponse]:
        rows = await self._repository.list_all(db, engagement_type_id=engagement_type_id)
        type_map = await self._build_type_map(db)
        return [self._to_response(row, type_map) for row in rows]

    async def get_by_id(self, db: AsyncSession, event_id: int) -> NotificationEventResponse:
        obj = await self._repository.get_by_id(db, event_id)
        if obj is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Notification event not found")
        type_map = await self._build_type_map(db)
        return self._to_response(obj, type_map)

    async def create(
        self,
        db: AsyncSession,
        payload: NotificationEventCreateRequest,
    ) -> NotificationEventResponse:
        existing = await self._repository.get_by_event_code(db, payload.event_code)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="DUPLICATE_EVENT_CODE",
                message=f"Notification event with code '{payload.event_code}' already exists",
            )
        obj = await self._repository.create(
            db,
            engagement_type_ids=payload.engagement_type_ids,
            event_code=payload.event_code,
            display_name=payload.display_name,
            description=payload.description,
        )
        type_map = await self._build_type_map(db)
        return self._to_response(obj, type_map)

    async def update(
        self,
        db: AsyncSession,
        event_id: int,
        payload: NotificationEventUpdateRequest,
    ) -> NotificationEventResponse:
        obj = await self._repository.update(
            db,
            event_id,
            engagement_type_ids=payload.engagement_type_ids,
            display_name=payload.display_name,
            description=payload.description if payload.description is not None else ...,
        )
        if obj is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Notification event not found")
        type_map = await self._build_type_map(db)
        return self._to_response(obj, type_map)

    async def delete(self, db: AsyncSession, event_id: int) -> None:
        success = await self._repository.delete(db, event_id)
        if not success:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Notification event not found")
