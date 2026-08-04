"""Engagement types service."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.engagement_types.repository import EngagementTypesRepository
from modules.engagement_types.schemas import (
    EngagementTypeCreateRequest,
    EngagementTypeResponse,
    EngagementTypeUpdateRequest,
)


class EngagementTypesService:
    def __init__(self, repository: EngagementTypesRepository | None = None):
        self._repository = repository or EngagementTypesRepository()

    async def list_all(
        self,
        db: AsyncSession,
        *,
        is_active: bool | None = None,
    ) -> list[EngagementTypeResponse]:
        rows = await self._repository.list_all(db, is_active=is_active)
        return [
            EngagementTypeResponse(
                id=row.id,
                code=row.code,
                display_name=row.display_name,
                is_active=row.is_active,
            )
            for row in rows
        ]

    async def create(
        self,
        db: AsyncSession,
        payload: EngagementTypeCreateRequest,
    ) -> EngagementTypeResponse:
        existing = await self._repository.get_by_code(db, payload.code)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="DUPLICATE_CODE",
                message=f"Engagement type with code '{payload.code}' already exists",
            )
        obj = await self._repository.create(
            db,
            code=payload.code,
            display_name=payload.display_name,
            is_active=payload.is_active,
        )
        return EngagementTypeResponse(
            id=obj.id,
            code=obj.code,
            display_name=obj.display_name,
            is_active=obj.is_active,
        )

    async def update(
        self,
        db: AsyncSession,
        type_id: int,
        payload: EngagementTypeUpdateRequest,
    ) -> EngagementTypeResponse:
        obj = await self._repository.update(
            db,
            type_id,
            display_name=payload.display_name,
            is_active=payload.is_active,
        )
        if obj is None:
            raise AppError(
                status_code=404,
                error_code="NOT_FOUND",
                message="Engagement type not found",
            )
        return EngagementTypeResponse(
            id=obj.id,
            code=obj.code,
            display_name=obj.display_name,
            is_active=obj.is_active,
        )

    async def delete(self, db: AsyncSession, type_id: int) -> None:
        success = await self._repository.soft_delete(db, type_id)
        if not success:
            raise AppError(
                status_code=404,
                error_code="NOT_FOUND",
                message="Engagement type not found",
            )
