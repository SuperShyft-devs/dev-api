"""Engagement types repository."""

from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import EngagementType


class EngagementTypesRepository:
    """Database queries for engagement_types."""

    async def list_all(
        self,
        db: AsyncSession,
        *,
        is_active: bool | None = None,
    ) -> list[EngagementType]:
        query = select(EngagementType).order_by(EngagementType.id.asc())
        if is_active is not None:
            query = query.where(EngagementType.is_active == is_active)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_by_id(self, db: AsyncSession, type_id: int) -> EngagementType | None:
        query = select(EngagementType).where(EngagementType.id == type_id)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_by_code(self, db: AsyncSession, code: str) -> EngagementType | None:
        query = select(EngagementType).where(EngagementType.code == code)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def create(self, db: AsyncSession, *, code: str, display_name: str, is_active: bool = True) -> EngagementType:
        obj = EngagementType(code=code, display_name=display_name, is_active=is_active)
        db.add(obj)
        await db.flush()
        return obj

    async def update(
        self,
        db: AsyncSession,
        type_id: int,
        *,
        display_name: str | None = None,
        is_active: bool | None = None,
    ) -> EngagementType | None:
        obj = await self.get_by_id(db, type_id)
        if obj is None:
            return None
        if display_name is not None:
            obj.display_name = display_name
        if is_active is not None:
            obj.is_active = is_active
        await db.flush()
        return obj

    async def soft_delete(self, db: AsyncSession, type_id: int) -> bool:
        stmt = (
            update(EngagementType)
            .where(EngagementType.id == type_id)
            .values(is_active=False)
        )
        result = await db.execute(stmt)
        return result.rowcount > 0
