"""Repository for shared engagement slot_detail records."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import Engagement, EngagementSlotInfo


class EngagementSlotInfoRepository:
    """Database access for `engagement_slot_info`."""

    async def create(self, db: AsyncSession, slot_detail: dict[str, Any]) -> int:
        row = EngagementSlotInfo(slot_detail=slot_detail)
        db.add(row)
        await db.flush()
        return int(row.slot_detail_id)

    async def update(self, db: AsyncSession, slot_detail_id: int, slot_detail: dict[str, Any]) -> None:
        row = await db.get(EngagementSlotInfo, slot_detail_id)
        if row is None:
            raise ValueError(f"engagement_slot_info {slot_detail_id} not found")
        row.slot_detail = slot_detail
        await db.flush()

    async def get_by_id(self, db: AsyncSession, slot_detail_id: int) -> dict[str, Any] | None:
        row = await db.get(EngagementSlotInfo, slot_detail_id)
        if row is None:
            return None
        value = row.slot_detail
        return value if isinstance(value, dict) else None

    async def get_by_ids(self, db: AsyncSession, slot_detail_ids: list[int]) -> dict[int, dict[str, Any]]:
        if not slot_detail_ids:
            return {}
        result = await db.execute(
            select(EngagementSlotInfo).where(EngagementSlotInfo.slot_detail_id.in_(slot_detail_ids))
        )
        rows = result.scalars().all()
        out: dict[int, dict[str, Any]] = {}
        for row in rows:
            if isinstance(row.slot_detail, dict):
                out[int(row.slot_detail_id)] = row.slot_detail
        return out

    async def find_shared_slot_detail_id(
        self,
        db: AsyncSession,
        *,
        organization_id: int | None,
        city: str | None,
        start_date: date,
        end_date: date,
        exclude_engagement_id: int | None = None,
    ) -> int | None:
        query = (
            select(Engagement.slot_detail_id)
            .where(Engagement.slot_detail_id.isnot(None))
            .where(Engagement.start_date == start_date)
            .where(Engagement.end_date == end_date)
        )
        if organization_id is None:
            query = query.where(Engagement.organization_id.is_(None))
        else:
            query = query.where(Engagement.organization_id == organization_id)

        normalized_city = (city or "").strip().lower()
        if normalized_city:
            query = query.where(func.lower(func.trim(Engagement.city)) == normalized_city)
        else:
            query = query.where(func.coalesce(func.trim(Engagement.city), "") == "")

        if exclude_engagement_id is not None:
            query = query.where(Engagement.engagement_id != exclude_engagement_id)

        row = (await db.execute(query.limit(1))).scalar_one_or_none()
        return int(row) if row is not None else None
