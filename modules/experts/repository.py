"""Experts repository."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.experts.models import Expert, ExpertExpertiseTag, ExpertReview


class ExpertsRepository:
    async def get_by_id(self, db: AsyncSession, expert_id: int) -> Expert | None:
        result = await db.execute(select(Expert).where(Expert.expert_id == expert_id))
        return result.scalar_one_or_none()

    async def count_experts(
        self,
        db: AsyncSession,
        *,
        expert_type: str | None,
        status: str | None,
    ) -> int:
        query = select(func.count()).select_from(Expert)
        if expert_type is not None:
            query = query.where(Expert.expert_type == expert_type)
        if status is not None:
            query = query.where(Expert.status == status)
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_experts(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        expert_type: str | None,
        status: str | None,
    ) -> list[Expert]:
        offset = (page - 1) * limit
        query = select(Expert)
        if expert_type is not None:
            query = query.where(Expert.expert_type == expert_type)
        if status is not None:
            query = query.where(Expert.status == status)
        query = query.order_by(Expert.expert_id.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def create(self, db: AsyncSession, expert: Expert) -> Expert:
        db.add(expert)
        await db.flush()
        return expert

    async def update(self, db: AsyncSession, expert: Expert) -> Expert:
        expert.updated_at = datetime.now(timezone.utc)
        db.add(expert)
        await db.flush()
        return expert

    async def list_tags(self, db: AsyncSession, expert_id: int) -> list[ExpertExpertiseTag]:
        result = await db.execute(
            select(ExpertExpertiseTag)
            .where(ExpertExpertiseTag.expert_id == expert_id)
            .order_by(ExpertExpertiseTag.display_order.asc().nulls_last(), ExpertExpertiseTag.tag_id.asc())
        )
        return list(result.scalars().all())

    async def get_tag(self, db: AsyncSession, tag_id: int, expert_id: int) -> ExpertExpertiseTag | None:
        result = await db.execute(
            select(ExpertExpertiseTag).where(
                ExpertExpertiseTag.tag_id == tag_id,
                ExpertExpertiseTag.expert_id == expert_id,
            )
        )
        return result.scalar_one_or_none()

    async def add_tag(self, db: AsyncSession, tag: ExpertExpertiseTag) -> ExpertExpertiseTag:
        db.add(tag)
        await db.flush()
        return tag

    async def delete_tag(self, db: AsyncSession, tag: ExpertExpertiseTag) -> None:
        await db.delete(tag)
        await db.flush()

    async def count_reviews(self, db: AsyncSession, expert_id: int) -> int:
        q = select(func.count()).select_from(ExpertReview).where(ExpertReview.expert_id == expert_id)
        result = await db.execute(q)
        return int(result.scalar_one())

    async def list_reviews(
        self,
        db: AsyncSession,
        *,
        expert_id: int,
        page: int,
        limit: int,
    ) -> list[ExpertReview]:
        offset = (page - 1) * limit
        result = await db.execute(
            select(ExpertReview)
            .where(ExpertReview.expert_id == expert_id)
            .order_by(ExpertReview.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_review_by_expert_and_user(
        self,
        db: AsyncSession,
        *,
        expert_id: int,
        user_id: int,
    ) -> ExpertReview | None:
        result = await db.execute(
            select(ExpertReview).where(
                ExpertReview.expert_id == expert_id,
                ExpertReview.user_id == user_id,
            )
        )
        return result.scalar_one_or_none()

    async def create_review(self, db: AsyncSession, review: ExpertReview) -> ExpertReview:
        db.add(review)
        await db.flush()
        return review

    async def refresh_expert_rating_from_reviews(self, db: AsyncSession, expert_id: int) -> None:
        row = await db.execute(
            select(func.coalesce(func.avg(ExpertReview.rating), 0), func.count(ExpertReview.review_id)).where(
                ExpertReview.expert_id == expert_id
            )
        )
        avg_rating, cnt = row.one()
        expert = await self.get_by_id(db, expert_id)
        if expert is None:
            return
        count = int(cnt or 0)
        expert.review_count = count
        if count == 0:
            expert.rating = Decimal("0")
        else:
            expert.rating = Decimal(str(round(float(avg_rating), 2))).quantize(Decimal("0.01"))
        await self.update(db, expert)
