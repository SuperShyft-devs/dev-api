"""Experts repository."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.experts.models import Expert, ExpertExpertiseTag, ExpertReview, ExpertTypeModel


class ExpertTypesRepository:

    async def list_all(self, db: AsyncSession) -> list[ExpertTypeModel]:
        result = await db.execute(select(ExpertTypeModel).order_by(ExpertTypeModel.id.asc()))
        return list(result.scalars().all())

    async def get_by_id(self, db: AsyncSession, expert_type_id: int) -> ExpertTypeModel | None:
        result = await db.execute(select(ExpertTypeModel).where(ExpertTypeModel.id == expert_type_id))
        return result.scalar_one_or_none()

    async def get_by_key(self, db: AsyncSession, type_key: str) -> ExpertTypeModel | None:
        result = await db.execute(select(ExpertTypeModel).where(ExpertTypeModel.type_key == type_key))
        return result.scalar_one_or_none()

    async def create(self, db: AsyncSession, expert_type: ExpertTypeModel) -> ExpertTypeModel:
        db.add(expert_type)
        await db.flush()
        return expert_type

    async def update(self, db: AsyncSession, expert_type: ExpertTypeModel) -> ExpertTypeModel:
        db.add(expert_type)
        await db.flush()
        return expert_type

    async def delete(self, db: AsyncSession, expert_type: ExpertTypeModel) -> None:
        await db.delete(expert_type)
        await db.flush()


class ExpertsRepository:
    _EXPERT_SORT_COLUMNS = {
        "expert_id": Expert.expert_id,
        "expert_type": Expert.expert_type,
        "specialization": Expert.specialization,
        "status": Expert.status,
        "experience_years": Expert.experience_years,
    }

    def _apply_expert_list_filters(
        self,
        query,
        *,
        expert_type: str | None,
        status: str | None,
        search: str | None = None,
    ):
        if expert_type is not None:
            query = query.where(Expert.expert_type == expert_type)
        if status is not None:
            query = query.where(Expert.status == status)
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    Expert.specialization.ilike(pattern),
                    Expert.qualifications.ilike(pattern),
                    Expert.about_text.ilike(pattern),
                )
            )
        return query

    async def get_by_id(self, db: AsyncSession, expert_id: int) -> Expert | None:
        result = await db.execute(select(Expert).where(Expert.expert_id == expert_id))
        return result.scalar_one_or_none()

    async def get_by_user_id(self, db: AsyncSession, user_id: int) -> Expert | None:
        result = await db.execute(
            select(Expert).where(Expert.user_id == user_id).order_by(Expert.expert_id.asc()).limit(1)
        )
        return result.scalar_one_or_none()

    async def count_experts(
        self,
        db: AsyncSession,
        *,
        expert_type: str | None,
        status: str | None,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Expert)
        query = self._apply_expert_list_filters(query, expert_type=expert_type, status=status, search=search)
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
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[Expert]:
        offset = (page - 1) * limit
        query = select(Expert)
        query = self._apply_expert_list_filters(query, expert_type=expert_type, status=status, search=search)
        query = apply_sort(
            query,
            sort_by=sort_by,
            sort_dir=sort_dir,
            columns=self._EXPERT_SORT_COLUMNS,
            default_column=Expert.expert_id,
        )
        query = query.offset(offset).limit(limit)
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
