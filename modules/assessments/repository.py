"""Assessments repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select, update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import (
    AssessmentCategoryProgress,
    AssessmentInstance,
    AssessmentPackage,
    AssessmentPackageCategory,
)
from modules.engagements.models import Engagement
from modules.questionnaire.models import QuestionnaireCategoryQuestion


class AssessmentsRepository:
    """Assessment database queries."""

    async def list_package_categories(self, db: AsyncSession, *, package_id: int) -> list[AssessmentPackageCategory]:
        result = await db.execute(
            select(AssessmentPackageCategory)
            .where(AssessmentPackageCategory.package_id == package_id)
            .order_by(AssessmentPackageCategory.display_order.asc().nulls_last(), AssessmentPackageCategory.id.asc())
        )
        return list(result.scalars().all())

    async def get_package_category_link(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        category_id: int,
    ) -> AssessmentPackageCategory | None:
        result = await db.execute(
            select(AssessmentPackageCategory)
            .where(AssessmentPackageCategory.package_id == package_id)
            .where(AssessmentPackageCategory.category_id == category_id)
        )
        return result.scalar_one_or_none()

    async def create_package_category_link(
        self,
        db: AsyncSession,
        link: AssessmentPackageCategory,
    ) -> AssessmentPackageCategory:
        if link.display_order is None:
            max_order_result = await db.execute(
                select(func.max(AssessmentPackageCategory.display_order)).where(
                    AssessmentPackageCategory.package_id == link.package_id
                )
            )
            max_order = max_order_result.scalar_one_or_none()
            link.display_order = (int(max_order) if max_order is not None else 0) + 1
        db.add(link)
        await db.flush()
        return link

    async def delete_package_category_link(self, db: AsyncSession, *, package_id: int, category_id: int) -> int:
        result = await db.execute(
            delete(AssessmentPackageCategory)
            .where(AssessmentPackageCategory.package_id == package_id)
            .where(AssessmentPackageCategory.category_id == category_id)
        )
        return int(result.rowcount or 0)

    async def get_assigned_category_ids_for_package_ordered(
        self,
        db: AsyncSession,
        *,
        package_id: int,
    ) -> list[int]:
        result = await db.execute(
            select(AssessmentPackageCategory.category_id)
            .where(AssessmentPackageCategory.package_id == package_id)
            .order_by(AssessmentPackageCategory.display_order.asc().nulls_last(), AssessmentPackageCategory.id.asc())
        )
        return [int(v) for v in result.scalars().all()]

    async def reorder_package_categories(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        category_ids: list[int],
    ) -> None:
        for index, category_id in enumerate(category_ids, start=1):
            await db.execute(
                sql_update(AssessmentPackageCategory)
                .where(
                    AssessmentPackageCategory.package_id == package_id,
                    AssessmentPackageCategory.category_id == category_id,
                )
                .values(display_order=index)
            )
        await db.flush()

    async def list_question_ids_for_package(self, db: AsyncSession, *, package_id: int) -> list[int]:
        result = await db.execute(
            select(QuestionnaireCategoryQuestion.question_id).distinct()
            .join(
                AssessmentPackageCategory,
                AssessmentPackageCategory.category_id == QuestionnaireCategoryQuestion.category_id,
            )
            .where(AssessmentPackageCategory.package_id == package_id)
            .order_by(QuestionnaireCategoryQuestion.question_id.asc())
        )
        return [int(v) for v in result.scalars().all()]

    async def get_instance_by_user_engagement_package(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        package_id: int,
    ) -> AssessmentInstance | None:
        query = (
            select(AssessmentInstance)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.engagement_id == engagement_id)
            .where(AssessmentInstance.package_id == package_id)
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_latest_instance_for_user_package(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        package_id: int,
    ) -> AssessmentInstance | None:
        result = await db.execute(
            select(AssessmentInstance)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.package_id == package_id)
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create_instance(self, db: AsyncSession, instance: AssessmentInstance) -> AssessmentInstance:
        db.add(instance)
        await db.flush()
        return instance


    async def get_package_by_id(self, db: AsyncSession, *, package_id: int) -> AssessmentPackage | None:
        result = await db.execute(select(AssessmentPackage).where(AssessmentPackage.package_id == package_id))
        return result.scalar_one_or_none()

    async def get_package_by_code(self, db: AsyncSession, *, package_code: str) -> AssessmentPackage | None:
        result = await db.execute(select(AssessmentPackage).where(AssessmentPackage.package_code == package_code))
        return result.scalar_one_or_none()

    async def create_package(self, db: AsyncSession, package: AssessmentPackage) -> AssessmentPackage:
        db.add(package)
        await db.flush()
        return package

    async def update_package(self, db: AsyncSession, package: AssessmentPackage) -> AssessmentPackage:
        db.add(package)
        await db.flush()
        return package

    async def list_packages(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None,
    ) -> list[AssessmentPackage]:
        offset = (page - 1) * limit

        query = select(AssessmentPackage).order_by(AssessmentPackage.package_id.desc()).offset(offset).limit(limit)
        if status is not None:
            query = query.where(AssessmentPackage.status == status)

        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_packages(self, db: AsyncSession, *, status: str | None) -> int:
        query = select(func.count()).select_from(AssessmentPackage)
        if status is not None:
            query = query.where(AssessmentPackage.status == status)
        result = await db.execute(query)
        return int(result.scalar_one())

    async def count_instances_for_user(self, db: AsyncSession, *, user_id: int) -> int:
        result = await db.execute(
            select(func.count()).select_from(AssessmentInstance).where(AssessmentInstance.user_id == user_id)
        )
        return int(result.scalar_one())

    async def list_instances_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        page: int,
        limit: int,
    ) -> list[tuple[AssessmentInstance, AssessmentPackage | None]]:
        offset = (page - 1) * limit

        query = (
            select(AssessmentInstance, AssessmentPackage)
            .outerjoin(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.user_id == user_id)
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await db.execute(query)
        return list(result.all())

    async def list_instances_for_user_with_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        page: int,
        limit: int,
    ) -> list[tuple[AssessmentInstance, AssessmentPackage | None, Engagement | None]]:
        offset = (page - 1) * limit

        query = (
            select(AssessmentInstance, AssessmentPackage, Engagement)
            .outerjoin(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .outerjoin(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .where(AssessmentInstance.user_id == user_id)
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await db.execute(query)
        return list(result.all())

    async def get_instance_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
    ) -> tuple[AssessmentInstance, AssessmentPackage | None] | None:
        query = (
            select(AssessmentInstance, AssessmentPackage)
            .outerjoin(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.assessment_instance_id == assessment_instance_id)
            .where(AssessmentInstance.user_id == user_id)
        )
        result = await db.execute(query)
        row = result.first()
        if row is None:
            return None
        return row

    async def get_instance_for_user_with_engagement(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
    ) -> tuple[AssessmentInstance, AssessmentPackage | None, Engagement | None] | None:
        query = (
            select(AssessmentInstance, AssessmentPackage, Engagement)
            .outerjoin(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .outerjoin(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .where(AssessmentInstance.assessment_instance_id == assessment_instance_id)
            .where(AssessmentInstance.user_id == user_id)
        )
        result = await db.execute(query)
        row = result.first()
        if row is None:
            return None
        return row

    async def get_instance_by_id(self, db: AsyncSession, assessment_instance_id: int) -> AssessmentInstance | None:
        result = await db.execute(
            select(AssessmentInstance).where(AssessmentInstance.assessment_instance_id == assessment_instance_id)
        )
        return result.scalar_one_or_none()

    async def list_instances_for_user_category(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        category_id: int,
    ) -> list[AssessmentInstance]:
        result = await db.execute(
            select(AssessmentInstance)
            .join(
                AssessmentPackageCategory,
                AssessmentPackageCategory.package_id == AssessmentInstance.package_id,
            )
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentPackageCategory.category_id == category_id)
            .order_by(AssessmentInstance.assessment_instance_id.desc())
        )
        return list(result.scalars().all())

    async def update_instance(self, db: AsyncSession, instance: AssessmentInstance) -> AssessmentInstance:
        db.add(instance)
        await db.flush()
        return instance

    async def list_category_progress_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> list[AssessmentCategoryProgress]:
        result = await db.execute(
            select(AssessmentCategoryProgress)
            .where(AssessmentCategoryProgress.assessment_instance_id == assessment_instance_id)
            .order_by(AssessmentCategoryProgress.category_id.asc())
        )
        return list(result.scalars().all())

    async def get_category_progress(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        category_id: int,
    ) -> AssessmentCategoryProgress | None:
        result = await db.execute(
            select(AssessmentCategoryProgress)
            .where(AssessmentCategoryProgress.assessment_instance_id == assessment_instance_id)
            .where(AssessmentCategoryProgress.category_id == category_id)
        )
        return result.scalar_one_or_none()

    async def create_category_progress(
        self,
        db: AsyncSession,
        row: AssessmentCategoryProgress,
    ) -> AssessmentCategoryProgress:
        db.add(row)
        await db.flush()
        return row

    async def update_category_progress(
        self,
        db: AsyncSession,
        row: AssessmentCategoryProgress,
    ) -> AssessmentCategoryProgress:
        db.add(row)
        await db.flush()
        return row
