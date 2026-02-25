"""Assessments repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage, AssessmentPackageQuestion


class AssessmentsRepository:
    """Assessment database queries."""

    async def list_package_questions(self, db: AsyncSession, *, package_id: int) -> list[AssessmentPackageQuestion]:
        result = await db.execute(
            select(AssessmentPackageQuestion)
            .where(AssessmentPackageQuestion.package_id == package_id)
            .order_by(AssessmentPackageQuestion.id.asc())
        )
        return list(result.scalars().all())

    async def get_package_question_link(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        question_id: int,
    ) -> AssessmentPackageQuestion | None:
        result = await db.execute(
            select(AssessmentPackageQuestion)
            .where(AssessmentPackageQuestion.package_id == package_id)
            .where(AssessmentPackageQuestion.question_id == question_id)
        )
        return result.scalar_one_or_none()

    async def create_package_question_link(
        self,
        db: AsyncSession,
        link: AssessmentPackageQuestion,
    ) -> AssessmentPackageQuestion:
        db.add(link)
        await db.flush()
        return link

    async def delete_package_question_link(self, db: AsyncSession, *, package_id: int, question_id: int) -> int:
        result = await db.execute(
            delete(AssessmentPackageQuestion)
            .where(AssessmentPackageQuestion.package_id == package_id)
            .where(AssessmentPackageQuestion.question_id == question_id)
        )
        return int(result.rowcount or 0)

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

    async def get_instance_by_id(self, db: AsyncSession, assessment_instance_id: int) -> AssessmentInstance | None:
        result = await db.execute(
            select(AssessmentInstance).where(AssessmentInstance.assessment_instance_id == assessment_instance_id)
        )
        return result.scalar_one_or_none()

    async def update_instance(self, db: AsyncSession, instance: AssessmentInstance) -> AssessmentInstance:
        db.add(instance)
        await db.flush()
        return instance
