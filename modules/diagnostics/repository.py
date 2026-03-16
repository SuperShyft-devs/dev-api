"""Diagnostics repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, distinct, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageFilter,
    DiagnosticPackagePreparation,
    DiagnosticPackageReason,
    DiagnosticPackageSample,
    DiagnosticPackageTag,
    DiagnosticTest,
    DiagnosticTestGroup,
)


class DiagnosticsRepository:
    """Diagnostics database queries."""

    async def get_all_packages(
        self,
        db: AsyncSession,
        *,
        gender: str | None = None,
        tag: str | None = None,
    ) -> list[DiagnosticPackage]:
        query = (
            select(DiagnosticPackage)
            .options(selectinload(DiagnosticPackage.tags))
            .where(DiagnosticPackage.status == "active")
        )

        if gender is not None:
            query = query.where(
                or_(DiagnosticPackage.gender_suitability == gender, DiagnosticPackage.gender_suitability == "both")
            )

        if tag is not None:
            tag_package_subquery = select(DiagnosticPackageTag.diagnostic_package_id).where(
                DiagnosticPackageTag.tag_name == tag
            )
            query = query.where(DiagnosticPackage.diagnostic_package_id.in_(tag_package_subquery))

        query = query.order_by(DiagnosticPackage.diagnostic_package_id.desc())
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_package_by_id(self, db: AsyncSession, *, package_id: int) -> DiagnosticPackage | None:
        result = await db.execute(
            select(DiagnosticPackage)
            .where(DiagnosticPackage.diagnostic_package_id == package_id)
            .options(
                joinedload(DiagnosticPackage.reasons),
                joinedload(DiagnosticPackage.tags),
                joinedload(DiagnosticPackage.samples),
                joinedload(DiagnosticPackage.preparations),
            )
        )
        return result.unique().scalar_one_or_none()

    async def get_package_by_id_basic(self, db: AsyncSession, *, package_id: int) -> DiagnosticPackage | None:
        result = await db.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == package_id)
        )
        return result.scalar_one_or_none()

    async def get_package_tests(self, db: AsyncSession, *, package_id: int) -> list[DiagnosticTestGroup]:
        result = await db.execute(
            select(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.diagnostic_package_id == package_id)
            .options(selectinload(DiagnosticTestGroup.tests))
            .order_by(DiagnosticTestGroup.display_order.asc().nulls_last(), DiagnosticTestGroup.group_id.asc())
        )
        return list(result.scalars().all())

    async def create_package(self, db: AsyncSession, package: DiagnosticPackage) -> DiagnosticPackage:
        db.add(package)
        await db.flush()
        return package

    async def update_package(self, db: AsyncSession, *, package_id: int, data: dict) -> DiagnosticPackage | None:
        package = await self.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            return None

        for key, value in data.items():
            if value is not None:
                setattr(package, key, value)

        db.add(package)
        await db.flush()
        return package

    async def update_package_status(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        status: str,
    ) -> DiagnosticPackage | None:
        package = await self.get_package_by_id_basic(db, package_id=package_id)
        if package is None:
            return None

        package.status = status
        db.add(package)
        await db.flush()
        return package

    async def get_all_filters(self, db: AsyncSession) -> list[DiagnosticPackageFilter]:
        result = await db.execute(
            select(DiagnosticPackageFilter)
            .where(DiagnosticPackageFilter.status == "active")
            .order_by(DiagnosticPackageFilter.display_order.asc().nulls_last(), DiagnosticPackageFilter.filter_id.asc())
        )
        return list(result.scalars().all())

    async def get_filter_by_id(self, db: AsyncSession, *, filter_id: int) -> DiagnosticPackageFilter | None:
        result = await db.execute(
            select(DiagnosticPackageFilter).where(DiagnosticPackageFilter.filter_id == filter_id)
        )
        return result.scalar_one_or_none()

    async def create_filter(self, db: AsyncSession, data: DiagnosticPackageFilter) -> DiagnosticPackageFilter:
        db.add(data)
        await db.flush()
        return data

    async def update_filter(self, db: AsyncSession, *, filter_id: int, data: dict) -> DiagnosticPackageFilter | None:
        row = await self.get_filter_by_id(db, filter_id=filter_id)
        if row is None:
            return None

        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)

        db.add(row)
        await db.flush()
        return row

    async def delete_filter(self, db: AsyncSession, *, filter_id: int) -> int:
        result = await db.execute(
            delete(DiagnosticPackageFilter).where(DiagnosticPackageFilter.filter_id == filter_id)
        )
        return int(result.rowcount or 0)

    async def get_reasons(self, db: AsyncSession, *, package_id: int) -> list[DiagnosticPackageReason]:
        result = await db.execute(
            select(DiagnosticPackageReason)
            .where(DiagnosticPackageReason.diagnostic_package_id == package_id)
            .order_by(DiagnosticPackageReason.display_order.asc().nulls_last(), DiagnosticPackageReason.reason_id.asc())
        )
        return list(result.scalars().all())

    async def get_reason_by_id(self, db: AsyncSession, *, reason_id: int) -> DiagnosticPackageReason | None:
        result = await db.execute(select(DiagnosticPackageReason).where(DiagnosticPackageReason.reason_id == reason_id))
        return result.scalar_one_or_none()

    async def create_reason(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        data: DiagnosticPackageReason,
    ) -> DiagnosticPackageReason:
        data.diagnostic_package_id = package_id
        db.add(data)
        await db.flush()
        return data

    async def update_reason(self, db: AsyncSession, *, reason_id: int, data: dict) -> DiagnosticPackageReason | None:
        row = await self.get_reason_by_id(db, reason_id=reason_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_reason(self, db: AsyncSession, *, reason_id: int) -> int:
        result = await db.execute(delete(DiagnosticPackageReason).where(DiagnosticPackageReason.reason_id == reason_id))
        return int(result.rowcount or 0)

    async def get_tags(self, db: AsyncSession, *, package_id: int) -> list[DiagnosticPackageTag]:
        result = await db.execute(
            select(DiagnosticPackageTag)
            .where(DiagnosticPackageTag.diagnostic_package_id == package_id)
            .order_by(DiagnosticPackageTag.display_order.asc().nulls_last(), DiagnosticPackageTag.tag_id.asc())
        )
        return list(result.scalars().all())

    async def get_tag_by_id(self, db: AsyncSession, *, tag_id: int) -> DiagnosticPackageTag | None:
        result = await db.execute(select(DiagnosticPackageTag).where(DiagnosticPackageTag.tag_id == tag_id))
        return result.scalar_one_or_none()

    async def get_distinct_tag_names(self, db: AsyncSession) -> list[str]:
        result = await db.execute(
            select(distinct(DiagnosticPackageTag.tag_name))
            .where(DiagnosticPackageTag.tag_name.is_not(None))
            .order_by(DiagnosticPackageTag.tag_name.asc())
        )
        return [tag_name for tag_name in result.scalars().all() if isinstance(tag_name, str) and tag_name.strip()]

    async def create_tag(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        data: DiagnosticPackageTag,
    ) -> DiagnosticPackageTag:
        data.diagnostic_package_id = package_id
        db.add(data)
        await db.flush()
        return data

    async def delete_tag(self, db: AsyncSession, *, tag_id: int) -> int:
        result = await db.execute(delete(DiagnosticPackageTag).where(DiagnosticPackageTag.tag_id == tag_id))
        return int(result.rowcount or 0)

    async def get_test_groups(self, db: AsyncSession, *, package_id: int) -> list[DiagnosticTestGroup]:
        result = await db.execute(
            select(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.diagnostic_package_id == package_id)
            .order_by(DiagnosticTestGroup.display_order.asc().nulls_last(), DiagnosticTestGroup.group_id.asc())
        )
        return list(result.scalars().all())

    async def get_test_group_by_id(self, db: AsyncSession, *, group_id: int) -> DiagnosticTestGroup | None:
        result = await db.execute(select(DiagnosticTestGroup).where(DiagnosticTestGroup.group_id == group_id))
        return result.scalar_one_or_none()

    async def create_test_group(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        data: DiagnosticTestGroup,
    ) -> DiagnosticTestGroup:
        data.diagnostic_package_id = package_id
        db.add(data)
        await db.flush()
        return data

    async def update_test_group(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        data: dict,
    ) -> DiagnosticTestGroup | None:
        row = await self.get_test_group_by_id(db, group_id=group_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_test_group(self, db: AsyncSession, *, group_id: int) -> int:
        result = await db.execute(delete(DiagnosticTestGroup).where(DiagnosticTestGroup.group_id == group_id))
        return int(result.rowcount or 0)

    async def get_test_by_id(self, db: AsyncSession, *, test_id: int) -> DiagnosticTest | None:
        result = await db.execute(select(DiagnosticTest).where(DiagnosticTest.test_id == test_id))
        return result.scalar_one_or_none()

    async def create_test(self, db: AsyncSession, *, group_id: int, data: DiagnosticTest) -> DiagnosticTest:
        data.group_id = group_id
        db.add(data)
        await db.flush()
        return data

    async def update_test(self, db: AsyncSession, *, test_id: int, data: dict) -> DiagnosticTest | None:
        row = await self.get_test_by_id(db, test_id=test_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_test(self, db: AsyncSession, *, test_id: int) -> int:
        result = await db.execute(delete(DiagnosticTest).where(DiagnosticTest.test_id == test_id))
        return int(result.rowcount or 0)

    async def get_samples(self, db: AsyncSession, *, package_id: int) -> list[DiagnosticPackageSample]:
        result = await db.execute(
            select(DiagnosticPackageSample)
            .where(DiagnosticPackageSample.diagnostic_package_id == package_id)
            .order_by(DiagnosticPackageSample.display_order.asc().nulls_last(), DiagnosticPackageSample.sample_id.asc())
        )
        return list(result.scalars().all())

    async def get_sample_by_id(self, db: AsyncSession, *, sample_id: int) -> DiagnosticPackageSample | None:
        result = await db.execute(select(DiagnosticPackageSample).where(DiagnosticPackageSample.sample_id == sample_id))
        return result.scalar_one_or_none()

    async def create_sample(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        data: DiagnosticPackageSample,
    ) -> DiagnosticPackageSample:
        data.diagnostic_package_id = package_id
        db.add(data)
        await db.flush()
        return data

    async def update_sample(
        self,
        db: AsyncSession,
        *,
        sample_id: int,
        data: dict,
    ) -> DiagnosticPackageSample | None:
        row = await self.get_sample_by_id(db, sample_id=sample_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_sample(self, db: AsyncSession, *, sample_id: int) -> int:
        result = await db.execute(delete(DiagnosticPackageSample).where(DiagnosticPackageSample.sample_id == sample_id))
        return int(result.rowcount or 0)

    async def get_preparations(self, db: AsyncSession, *, package_id: int) -> list[DiagnosticPackagePreparation]:
        result = await db.execute(
            select(DiagnosticPackagePreparation)
            .where(DiagnosticPackagePreparation.diagnostic_package_id == package_id)
            .order_by(
                DiagnosticPackagePreparation.display_order.asc().nulls_last(),
                DiagnosticPackagePreparation.preparation_id.asc(),
            )
        )
        return list(result.scalars().all())

    async def get_preparation_by_id(
        self,
        db: AsyncSession,
        *,
        preparation_id: int,
    ) -> DiagnosticPackagePreparation | None:
        result = await db.execute(
            select(DiagnosticPackagePreparation).where(
                DiagnosticPackagePreparation.preparation_id == preparation_id
            )
        )
        return result.scalar_one_or_none()

    async def create_preparation(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        data: DiagnosticPackagePreparation,
    ) -> DiagnosticPackagePreparation:
        data.diagnostic_package_id = package_id
        db.add(data)
        await db.flush()
        return data

    async def update_preparation(
        self,
        db: AsyncSession,
        *,
        preparation_id: int,
        data: dict,
    ) -> DiagnosticPackagePreparation | None:
        row = await self.get_preparation_by_id(db, preparation_id=preparation_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_preparation(self, db: AsyncSession, *, preparation_id: int) -> int:
        result = await db.execute(
            delete(DiagnosticPackagePreparation).where(
                DiagnosticPackagePreparation.preparation_id == preparation_id
            )
        )
        return int(result.rowcount or 0)
