"""Diagnostics repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import delete, distinct, func, or_, select, update as sql_update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageFilterChip,
    DiagnosticPackageFilterChipLink,
    DiagnosticPackagePreparation,
    DiagnosticPackageReason,
    DiagnosticPackageSample,
    DiagnosticPackageTag,
    DiagnosticPackageTestGroup,
    DiagnosticTestGroup,
    DiagnosticTestGroupTest,
    HealthParameter,
    ParameterType,
)


class DiagnosticsRepository:
    """Diagnostics database queries."""

    async def get_all_packages(
        self,
        db: AsyncSession,
        *,
        gender: str | None = None,
        tag: str | None = None,
        filter_chip: str | None = None,
        active_only: bool = True,
        public_only: bool = False,
        created_by_user_id: int | None = None,
    ) -> list[DiagnosticPackage]:
        query = (
            select(DiagnosticPackage)
            .options(
                selectinload(DiagnosticPackage.tags),
                selectinload(DiagnosticPackage.filter_chip_links).selectinload(
                    DiagnosticPackageFilterChipLink.filter_chip
                ),
            )
        )

        if active_only:
            query = query.where(DiagnosticPackage.status == "active")

        if public_only:
            query = query.where(DiagnosticPackage.created_by_user_id.is_(None))
        elif created_by_user_id is not None:
            query = query.where(DiagnosticPackage.created_by_user_id == created_by_user_id)

        if gender is not None:
            query = query.where(
                or_(DiagnosticPackage.gender_suitability == gender, DiagnosticPackage.gender_suitability == "both")
            )

        if tag is not None:
            tag_package_subquery = select(DiagnosticPackageTag.diagnostic_package_id).where(
                DiagnosticPackageTag.tag_name == tag
            )
            query = query.where(DiagnosticPackage.diagnostic_package_id.in_(tag_package_subquery))

        if filter_chip is not None:
            chip_package_subquery = (
                select(DiagnosticPackageFilterChipLink.diagnostic_package_id)
                .join(
                    DiagnosticPackageFilterChip,
                    DiagnosticPackageFilterChip.filter_chip_id == DiagnosticPackageFilterChipLink.filter_chip_id,
                )
                .where(
                    DiagnosticPackageFilterChip.chip_key == filter_chip,
                    DiagnosticPackageFilterChipLink.diagnostic_package_id.isnot(None),
                )
            )
            query = query.where(DiagnosticPackage.diagnostic_package_id.in_(chip_package_subquery))

        query = query.order_by(DiagnosticPackage.diagnostic_package_id.desc())
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_distinct_tests_for_packages(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> dict[int, int]:
        if not package_ids:
            return {}
        pkg_col = DiagnosticPackageTestGroup.diagnostic_package_id
        subq = (
            select(
                pkg_col.label("diagnostic_package_id"),
                func.count(distinct(DiagnosticTestGroupTest.test_id)).label("cnt"),
            )
            .select_from(DiagnosticPackageTestGroup)
            .join(
                DiagnosticTestGroupTest,
                DiagnosticTestGroupTest.group_id == DiagnosticPackageTestGroup.group_id,
            )
            .where(pkg_col.in_(package_ids))
            .group_by(pkg_col)
        )
        result = await db.execute(subq)
        rows = result.all()
        out = {int(pid): 0 for pid in package_ids}
        for row in rows:
            out[int(row.diagnostic_package_id)] = int(row.cnt or 0)
        return out

    async def get_package_by_id(self, db: AsyncSession, *, package_id: int) -> DiagnosticPackage | None:
        result = await db.execute(
            select(DiagnosticPackage)
            .where(DiagnosticPackage.diagnostic_package_id == package_id)
            .options(
                joinedload(DiagnosticPackage.reasons),
                joinedload(DiagnosticPackage.tags),
                joinedload(DiagnosticPackage.samples),
                joinedload(DiagnosticPackage.preparations),
                joinedload(DiagnosticPackage.filter_chip_links).joinedload(
                    DiagnosticPackageFilterChipLink.filter_chip
                ),
            )
        )
        return result.unique().scalar_one_or_none()

    async def get_package_by_id_basic(self, db: AsyncSession, *, package_id: int) -> DiagnosticPackage | None:
        result = await db.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == package_id)
        )
        return result.scalar_one_or_none()

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

    async def get_all_filter_chips(
        self,
        db: AsyncSession,
        *,
        chip_for: str | None = None,
    ) -> list[DiagnosticPackageFilterChip]:
        query = select(DiagnosticPackageFilterChip).where(DiagnosticPackageFilterChip.status == "active")
        if chip_for is not None:
            query = query.where(DiagnosticPackageFilterChip.chip_for == chip_for)
        query = query.order_by(
            DiagnosticPackageFilterChip.display_order.asc().nulls_last(),
            DiagnosticPackageFilterChip.filter_chip_id.asc(),
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_filter_chip_by_id(
        self, db: AsyncSession, *, filter_chip_id: int
    ) -> DiagnosticPackageFilterChip | None:
        result = await db.execute(
            select(DiagnosticPackageFilterChip).where(
                DiagnosticPackageFilterChip.filter_chip_id == filter_chip_id
            )
        )
        return result.scalar_one_or_none()

    async def get_filter_chip_by_chip_key(
        self, db: AsyncSession, *, chip_key: str
    ) -> DiagnosticPackageFilterChip | None:
        result = await db.execute(
            select(DiagnosticPackageFilterChip).where(DiagnosticPackageFilterChip.chip_key == chip_key)
        )
        return result.scalar_one_or_none()

    async def create_filter_chip(
        self, db: AsyncSession, data: DiagnosticPackageFilterChip
    ) -> DiagnosticPackageFilterChip:
        db.add(data)
        await db.flush()
        return data

    async def update_filter_chip(
        self, db: AsyncSession, *, filter_chip_id: int, data: dict
    ) -> DiagnosticPackageFilterChip | None:
        row = await self.get_filter_chip_by_id(db, filter_chip_id=filter_chip_id)
        if row is None:
            return None

        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)

        db.add(row)
        await db.flush()
        return row

    async def delete_filter_chip(self, db: AsyncSession, *, filter_chip_id: int) -> int:
        result = await db.execute(
            delete(DiagnosticPackageFilterChip).where(
                DiagnosticPackageFilterChip.filter_chip_id == filter_chip_id
            )
        )
        return int(result.rowcount or 0)

    async def get_filter_chip_link(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        filter_chip_id: int,
    ) -> DiagnosticPackageFilterChipLink | None:
        result = await db.execute(
            select(DiagnosticPackageFilterChipLink).where(
                DiagnosticPackageFilterChipLink.diagnostic_package_id == package_id,
                DiagnosticPackageFilterChipLink.filter_chip_id == filter_chip_id,
                DiagnosticPackageFilterChipLink.group_id.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def get_group_filter_chip_link(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        filter_chip_id: int,
    ) -> DiagnosticPackageFilterChipLink | None:
        result = await db.execute(
            select(DiagnosticPackageFilterChipLink).where(
                DiagnosticPackageFilterChipLink.group_id == group_id,
                DiagnosticPackageFilterChipLink.filter_chip_id == filter_chip_id,
                DiagnosticPackageFilterChipLink.diagnostic_package_id.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def add_filter_chip_link(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        filter_chip_id: int,
        display_order: int | None = None,
    ) -> DiagnosticPackageFilterChipLink:
        link = DiagnosticPackageFilterChipLink(
            diagnostic_package_id=package_id,
            filter_chip_id=filter_chip_id,
            display_order=display_order,
            group_id=None,
        )
        db.add(link)
        await db.flush()
        return link

    async def delete_filter_chip_link(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        filter_chip_id: int,
    ) -> int:
        result = await db.execute(
            delete(DiagnosticPackageFilterChipLink).where(
                DiagnosticPackageFilterChipLink.diagnostic_package_id == package_id,
                DiagnosticPackageFilterChipLink.filter_chip_id == filter_chip_id,
                DiagnosticPackageFilterChipLink.group_id.is_(None),
            )
        )
        return int(result.rowcount or 0)

    async def delete_group_filter_chip_link(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        filter_chip_id: int,
    ) -> int:
        result = await db.execute(
            delete(DiagnosticPackageFilterChipLink).where(
                DiagnosticPackageFilterChipLink.group_id == group_id,
                DiagnosticPackageFilterChipLink.filter_chip_id == filter_chip_id,
                DiagnosticPackageFilterChipLink.diagnostic_package_id.is_(None),
            )
        )
        return int(result.rowcount or 0)

    async def get_max_filter_chip_link_display_order(
        self, db: AsyncSession, *, package_id: int
    ) -> int | None:
        result = await db.execute(
            select(func.max(DiagnosticPackageFilterChipLink.display_order)).where(
                DiagnosticPackageFilterChipLink.diagnostic_package_id == package_id,
                DiagnosticPackageFilterChipLink.group_id.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def get_max_group_filter_chip_link_display_order(
        self, db: AsyncSession, *, group_id: int
    ) -> int | None:
        result = await db.execute(
            select(func.max(DiagnosticPackageFilterChipLink.display_order)).where(
                DiagnosticPackageFilterChipLink.group_id == group_id,
                DiagnosticPackageFilterChipLink.diagnostic_package_id.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def add_group_filter_chip_link(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        filter_chip_id: int,
        display_order: int | None = None,
    ) -> DiagnosticPackageFilterChipLink:
        link = DiagnosticPackageFilterChipLink(
            group_id=group_id,
            filter_chip_id=filter_chip_id,
            display_order=display_order,
            diagnostic_package_id=None,
        )
        db.add(link)
        await db.flush()
        return link

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

    async def get_all_parameters(
        self,
        db: AsyncSession,
        *,
        parameter_type: ParameterType | None = None,
    ) -> list[HealthParameter]:
        query = select(HealthParameter).order_by(
            HealthParameter.display_order.asc().nulls_last(),
            HealthParameter.test_id.asc(),
        )
        if parameter_type is not None:
            query = query.where(HealthParameter.parameter_type == parameter_type)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_parameter_by_id(self, db: AsyncSession, *, test_id: int) -> HealthParameter | None:
        result = await db.execute(select(HealthParameter).where(HealthParameter.test_id == test_id))
        return result.scalar_one_or_none()

    async def get_parameter_by_parameter_key(self, db: AsyncSession, *, parameter_key: str) -> HealthParameter | None:
        """Match case-insensitively. Prefer METRIC over TEST when both exist."""
        key_lower = parameter_key.strip().lower()
        result = await db.execute(
            select(HealthParameter).where(func.lower(HealthParameter.parameter_key) == key_lower)
        )
        rows = list(result.scalars().all())
        if not rows:
            return None
        rows.sort(key=lambda r: 0 if r.parameter_type == ParameterType.METRIC else 1)
        return rows[0]

    async def create_parameter(self, db: AsyncSession, data: HealthParameter) -> HealthParameter:
        db.add(data)
        await db.flush()
        return data

    async def update_parameter(self, db: AsyncSession, *, test_id: int, data: dict) -> HealthParameter | None:
        row = await self.get_parameter_by_id(db, test_id=test_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_parameter(self, db: AsyncSession, *, test_id: int) -> int:
        result = await db.execute(delete(HealthParameter).where(HealthParameter.test_id == test_id))
        return int(result.rowcount or 0)

    async def get_all_groups(
        self,
        db: AsyncSession,
        *,
        filter_chip_key: str | None = None,
    ) -> list[tuple[DiagnosticTestGroup, int]]:
        query = (
            select(DiagnosticTestGroup, func.count(DiagnosticTestGroupTest.id))
            .outerjoin(DiagnosticTestGroupTest, DiagnosticTestGroupTest.group_id == DiagnosticTestGroup.group_id)
        )
        if filter_chip_key is not None:
            chip_sub = (
                select(DiagnosticPackageFilterChipLink.group_id)
                .join(
                    DiagnosticPackageFilterChip,
                    DiagnosticPackageFilterChip.filter_chip_id == DiagnosticPackageFilterChipLink.filter_chip_id,
                )
                .where(
                    DiagnosticPackageFilterChip.chip_key == filter_chip_key,
                    DiagnosticPackageFilterChipLink.group_id.isnot(None),
                )
            )
            query = query.where(DiagnosticTestGroup.group_id.in_(chip_sub))
        query = query.group_by(DiagnosticTestGroup.group_id).order_by(
            DiagnosticTestGroup.display_order.asc().nulls_last(),
            DiagnosticTestGroup.group_id.asc(),
        )
        result = await db.execute(query)
        return [(row[0], int(row[1] or 0)) for row in result.all()]

    async def get_group_by_id(self, db: AsyncSession, *, group_id: int) -> DiagnosticTestGroup | None:
        result = await db.execute(select(DiagnosticTestGroup).where(DiagnosticTestGroup.group_id == group_id))
        return result.scalar_one_or_none()

    async def get_group_filter_chip_links(
        self,
        db: AsyncSession,
        *,
        group_id: int,
    ) -> list[DiagnosticPackageFilterChipLink]:
        result = await db.execute(
            select(DiagnosticPackageFilterChipLink)
            .where(
                DiagnosticPackageFilterChipLink.group_id == group_id,
                DiagnosticPackageFilterChipLink.diagnostic_package_id.is_(None),
            )
            .options(joinedload(DiagnosticPackageFilterChipLink.filter_chip))
            .order_by(
                DiagnosticPackageFilterChipLink.display_order.asc().nulls_last(),
                DiagnosticPackageFilterChipLink.link_id.asc(),
            )
        )
        return list(result.scalars().unique().all())

    async def get_parameters_for_group(self, db: AsyncSession, *, group_id: int) -> list[HealthParameter]:
        result = await db.execute(
            select(HealthParameter)
            .join(DiagnosticTestGroupTest, DiagnosticTestGroupTest.test_id == HealthParameter.test_id)
            .where(DiagnosticTestGroupTest.group_id == group_id)
            .order_by(DiagnosticTestGroupTest.display_order.asc().nulls_last(), HealthParameter.test_id.asc())
        )
        return list(result.scalars().all())

    async def create_group(self, db: AsyncSession, data: DiagnosticTestGroup) -> DiagnosticTestGroup:
        db.add(data)
        await db.flush()
        return data

    async def update_group(self, db: AsyncSession, *, group_id: int, data: dict) -> DiagnosticTestGroup | None:
        row = await self.get_group_by_id(db, group_id=group_id)
        if row is None:
            return None
        for key, value in data.items():
            if value is not None:
                setattr(row, key, value)
        db.add(row)
        await db.flush()
        return row

    async def delete_group(self, db: AsyncSession, *, group_id: int) -> int:
        result = await db.execute(delete(DiagnosticTestGroup).where(DiagnosticTestGroup.group_id == group_id))
        return int(result.rowcount or 0)

    async def get_assigned_test_ids_for_group(self, db: AsyncSession, *, group_id: int) -> set[int]:
        result = await db.execute(
            select(DiagnosticTestGroupTest.test_id).where(DiagnosticTestGroupTest.group_id == group_id)
        )
        return set(result.scalars().all())

    async def get_assigned_test_ids_for_group_ordered(self, db: AsyncSession, *, group_id: int) -> list[int]:
        result = await db.execute(
            select(DiagnosticTestGroupTest.test_id)
            .where(DiagnosticTestGroupTest.group_id == group_id)
            .order_by(DiagnosticTestGroupTest.display_order.asc().nulls_last(), DiagnosticTestGroupTest.id.asc())
        )
        return list(result.scalars().all())

    async def assign_tests_to_group(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        test_ids: list[int],
    ) -> tuple[list[int], list[int]]:
        unique_ids = list(dict.fromkeys(test_ids))
        added: list[int] = []
        skipped: list[int] = []
        max_order_result = await db.execute(
            select(func.max(DiagnosticTestGroupTest.display_order)).where(DiagnosticTestGroupTest.group_id == group_id)
        )
        max_order = max_order_result.scalar_one_or_none()
        next_order = (int(max_order) if max_order is not None else 0) + 1

        for test_id in unique_ids:
            stmt = (
                insert(DiagnosticTestGroupTest)
                .values(group_id=group_id, test_id=test_id, display_order=next_order)
                .on_conflict_do_nothing(index_elements=["group_id", "test_id"])
                .returning(DiagnosticTestGroupTest.id)
            )
            result = await db.execute(stmt)
            inserted_id = result.scalar_one_or_none()
            if inserted_id is None:
                skipped.append(test_id)
            else:
                added.append(test_id)
                next_order += 1
        return added, skipped

    async def remove_test_from_group(self, db: AsyncSession, *, group_id: int, test_id: int) -> bool:
        result = await db.execute(
            delete(DiagnosticTestGroupTest).where(
                DiagnosticTestGroupTest.group_id == group_id,
                DiagnosticTestGroupTest.test_id == test_id,
            )
        )
        return int(result.rowcount or 0) > 0

    async def reorder_group_tests(self, db: AsyncSession, *, group_id: int, test_ids: list[int]) -> None:
        for index, test_id in enumerate(test_ids, start=1):
            await db.execute(
                sql_update(DiagnosticTestGroupTest)
                .where(
                    DiagnosticTestGroupTest.group_id == group_id,
                    DiagnosticTestGroupTest.test_id == test_id,
                )
                .values(display_order=index)
            )
        await db.flush()

    async def get_package_test_groups(
        self,
        db: AsyncSession,
        *,
        package_id: int,
    ) -> list[tuple[DiagnosticTestGroup, list[HealthParameter]]]:
        result = await db.execute(
            select(DiagnosticTestGroup)
            .join(DiagnosticPackageTestGroup, DiagnosticPackageTestGroup.group_id == DiagnosticTestGroup.group_id)
            .where(DiagnosticPackageTestGroup.diagnostic_package_id == package_id)
            .order_by(DiagnosticPackageTestGroup.display_order.asc().nulls_last(), DiagnosticTestGroup.group_id.asc())
        )
        groups = list(result.scalars().all())
        response: list[tuple[DiagnosticTestGroup, list[HealthParameter]]] = []
        for group in groups:
            tests = await self.get_parameters_for_group(db, group_id=group.group_id)
            response.append((group, tests))
        return response

    async def get_assigned_group_ids_for_package(self, db: AsyncSession, *, package_id: int) -> set[int]:
        result = await db.execute(
            select(DiagnosticPackageTestGroup.group_id).where(
                DiagnosticPackageTestGroup.diagnostic_package_id == package_id
            )
        )
        return set(result.scalars().all())

    async def get_assigned_group_ids_for_package_ordered(self, db: AsyncSession, *, package_id: int) -> list[int]:
        result = await db.execute(
            select(DiagnosticPackageTestGroup.group_id)
            .where(DiagnosticPackageTestGroup.diagnostic_package_id == package_id)
            .order_by(DiagnosticPackageTestGroup.display_order.asc().nulls_last(), DiagnosticPackageTestGroup.id.asc())
        )
        return list(result.scalars().all())

    async def assign_groups_to_package(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        group_ids: list[int],
    ) -> tuple[list[int], list[int]]:
        unique_ids = list(dict.fromkeys(group_ids))
        added: list[int] = []
        skipped: list[int] = []
        max_order_result = await db.execute(
            select(func.max(DiagnosticPackageTestGroup.display_order)).where(
                DiagnosticPackageTestGroup.diagnostic_package_id == package_id
            )
        )
        max_order = max_order_result.scalar_one_or_none()
        next_order = (int(max_order) if max_order is not None else 0) + 1

        for group_id in unique_ids:
            stmt = (
                insert(DiagnosticPackageTestGroup)
                .values(diagnostic_package_id=package_id, group_id=group_id, display_order=next_order)
                .on_conflict_do_nothing(index_elements=["diagnostic_package_id", "group_id"])
                .returning(DiagnosticPackageTestGroup.id)
            )
            result = await db.execute(stmt)
            inserted_id = result.scalar_one_or_none()
            if inserted_id is None:
                skipped.append(group_id)
            else:
                added.append(group_id)
                next_order += 1
        return added, skipped

    async def remove_group_from_package(self, db: AsyncSession, *, package_id: int, group_id: int) -> bool:
        result = await db.execute(
            delete(DiagnosticPackageTestGroup).where(
                DiagnosticPackageTestGroup.diagnostic_package_id == package_id,
                DiagnosticPackageTestGroup.group_id == group_id,
            )
        )
        return int(result.rowcount or 0) > 0

    async def reorder_package_groups(self, db: AsyncSession, *, package_id: int, group_ids: list[int]) -> None:
        for index, group_id in enumerate(group_ids, start=1):
            await db.execute(
                sql_update(DiagnosticPackageTestGroup)
                .where(
                    DiagnosticPackageTestGroup.diagnostic_package_id == package_id,
                    DiagnosticPackageTestGroup.group_id == group_id,
                )
                .values(display_order=index)
            )
        await db.flush()

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
