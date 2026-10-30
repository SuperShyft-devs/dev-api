"""Diagnostics repository.

Only database queries live here.
"""

from __future__ import annotations

from common.listing import ilike_pattern
from sqlalchemy import String, case, cast, delete, distinct, func, or_, select, update as sql_update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from modules.diagnostics.models import (
    DiagnosticPackage,
    DiagnosticPackageGroup,
    DiagnosticProfileGroup,
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

    def _apply_package_list_filters(
        self,
        query,
        *,
        gender: str | None = None,
        tag: str | None = None,
        filter_chip: str | None = None,
        active_only: bool = True,
        public_only: bool = False,
        created_by_user_id: int | None = None,
        package_for: str | None = None,
        search: str | None = None,
        diagnostic_provider: str | None = None,
        status: str | None = None,
        default_for_group_only: bool = False,
    ):
        if active_only:
            query = query.where(DiagnosticPackage.status == "active")

        if package_for is not None:
            query = query.where(DiagnosticPackage.package_for == package_for)

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

        if search:
            pattern = ilike_pattern(search)
            query = query.where(DiagnosticPackage.package_name.ilike(pattern, escape="\\"))

        if diagnostic_provider is not None:
            query = query.where(DiagnosticPackage.diagnostic_provider == diagnostic_provider)

        if status is not None:
            query = query.where(DiagnosticPackage.status == status)

        if default_for_group_only:
            query = query.where(DiagnosticPackage.is_default_for_group.is_(True))

        return query

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
        package_for: str | None = None,
        search: str | None = None,
        diagnostic_provider: str | None = None,
        status: str | None = None,
        default_for_group_only: bool = False,
        page: int | None = None,
        limit: int | None = None,
    ) -> list[DiagnosticPackage]:
        query = select(DiagnosticPackage)
        query = self._apply_package_list_filters(
            query,
            gender=gender,
            tag=tag,
            filter_chip=filter_chip,
            active_only=active_only,
            public_only=public_only,
            created_by_user_id=created_by_user_id,
            package_for=package_for,
            search=search,
            diagnostic_provider=diagnostic_provider,
            status=status,
            default_for_group_only=default_for_group_only,
        )
        query = query.order_by(
            DiagnosticPackage.display_order.asc().nulls_last(),
            DiagnosticPackage.diagnostic_package_id.desc(),
        )
        if page is not None and limit is not None:
            offset = (page - 1) * limit
            query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_packages(
        self,
        db: AsyncSession,
        *,
        gender: str | None = None,
        tag: str | None = None,
        filter_chip: str | None = None,
        active_only: bool = True,
        public_only: bool = False,
        created_by_user_id: int | None = None,
        package_for: str | None = None,
        search: str | None = None,
        diagnostic_provider: str | None = None,
        status: str | None = None,
        default_for_group_only: bool = False,
    ) -> int:
        query = select(func.count()).select_from(DiagnosticPackage)
        query = self._apply_package_list_filters(
            query,
            gender=gender,
            tag=tag,
            filter_chip=filter_chip,
            active_only=active_only,
            public_only=public_only,
            created_by_user_id=created_by_user_id,
            package_for=package_for,
            search=search,
            diagnostic_provider=diagnostic_provider,
            status=status,
            default_for_group_only=default_for_group_only,
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_distinct_package_tag_names(
        self,
        db: AsyncSession,
        *,
        gender: str | None = None,
        tag: str | None = None,
        filter_chip: str | None = None,
        active_only: bool = True,
        public_only: bool = False,
        created_by_user_id: int | None = None,
        package_for: str | None = None,
        search: str | None = None,
        diagnostic_provider: str | None = None,
        status: str | None = None,
        default_for_group_only: bool = False,
    ) -> list[str]:
        pkg_ids_query = select(DiagnosticPackage.diagnostic_package_id)
        pkg_ids_query = self._apply_package_list_filters(
            pkg_ids_query,
            gender=gender,
            tag=tag,
            filter_chip=filter_chip,
            active_only=active_only,
            public_only=public_only,
            created_by_user_id=created_by_user_id,
            package_for=package_for,
            search=search,
            diagnostic_provider=diagnostic_provider,
            status=status,
            default_for_group_only=default_for_group_only,
        )
        result = await db.execute(
            select(distinct(DiagnosticPackageTag.tag_name))
            .where(DiagnosticPackageTag.diagnostic_package_id.in_(pkg_ids_query))
            .order_by(DiagnosticPackageTag.tag_name.asc())
        )
        return [row[0] for row in result.all() if row[0]]

    async def list_tags_for_package_ids(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> dict[int, list[DiagnosticPackageTag]]:
        if not package_ids:
            return {}
        result = await db.execute(
            select(DiagnosticPackageTag)
            .where(DiagnosticPackageTag.diagnostic_package_id.in_(package_ids))
            .order_by(
                DiagnosticPackageTag.diagnostic_package_id.asc(),
                DiagnosticPackageTag.display_order.asc().nulls_last(),
                DiagnosticPackageTag.tag_id.asc(),
            )
        )
        out: dict[int, list[DiagnosticPackageTag]] = {int(pid): [] for pid in package_ids}
        for tag in result.scalars().all():
            out.setdefault(int(tag.diagnostic_package_id), []).append(tag)
        return out

    async def list_filter_chip_links_for_package_ids(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> dict[int, list[DiagnosticPackageFilterChipLink]]:
        if not package_ids:
            return {}
        result = await db.execute(
            select(DiagnosticPackageFilterChipLink)
            .where(DiagnosticPackageFilterChipLink.diagnostic_package_id.in_(package_ids))
            .options(selectinload(DiagnosticPackageFilterChipLink.filter_chip))
            .order_by(
                DiagnosticPackageFilterChipLink.diagnostic_package_id.asc(),
                DiagnosticPackageFilterChipLink.display_order.asc().nulls_last(),
                DiagnosticPackageFilterChipLink.link_id.asc(),
            )
        )
        out: dict[int, list[DiagnosticPackageFilterChipLink]] = {int(pid): [] for pid in package_ids}
        for link in result.scalars().all():
            pkg_id = link.diagnostic_package_id
            if pkg_id is None:
                continue
            out.setdefault(int(pkg_id), []).append(link)
        return out

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

    async def screening_flags_for_packages(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> dict[int, tuple[bool, bool]]:
        """OR-aggregate has_allergy_screening / has_cancer_screening across assigned test groups."""
        if not package_ids:
            return {}
        pkg_col = DiagnosticPackageTestGroup.diagnostic_package_id
        query = (
            select(
                pkg_col.label("diagnostic_package_id"),
                func.bool_or(DiagnosticTestGroup.has_allergy_screening).label("has_allergy"),
                func.bool_or(DiagnosticTestGroup.has_cancer_screening).label("has_cancer"),
            )
            .select_from(DiagnosticPackageTestGroup)
            .join(
                DiagnosticTestGroup,
                DiagnosticTestGroup.group_id == DiagnosticPackageTestGroup.group_id,
            )
            .where(pkg_col.in_(package_ids))
            .group_by(pkg_col)
        )
        result = await db.execute(query)
        out = {int(pid): (False, False) for pid in package_ids}
        for row in result.all():
            out[int(row.diagnostic_package_id)] = (
                bool(row.has_allergy),
                bool(row.has_cancer),
            )
        return out

    async def count_unmapped_tests_for_packages(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> dict[int, int]:
        """Distinct tests whose provider parameter key is null or blank.

        Orange Health uses ``orangehealth_parameter_key``. Every other provider
        uses ``healthians_parameter_key``, matching ``provider_parameter_key``.
        """
        if not package_ids:
            return {}
        pkg_col = DiagnosticPackageTestGroup.diagnostic_package_id
        provider_text = func.lower(func.trim(cast(DiagnosticPackage.diagnostic_provider, String)))
        is_orange = provider_text == "orange_health"
        relevant_key = case(
            (is_orange, HealthParameter.orangehealth_parameter_key),
            else_=HealthParameter.healthians_parameter_key,
        )
        unmapped = or_(relevant_key.is_(None), func.trim(relevant_key) == "")
        query = (
            select(
                pkg_col.label("diagnostic_package_id"),
                func.count(distinct(DiagnosticTestGroupTest.test_id)).label("cnt"),
            )
            .select_from(DiagnosticPackageTestGroup)
            .join(
                DiagnosticPackage,
                DiagnosticPackage.diagnostic_package_id == pkg_col,
            )
            .join(
                DiagnosticTestGroupTest,
                DiagnosticTestGroupTest.group_id == DiagnosticPackageTestGroup.group_id,
            )
            .join(
                HealthParameter,
                HealthParameter.test_id == DiagnosticTestGroupTest.test_id,
            )
            .where(pkg_col.in_(package_ids), unmapped)
            .group_by(pkg_col)
        )
        result = await db.execute(query)
        rows = result.all()
        out = {int(pid): 0 for pid in package_ids}
        for row in rows:
            out[int(row.diagnostic_package_id)] = int(row.cnt or 0)
        return out

    async def packages_with_unmapped_tests(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> set[int]:
        """Package ids that have at least one distinct test with a missing provider key."""
        if not package_ids:
            return set()
        pkg_col = DiagnosticPackageTestGroup.diagnostic_package_id
        provider_text = func.lower(func.trim(cast(DiagnosticPackage.diagnostic_provider, String)))
        is_orange = provider_text == "orange_health"
        relevant_key = case(
            (is_orange, HealthParameter.orangehealth_parameter_key),
            else_=HealthParameter.healthians_parameter_key,
        )
        unmapped = or_(relevant_key.is_(None), func.trim(relevant_key) == "")
        query = (
            select(pkg_col.label("diagnostic_package_id"))
            .select_from(DiagnosticPackageTestGroup)
            .join(
                DiagnosticPackage,
                DiagnosticPackage.diagnostic_package_id == pkg_col,
            )
            .join(
                DiagnosticTestGroupTest,
                DiagnosticTestGroupTest.group_id == DiagnosticPackageTestGroup.group_id,
            )
            .join(
                HealthParameter,
                HealthParameter.test_id == DiagnosticTestGroupTest.test_id,
            )
            .where(pkg_col.in_(package_ids), unmapped)
            .distinct()
        )
        result = await db.execute(query)
        return {int(row.diagnostic_package_id) for row in result.all()}

    async def packages_with_null_provider_test_groups(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> set[int]:
        """Package ids with at least one assigned test group whose diagnostic_provider is null."""
        if not package_ids:
            return set()
        pkg_col = DiagnosticPackageTestGroup.diagnostic_package_id
        query = (
            select(pkg_col.label("diagnostic_package_id"))
            .select_from(DiagnosticPackageTestGroup)
            .join(DiagnosticTestGroup, DiagnosticTestGroup.group_id == DiagnosticPackageTestGroup.group_id)
            .where(
                pkg_col.in_(package_ids),
                DiagnosticTestGroup.diagnostic_provider.is_(None),
            )
            .distinct()
        )
        result = await db.execute(query)
        return {int(row.diagnostic_package_id) for row in result.all()}

    async def create_profile_group(self, db: AsyncSession) -> DiagnosticProfileGroup:
        group = DiagnosticProfileGroup()
        db.add(group)
        await db.flush()
        return group

    async def list_test_groups_by_profile_group_ids(
        self,
        db: AsyncSession,
        *,
        profile_group_ids: list[int],
    ) -> dict[int, list[DiagnosticTestGroup]]:
        if not profile_group_ids:
            return {}
        result = await db.execute(
            select(DiagnosticTestGroup).where(DiagnosticTestGroup.profile_group_id.in_(profile_group_ids))
        )
        grouped: dict[int, list[DiagnosticTestGroup]] = {}
        for row in result.scalars().all():
            if row.profile_group_id is None:
                continue
            grouped.setdefault(int(row.profile_group_id), []).append(row)
        return grouped

    async def list_test_groups_in_profile_group(
        self,
        db: AsyncSession,
        *,
        profile_group_id: int,
    ) -> list[DiagnosticTestGroup]:
        result = await db.execute(
            select(DiagnosticTestGroup).where(DiagnosticTestGroup.profile_group_id == profile_group_id)
        )
        return list(result.scalars().all())

    async def profile_group_has_default(self, db: AsyncSession, *, profile_group_id: int) -> bool:
        result = await db.execute(
            select(func.count())
            .select_from(DiagnosticTestGroup)
            .where(
                DiagnosticTestGroup.profile_group_id == profile_group_id,
                DiagnosticTestGroup.is_default_for_group.is_(True),
            )
        )
        return int(result.scalar_one()) > 0

    async def set_test_group_as_profile_default(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        profile_group_id: int,
    ) -> None:
        await db.execute(
            sql_update(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.profile_group_id == profile_group_id)
            .values(is_default_for_group=False)
        )
        row = await self.get_group_by_id(db, group_id=group_id)
        if row is not None:
            row.is_default_for_group = True
            db.add(row)
            await db.flush()

    async def promote_min_price_default_in_profile_group(
        self,
        db: AsyncSession,
        *,
        profile_group_id: int,
        exclude_group_id: int | None = None,
    ) -> None:
        members = await self.list_test_groups_in_profile_group(db, profile_group_id=profile_group_id)
        candidates = [
            member
            for member in members
            if exclude_group_id is None or member.group_id != exclude_group_id
        ]
        if not candidates:
            return
        await db.execute(
            sql_update(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.profile_group_id == profile_group_id)
            .values(is_default_for_group=False)
        )
        chosen = min(
            candidates,
            key=lambda row: (
                row.price is None,
                float(row.price) if row.price is not None else 0,
                row.group_id,
            ),
        )
        chosen.is_default_for_group = True
        db.add(chosen)
        await db.flush()

    async def copy_tests_and_chips_between_groups(
        self,
        db: AsyncSession,
        *,
        source_group_id: int,
        target_group_id: int,
    ) -> None:
        source = await db.execute(
            select(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.group_id == source_group_id)
            .options(
                selectinload(DiagnosticTestGroup.tests),
                selectinload(DiagnosticTestGroup.filter_chip_links),
            )
        )
        source_row = source.unique().scalar_one_or_none()
        if source_row is None:
            return
        ordered_tests = sorted(
            source_row.tests,
            key=lambda row: (row.display_order is None, row.display_order or 0, row.id),
        )
        for link in ordered_tests:
            db.add(
                DiagnosticTestGroupTest(
                    group_id=target_group_id,
                    test_id=link.test_id,
                    display_order=link.display_order,
                )
            )
        for chip_link in source_row.filter_chip_links:
            if chip_link.diagnostic_package_id is not None:
                continue
            db.add(
                DiagnosticPackageFilterChipLink(
                    group_id=target_group_id,
                    filter_chip_id=chip_link.filter_chip_id,
                    display_order=chip_link.display_order,
                    diagnostic_package_id=None,
                )
            )
        await db.flush()

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

    async def create_package_group(self, db: AsyncSession) -> DiagnosticPackageGroup:
        group = DiagnosticPackageGroup()
        db.add(group)
        await db.flush()
        return group

    async def list_packages_by_group_ids(
        self,
        db: AsyncSession,
        *,
        group_ids: list[int],
    ) -> dict[int, list[DiagnosticPackage]]:
        if not group_ids:
            return {}
        result = await db.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.package_group_id.in_(group_ids))
        )
        grouped: dict[int, list[DiagnosticPackage]] = {}
        for row in result.scalars().all():
            if row.package_group_id is None:
                continue
            grouped.setdefault(int(row.package_group_id), []).append(row)
        return grouped

    async def external_package_codes_for_ids(
        self,
        db: AsyncSession,
        *,
        package_ids: list[int],
    ) -> dict[int, str | None]:
        if not package_ids:
            return {}
        result = await db.execute(
            select(
                DiagnosticPackage.diagnostic_package_id,
                DiagnosticPackage.external_package_code,
            ).where(DiagnosticPackage.diagnostic_package_id.in_(package_ids))
        )
        return {int(row.diagnostic_package_id): row.external_package_code for row in result.all()}

    async def list_packages_in_group(self, db: AsyncSession, *, group_id: int) -> list[DiagnosticPackage]:
        result = await db.execute(
            select(DiagnosticPackage).where(DiagnosticPackage.package_group_id == group_id)
        )
        return list(result.scalars().all())

    async def list_packages_for_test_group(
        self,
        db: AsyncSession,
        *,
        group_id: int,
    ) -> list[DiagnosticPackage]:
        result = await db.execute(
            select(DiagnosticPackage)
            .join(
                DiagnosticPackageTestGroup,
                DiagnosticPackageTestGroup.diagnostic_package_id == DiagnosticPackage.diagnostic_package_id,
            )
            .where(DiagnosticPackageTestGroup.group_id == group_id)
            .order_by(DiagnosticPackage.package_name.asc(), DiagnosticPackage.diagnostic_package_id.asc())
        )
        return list(result.scalars().unique().all())

    async def group_has_default(self, db: AsyncSession, *, group_id: int) -> bool:
        result = await db.execute(
            select(func.count())
            .select_from(DiagnosticPackage)
            .where(
                DiagnosticPackage.package_group_id == group_id,
                DiagnosticPackage.is_default_for_group.is_(True),
            )
        )
        return int(result.scalar_one()) > 0

    async def set_package_as_group_default(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        group_id: int,
    ) -> None:
        await db.execute(
            sql_update(DiagnosticPackage)
            .where(DiagnosticPackage.package_group_id == group_id)
            .values(is_default_for_group=False)
        )
        package = await self.get_package_by_id_basic(db, package_id=package_id)
        if package is not None:
            package.is_default_for_group = True
            db.add(package)
            await db.flush()

    async def promote_min_price_default_in_group(
        self,
        db: AsyncSession,
        *,
        group_id: int,
        exclude_package_id: int | None = None,
    ) -> None:
        members = await self.list_packages_in_group(db, group_id=group_id)
        candidates = [
            member
            for member in members
            if exclude_package_id is None or member.diagnostic_package_id != exclude_package_id
        ]
        if not candidates:
            return
        await db.execute(
            sql_update(DiagnosticPackage)
            .where(DiagnosticPackage.package_group_id == group_id)
            .values(is_default_for_group=False)
        )
        chosen = min(
            candidates,
            key=lambda row: (
                row.price is None,
                float(row.price) if row.price is not None else 0,
                row.diagnostic_package_id,
            ),
        )
        chosen.is_default_for_group = True
        db.add(chosen)
        await db.flush()

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

    async def delete_package(self, db: AsyncSession, *, package_id: int) -> int:
        result = await db.execute(
            delete(DiagnosticPackage).where(DiagnosticPackage.diagnostic_package_id == package_id)
        )
        return int(result.rowcount or 0)

    async def duplicate_package(self, db: AsyncSession, *, package_id: int) -> DiagnosticPackage | None:
        result = await db.execute(
            select(DiagnosticPackage)
            .where(DiagnosticPackage.diagnostic_package_id == package_id)
            .options(
                selectinload(DiagnosticPackage.reasons),
                selectinload(DiagnosticPackage.tags),
                selectinload(DiagnosticPackage.test_group_assignments),
                selectinload(DiagnosticPackage.samples),
                selectinload(DiagnosticPackage.preparations),
                selectinload(DiagnosticPackage.filter_chip_links),
            )
        )
        original = result.unique().scalar_one_or_none()
        if original is None:
            return None

        duplicated = DiagnosticPackage(
            reference_id=original.reference_id,
            package_name=f"{original.package_name} copy",
            package_image=original.package_image,
            diagnostic_provider=original.diagnostic_provider,
            external_package_code=original.external_package_code,
            created_by_user_id=original.created_by_user_id,
            report_duration_hours=original.report_duration_hours,
            collection_type=original.collection_type,
            health_areas_covered=original.health_areas_covered,
            about_text=original.about_text,
            bookings_count=0,
            price=original.price,
            original_price=original.original_price,
            is_most_popular=original.is_most_popular,
            complementary_consultation=original.complementary_consultation,
            gender_suitability=original.gender_suitability,
            status=original.status,
            package_for=original.package_for,
            display_order=original.display_order,
        )
        db.add(duplicated)
        await db.flush()

        new_group = DiagnosticPackageGroup()
        db.add(new_group)
        await db.flush()
        duplicated.package_group_id = new_group.package_group_id
        duplicated.is_default_for_group = True
        db.add(duplicated)

        for r in original.reasons:
            db.add(
                DiagnosticPackageReason(
                    diagnostic_package_id=duplicated.diagnostic_package_id,
                    display_order=r.display_order,
                    reason_text=r.reason_text,
                )
            )

        for t in original.tags:
            db.add(
                DiagnosticPackageTag(
                    diagnostic_package_id=duplicated.diagnostic_package_id,
                    tag_name=t.tag_name,
                    display_order=t.display_order,
                )
            )

        for tg in original.test_group_assignments:
            db.add(
                DiagnosticPackageTestGroup(
                    diagnostic_package_id=duplicated.diagnostic_package_id,
                    group_id=tg.group_id,
                    display_order=tg.display_order,
                )
            )

        for s in original.samples:
            db.add(
                DiagnosticPackageSample(
                    diagnostic_package_id=duplicated.diagnostic_package_id,
                    sample_type=s.sample_type,
                    description=s.description,
                    display_order=s.display_order,
                )
            )

        for p in original.preparations:
            db.add(
                DiagnosticPackagePreparation(
                    diagnostic_package_id=duplicated.diagnostic_package_id,
                    preparation_title=p.preparation_title,
                    steps=p.steps,
                    display_order=p.display_order,
                )
            )

        for fcl in original.filter_chip_links:
            if fcl.diagnostic_package_id is not None:
                db.add(
                    DiagnosticPackageFilterChipLink(
                        diagnostic_package_id=duplicated.diagnostic_package_id,
                        filter_chip_id=fcl.filter_chip_id,
                        display_order=fcl.display_order,
                    )
                )

        await db.flush()
        return await self.get_package_by_id(db, package_id=duplicated.diagnostic_package_id)

    async def reorder_packages(self, db: AsyncSession, *, package_ids: list[int]) -> None:
        for index, package_id in enumerate(package_ids, start=1):
            await db.execute(
                sql_update(DiagnosticPackage)
                .where(DiagnosticPackage.diagnostic_package_id == package_id)
                .values(display_order=index)
            )
        await db.flush()

    async def reorder_filter_chips(self, db: AsyncSession, *, filter_chip_ids: list[int]) -> None:
        from sqlalchemy import update as sql_update

        for index, chip_id in enumerate(filter_chip_ids, start=1):
            await db.execute(
                sql_update(DiagnosticPackageFilterChip)
                .where(DiagnosticPackageFilterChip.filter_chip_id == chip_id)
                .values(display_order=index)
            )
        await db.flush()

    async def reorder_package_reasons(
        self, db: AsyncSession, *, package_id: int, reason_ids: list[int]
    ) -> None:
        from sqlalchemy import update as sql_update

        for index, reason_id in enumerate(reason_ids, start=1):
            await db.execute(
                sql_update(DiagnosticPackageReason)
                .where(
                    DiagnosticPackageReason.reason_id == reason_id,
                    DiagnosticPackageReason.diagnostic_package_id == package_id,
                )
                .values(display_order=index)
            )
        await db.flush()

    def _apply_filter_chip_list_filters(
        self,
        query,
        *,
        chip_for: str | None = None,
        active_only: bool = True,
        search: str | None = None,
    ):
        if active_only:
            query = query.where(DiagnosticPackageFilterChip.status == "active")
        if chip_for is not None:
            query = query.where(DiagnosticPackageFilterChip.chip_for == chip_for)
        if search:
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    DiagnosticPackageFilterChip.display_name.ilike(pattern, escape="\\"),
                    DiagnosticPackageFilterChip.chip_key.ilike(pattern, escape="\\"),
                )
            )
        return query

    async def get_all_filter_chips(
        self,
        db: AsyncSession,
        *,
        chip_for: str | None = None,
        active_only: bool = True,
        search: str | None = None,
        page: int | None = None,
        limit: int | None = None,
    ) -> list[DiagnosticPackageFilterChip]:
        query = select(DiagnosticPackageFilterChip)
        query = self._apply_filter_chip_list_filters(
            query, chip_for=chip_for, active_only=active_only, search=search
        )
        query = query.order_by(
            DiagnosticPackageFilterChip.display_order.asc().nulls_last(),
            DiagnosticPackageFilterChip.filter_chip_id.asc(),
        )
        if page is not None and limit is not None:
            offset = (page - 1) * limit
            query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_filter_chips(
        self,
        db: AsyncSession,
        *,
        chip_for: str | None = None,
        active_only: bool = True,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(DiagnosticPackageFilterChip)
        query = self._apply_filter_chip_list_filters(
            query, chip_for=chip_for, active_only=active_only, search=search
        )
        result = await db.execute(query)
        return int(result.scalar_one())

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

    def _apply_parameter_list_filters(
        self,
        query,
        *,
        parameter_type: ParameterType | None = None,
        search: str | None = None,
    ):
        if parameter_type is not None:
            query = query.where(HealthParameter.parameter_type == parameter_type)
        if search:
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    HealthParameter.test_name.ilike(pattern, escape="\\"),
                    HealthParameter.parameter_key.ilike(pattern, escape="\\"),
                )
            )
        return query

    async def get_all_parameters(
        self,
        db: AsyncSession,
        *,
        parameter_type: ParameterType | None = None,
        search: str | None = None,
        page: int | None = None,
        limit: int | None = None,
    ) -> list[HealthParameter]:
        query = select(HealthParameter)
        query = self._apply_parameter_list_filters(query, parameter_type=parameter_type, search=search)
        query = query.order_by(
            HealthParameter.display_order.asc().nulls_last(),
            HealthParameter.test_id.asc(),
        )
        if page is not None and limit is not None:
            offset = (page - 1) * limit
            query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def count_parameters(
        self,
        db: AsyncSession,
        *,
        parameter_type: ParameterType | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(HealthParameter)
        query = self._apply_parameter_list_filters(query, parameter_type=parameter_type, search=search)
        result = await db.execute(query)
        return int(result.scalar_one())

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

    def _apply_group_list_filters(
        self,
        query,
        *,
        filter_chip_key: str | None = None,
        package_for: str | None = None,
        search: str | None = None,
        catalog_defaults_only: bool = False,
        exclude_forked: bool = False,
    ):
        if catalog_defaults_only:
            query = query.where(DiagnosticTestGroup.is_default_for_group.is_(True))
        if exclude_forked:
            query = query.where(DiagnosticTestGroup.forked_from_group_id.is_(None))
        if package_for is not None:
            query = query.where(DiagnosticTestGroup.package_for == package_for)
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
        if search:
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    DiagnosticTestGroup.group_name.ilike(pattern, escape="\\"),
                    DiagnosticTestGroup.group_key.ilike(pattern, escape="\\"),
                )
            )
        return query

    async def get_all_groups(
        self,
        db: AsyncSession,
        *,
        filter_chip_key: str | None = None,
        package_for: str | None = None,
        search: str | None = None,
        page: int | None = None,
        limit: int | None = None,
    ) -> list[tuple[DiagnosticTestGroup, int]]:
        query = (
            select(DiagnosticTestGroup, func.count(DiagnosticTestGroupTest.id))
            .outerjoin(DiagnosticTestGroupTest, DiagnosticTestGroupTest.group_id == DiagnosticTestGroup.group_id)
        )
        query = self._apply_group_list_filters(
            query,
            filter_chip_key=filter_chip_key,
            package_for=package_for,
            search=search,
        )
        query = query.group_by(DiagnosticTestGroup.group_id).order_by(
            DiagnosticTestGroup.display_order.asc().nulls_last(),
            DiagnosticTestGroup.group_id.asc(),
        )
        if page is not None and limit is not None:
            offset = (page - 1) * limit
            query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return [(row[0], int(row[1] or 0)) for row in result.all()]

    async def get_catalog_test_groups(
        self,
        db: AsyncSession,
        *,
        filter_chip_key: str | None = None,
        package_for: str | None = None,
        search: str | None = None,
        page: int | None = None,
        limit: int | None = None,
    ) -> list[tuple[DiagnosticTestGroup, int]]:
        query = (
            select(DiagnosticTestGroup, func.count(DiagnosticTestGroupTest.id))
            .outerjoin(DiagnosticTestGroupTest, DiagnosticTestGroupTest.group_id == DiagnosticTestGroup.group_id)
        )
        query = self._apply_group_list_filters(
            query,
            filter_chip_key=filter_chip_key,
            package_for=package_for,
            search=search,
            catalog_defaults_only=True,
            exclude_forked=True,
        )
        query = query.group_by(DiagnosticTestGroup.group_id).order_by(
            DiagnosticTestGroup.display_order.asc().nulls_last(),
            DiagnosticTestGroup.group_id.asc(),
        )
        if page is not None and limit is not None:
            offset = (page - 1) * limit
            query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return [(row[0], int(row[1] or 0)) for row in result.all()]

    async def count_catalog_test_groups(
        self,
        db: AsyncSession,
        *,
        filter_chip_key: str | None = None,
        package_for: str | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count(distinct(DiagnosticTestGroup.group_id))).select_from(DiagnosticTestGroup)
        query = query.outerjoin(
            DiagnosticTestGroupTest, DiagnosticTestGroupTest.group_id == DiagnosticTestGroup.group_id
        )
        query = self._apply_group_list_filters(
            query,
            filter_chip_key=filter_chip_key,
            package_for=package_for,
            search=search,
            catalog_defaults_only=True,
            exclude_forked=True,
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def count_groups(
        self,
        db: AsyncSession,
        *,
        filter_chip_key: str | None = None,
        package_for: str | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count(distinct(DiagnosticTestGroup.group_id))).select_from(DiagnosticTestGroup)
        query = query.outerjoin(
            DiagnosticTestGroupTest, DiagnosticTestGroupTest.group_id == DiagnosticTestGroup.group_id
        )
        query = self._apply_group_list_filters(
            query,
            filter_chip_key=filter_chip_key,
            package_for=package_for,
            search=search,
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def get_group_by_id(self, db: AsyncSession, *, group_id: int) -> DiagnosticTestGroup | None:
        result = await db.execute(select(DiagnosticTestGroup).where(DiagnosticTestGroup.group_id == group_id))
        return result.scalar_one_or_none()

    async def get_group_by_group_key(self, db: AsyncSession, *, group_key: str) -> DiagnosticTestGroup | None:
        result = await db.execute(
            select(DiagnosticTestGroup).where(DiagnosticTestGroup.group_key == group_key)
        )
        return result.scalar_one_or_none()

    async def package_has_group_assignment(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        group_id: int,
    ) -> bool:
        result = await db.execute(
            select(DiagnosticPackageTestGroup.id).where(
                DiagnosticPackageTestGroup.diagnostic_package_id == package_id,
                DiagnosticPackageTestGroup.group_id == group_id,
            )
        )
        return result.scalar_one_or_none() is not None

    async def get_package_assigned_fork(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        catalog_group_id: int,
    ) -> DiagnosticTestGroup | None:
        result = await db.execute(
            select(DiagnosticTestGroup)
            .join(
                DiagnosticPackageTestGroup,
                DiagnosticPackageTestGroup.group_id == DiagnosticTestGroup.group_id,
            )
            .where(
                DiagnosticPackageTestGroup.diagnostic_package_id == package_id,
                DiagnosticTestGroup.forked_from_group_id == catalog_group_id,
            )
        )
        return result.scalar_one_or_none()

    async def allocate_unique_group_key(
        self,
        db: AsyncSession,
        *,
        base_key: str,
        package_id: int,
    ) -> str:
        candidates = [f"{base_key}_pkg_{package_id}"]
        for index in range(1, 100):
            candidates.append(f"{base_key}_fork_{index}")
        for candidate in candidates:
            existing = await self.get_group_by_group_key(db, group_key=candidate)
            if existing is None:
                return candidate
        raise RuntimeError("Could not allocate unique diagnostic test group key")

    async def duplicate_test_group_for_package(
        self,
        db: AsyncSession,
        *,
        source_group_id: int,
        forked_from_group_id: int,
        package_id: int,
    ) -> DiagnosticTestGroup | None:
        result = await db.execute(
            select(DiagnosticTestGroup)
            .where(DiagnosticTestGroup.group_id == source_group_id)
            .options(
                selectinload(DiagnosticTestGroup.tests),
                selectinload(DiagnosticTestGroup.filter_chip_links),
            )
        )
        source = result.unique().scalar_one_or_none()
        if source is None:
            return None

        package = await self.get_package_by_id_basic(db, package_id=package_id)
        fork_provider = package.diagnostic_provider if package is not None else None

        new_key = await self.allocate_unique_group_key(db, base_key=source.group_key, package_id=package_id)
        new_profile = await self.create_profile_group(db)
        copy = DiagnosticTestGroup(
            group_name=source.group_name,
            group_key=new_key,
            profile_group_id=new_profile.profile_group_id,
            test_group_image=source.test_group_image,
            diagnostic_provider=fork_provider,
            external_test_group_code=source.external_test_group_code,
            is_default_for_group=True,
            display_order=source.display_order,
            price=source.price,
            original_price=source.original_price,
            is_most_popular=source.is_most_popular,
            has_allergy_screening=source.has_allergy_screening,
            has_cancer_screening=source.has_cancer_screening,
            gender_suitability=source.gender_suitability,
            package_for=source.package_for,
            forked_from_group_id=forked_from_group_id,
        )
        db.add(copy)
        await db.flush()

        ordered_tests = sorted(
            source.tests,
            key=lambda row: (row.display_order is None, row.display_order or 0, row.id),
        )
        for link in ordered_tests:
            db.add(
                DiagnosticTestGroupTest(
                    group_id=copy.group_id,
                    test_id=link.test_id,
                    display_order=link.display_order,
                )
            )

        for chip_link in source.filter_chip_links:
            if chip_link.diagnostic_package_id is not None:
                continue
            db.add(
                DiagnosticPackageFilterChipLink(
                    group_id=copy.group_id,
                    filter_chip_id=chip_link.filter_chip_id,
                    display_order=chip_link.display_order,
                    diagnostic_package_id=None,
                )
            )
        await db.flush()
        return copy

    async def replace_package_group_assignment(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        old_group_id: int,
        new_group_id: int,
    ) -> bool:
        result = await db.execute(
            sql_update(DiagnosticPackageTestGroup)
            .where(
                DiagnosticPackageTestGroup.diagnostic_package_id == package_id,
                DiagnosticPackageTestGroup.group_id == old_group_id,
            )
            .values(group_id=new_group_id)
            .returning(DiagnosticPackageTestGroup.id)
        )
        return result.scalar_one_or_none() is not None

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
            if value is not None or key in ("external_test_group_code",):
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

    async def list_distinct_parameter_keys_for_package(
        self,
        db: AsyncSession,
        *,
        package_id: int,
    ) -> set[str]:
        result = await db.execute(
            select(distinct(HealthParameter.parameter_key))
            .select_from(DiagnosticPackageTestGroup)
            .join(
                DiagnosticTestGroupTest,
                DiagnosticTestGroupTest.group_id == DiagnosticPackageTestGroup.group_id,
            )
            .join(HealthParameter, HealthParameter.test_id == DiagnosticTestGroupTest.test_id)
            .where(DiagnosticPackageTestGroup.diagnostic_package_id == package_id)
            .where(HealthParameter.parameter_key.isnot(None))
        )
        keys: set[str] = set()
        for row in result.all():
            raw = row[0]
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                keys.add(text.lower())
        return keys

    async def map_parameters_by_healthians_keys(
        self,
        db: AsyncSession,
        *,
        keys: list[str],
    ) -> dict[str, HealthParameter]:
        normalized = [str(key).strip() for key in keys if str(key).strip()]
        if not normalized:
            return {}
        result = await db.execute(
            select(HealthParameter).where(
                func.trim(HealthParameter.healthians_parameter_key).in_(normalized)
            )
        )
        mapped: dict[str, HealthParameter] = {}
        for param in result.scalars().all():
            raw = param.healthians_parameter_key
            if raw is None:
                continue
            key = str(raw).strip()
            if key and key not in mapped:
                mapped[key] = param
        return mapped

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

    async def copy_test_groups_from_package(
        self,
        db: AsyncSession,
        *,
        source_package_id: int,
        target_package_id: int,
    ) -> int:
        result = await db.execute(
            select(DiagnosticPackageTestGroup, DiagnosticTestGroup)
            .join(DiagnosticTestGroup, DiagnosticPackageTestGroup.group_id == DiagnosticTestGroup.group_id)
            .where(DiagnosticPackageTestGroup.diagnostic_package_id == source_package_id)
            .order_by(
                DiagnosticPackageTestGroup.display_order.asc().nulls_last(),
                DiagnosticPackageTestGroup.id.asc(),
            )
        )
        copied = 0
        for assignment, group in result.all():
            catalog_root = group.forked_from_group_id or group.group_id
            copy = await self.duplicate_test_group_for_package(
                db,
                source_group_id=int(group.group_id),
                forked_from_group_id=int(catalog_root),
                package_id=target_package_id,
            )
            if copy is None:
                continue
            db.add(
                DiagnosticPackageTestGroup(
                    diagnostic_package_id=target_package_id,
                    group_id=copy.group_id,
                    display_order=assignment.display_order,
                )
            )
            copied += 1
        await db.flush()
        return copied

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
