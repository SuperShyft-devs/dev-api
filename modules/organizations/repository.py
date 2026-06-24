"""Organizations repository.

Only database queries live here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.engagements.models import Engagement
from modules.organizations.models import Organization
from modules.reports.models import CampReport


class OrganizationsRepository:
    """Organization database queries."""

    _ORG_SORT_COLUMNS = {
        "organization_id": Organization.organization_id,
        "name": Organization.name,
        "city": Organization.city,
        "country": Organization.country,
        "status": Organization.status,
        "organization_type": Organization.organization_type,
    }

    def _apply_org_list_filters(
        self,
        query,
        *,
        status: str | None = None,
        organization_type: str | None = None,
        bd_employee_id: int | None = None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
    ):
        if status is not None:
            query = query.where(Organization.status == status)
        if organization_type is not None:
            query = query.where(Organization.organization_type == organization_type)
        if bd_employee_id is not None:
            query = query.where(Organization.bd_employee_id == bd_employee_id)
        if city is not None and city.strip():
            query = query.where(func.lower(func.trim(Organization.city)) == city.strip().lower())
        if country is not None and country.strip():
            query = query.where(func.lower(func.trim(Organization.country)) == country.strip().lower())
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    Organization.name.ilike(pattern),
                    Organization.city.ilike(pattern),
                    Organization.country.ilike(pattern),
                )
            )
        return query

    async def get_by_id(self, db: AsyncSession, organization_id: int) -> Organization | None:
        result = await db.execute(select(Organization).where(Organization.organization_id == organization_id))
        return result.scalar_one_or_none()

    async def get_by_name(self, db: AsyncSession, name: str) -> Organization | None:
        result = await db.execute(select(Organization).where(Organization.name == name))
        return result.scalar_one_or_none()

    async def count_organizations(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        organization_type: str | None = None,
        bd_employee_id: int | None = None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Organization)
        query = self._apply_org_list_filters(
            query,
            status=status,
            organization_type=organization_type,
            bd_employee_id=bd_employee_id,
            search=search,
            city=city,
            country=country,
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_organizations(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        organization_type: str | None = None,
        bd_employee_id: int | None = None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[Organization]:
        offset = (page - 1) * limit
        query = select(Organization)
        query = self._apply_org_list_filters(
            query,
            status=status,
            organization_type=organization_type,
            bd_employee_id=bd_employee_id,
            search=search,
            city=city,
            country=country,
        )
        query = apply_sort(
            query,
            sort_by=sort_by,
            sort_dir=sort_dir,
            columns=self._ORG_SORT_COLUMNS,
            default_column=Organization.organization_id,
        )
        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_distinct_cities_and_countries(self, db: AsyncSession) -> tuple[list[str], list[str]]:
        city_result = await db.execute(
            select(func.distinct(func.trim(Organization.city)))
            .where(Organization.city.isnot(None))
            .where(func.trim(Organization.city) != "")
            .order_by(func.trim(Organization.city).asc())
        )
        country_result = await db.execute(
            select(func.distinct(func.trim(Organization.country)))
            .where(Organization.country.isnot(None))
            .where(func.trim(Organization.country) != "")
            .order_by(func.trim(Organization.country).asc())
        )
        cities = [str(v) for v in city_result.scalars().all() if v]
        countries = [str(v) for v in country_result.scalars().all() if v]
        return cities, countries

    async def create(self, db: AsyncSession, organization: Organization) -> Organization:
        db.add(organization)
        await db.flush()
        return organization

    async def update(self, db: AsyncSession, organization: Organization) -> Organization:
        organization.updated_at = datetime.now(timezone.utc)
        db.add(organization)
        await db.flush()
        return organization

    async def count_participants_by_organization_id(
        self,
        db: AsyncSession,
        *,
        organization_id: int,
    ) -> int:
        """Count distinct users enrolled across all engagements for an organization."""
        from sqlalchemy import func, select
        from modules.engagements.models import Engagement, EngagementParticipant

        query = (
            select(func.count(func.distinct(EngagementParticipant.user_id)))
            .select_from(Engagement)
            .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
            .where(Engagement.organization_id == organization_id)
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_participants_by_organization_id(
        self,
        db: AsyncSession,
        *,
        organization_id: int,
        page: int,
        limit: int,
    ) -> list[tuple]:
        """Fetch distinct users enrolled across all engagements for an organization."""
        from sqlalchemy import select, distinct
        from modules.engagements.models import Engagement, EngagementParticipant
        from modules.users.models import User

        offset = (page - 1) * limit

        query = (
            select(
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
                User.address,
                User.pin_code,
                User.city,
                User.state,
                User.country,
                User.status,
            )
            .distinct()
            .select_from(Engagement)
            .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(Engagement.organization_id == organization_id)
            .order_by(User.user_id.asc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.all())

    def _department_count_expr(self):
        return func.coalesce(func.jsonb_array_length(cast(Organization.departments, JSONB)), 0)

    def _camp_report_counts_subquery(self):
        return (
            select(
                CampReport.camp_no,
                func.count().label("report_count"),
            )
            .group_by(CampReport.camp_no)
            .subquery()
        )

    def _camps_grouped_query(self, *, search: str | None = None):
        report_counts = self._camp_report_counts_subquery()
        query = (
            select(
                Engagement.camp_no,
                Engagement.organization_id,
                Organization.name.label("organization_name"),
                func.min(Engagement.start_date).label("start_date"),
                func.count().label("engagement_count"),
                func.max(self._department_count_expr()).label("department_count"),
                func.max(func.coalesce(report_counts.c.report_count, 0)).label("report_count"),
            )
            .select_from(Engagement)
            .join(Organization, Organization.organization_id == Engagement.organization_id)
            .outerjoin(report_counts, report_counts.c.camp_no == Engagement.camp_no)
            .where(Engagement.camp_no.isnot(None))
            .group_by(Engagement.camp_no, Engagement.organization_id, Organization.name)
        )
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            conditions = [Organization.name.ilike(pattern)]
            stripped = search.strip()
            if stripped.isdigit():
                conditions.append(Engagement.camp_no == int(stripped))
            query = query.where(or_(*conditions))
        return query

    async def count_camps(self, db: AsyncSession, *, search: str | None = None) -> int:
        subq = self._camps_grouped_query(search=search).subquery()
        result = await db.execute(select(func.count()).select_from(subq))
        return int(result.scalar_one())

    async def list_camps(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[tuple]:
        query = self._camps_grouped_query(search=search)
        normalized_sort = (sort_by or "camp_no").strip().lower()
        descending = (sort_dir or "desc").strip().lower() == "desc"

        if normalized_sort == "engagement_count":
            order_col = func.count()
        elif normalized_sort == "camp_name":
            order_col = Organization.name
        elif normalized_sort == "department_count":
            order_col = func.max(self._department_count_expr())
        else:
            order_col = Engagement.camp_no

        query = query.order_by(order_col.desc() if descending else order_col.asc())
        offset = (page - 1) * limit
        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.all())
