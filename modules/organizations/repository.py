"""Organizations repository.

Only database queries live here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import case, cast, func, or_, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.engagements.models import Engagement
from modules.organizations.models import Industry, Organization
from modules.reports.models import CampReport


class OrganizationsRepository:
    """Organization database queries."""

    _CONTACT_PERSON_JSON_MEMBER_SQL = """
        (
            organizations.contact_person_user_ids IS NOT NULL
            AND (
                organizations.contact_person_user_ids::jsonb->'organization_managers' @> :uid_json::jsonb
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_each(organizations.contact_person_user_ids::jsonb) AS city_entry(city_key, city_val)
                    WHERE city_key <> 'organization_managers'
                      AND jsonb_typeof(city_val) = 'object'
                      AND (
                          city_val->'managers' @> :uid_json::jsonb
                          OR EXISTS (
                              SELECT 1
                              FROM jsonb_each(city_val) AS dept_entry(dept_key, dept_val)
                              WHERE dept_key <> 'managers'
                                AND jsonb_typeof(dept_val) = 'array'
                                AND dept_val @> :uid_json::jsonb
                          )
                      )
                )
            )
        )
    """

    @staticmethod
    def _contact_person_user_ids_contains_user(user_id: int):
        import json

        return text(OrganizationsRepository._CONTACT_PERSON_JSON_MEMBER_SQL).bindparams(
            uid_json=json.dumps([user_id])
        )

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
        contact_person_user_id: int | None = None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
        industry_key: str | None = None,
    ):
        if status is not None:
            query = query.where(Organization.status == status)
        if organization_type is not None:
            query = query.where(Organization.organization_type == organization_type)
        if bd_employee_id is not None:
            query = query.where(Organization.bd_employee_id == bd_employee_id)
        if contact_person_user_id is not None:
            query = query.where(self._contact_person_user_ids_contains_user(contact_person_user_id))
        if city is not None and city.strip():
            query = query.where(func.lower(func.trim(Organization.city)) == city.strip().lower())
        if country is not None and country.strip():
            query = query.where(func.lower(func.trim(Organization.country)) == country.strip().lower())
        if industry_key is not None and industry_key.strip():
            query = query.where(Organization.industry_key == industry_key.strip())
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
        contact_person_user_id: int | None = None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
        industry_key: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Organization)
        query = self._apply_org_list_filters(
            query,
            status=status,
            organization_type=organization_type,
            bd_employee_id=bd_employee_id,
            contact_person_user_id=contact_person_user_id,
            search=search,
            city=city,
            country=country,
            industry_key=industry_key,
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
        contact_person_user_id: int | None = None,
        search: str | None = None,
        city: str | None = None,
        country: str | None = None,
        industry_key: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[tuple]:
        """Returns list of (Organization, industry_display_name) tuples."""
        offset = (page - 1) * limit
        query = (
            select(Organization, Industry.industry)
            .outerjoin(Industry, Industry.industry_key == Organization.industry_key)
        )
        query = self._apply_org_list_filters(
            query,
            status=status,
            organization_type=organization_type,
            bd_employee_id=bd_employee_id,
            contact_person_user_id=contact_person_user_id,
            search=search,
            city=city,
            country=country,
            industry_key=industry_key,
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
        return list(result.all())

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

    async def list_all_industries(self, db: AsyncSession) -> list[Industry]:
        """Return all industries ordered by display name."""
        result = await db.execute(
            select(Industry).order_by(Industry.industry.asc())
        )
        return list(result.scalars().all())

    async def get_industry_by_key(self, db: AsyncSession, industry_key: str) -> Industry | None:
        result = await db.execute(
            select(Industry).where(Industry.industry_key == industry_key)
        )
        return result.scalar_one_or_none()

    async def get_industry_by_id(self, db: AsyncSession, industry_id: int) -> Industry | None:
        result = await db.execute(
            select(Industry).where(Industry.id == industry_id)
        )
        return result.scalar_one_or_none()

    async def create_industry(self, db: AsyncSession, industry: Industry) -> Industry:
        db.add(industry)
        await db.flush()
        return industry

    async def delete_industry(self, db: AsyncSession, industry: Industry) -> None:
        await db.delete(industry)
        await db.flush()

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
        departments_jsonb = cast(Organization.departments, JSONB)
        return func.coalesce(
            case(
                (func.jsonb_typeof(departments_jsonb) == "array", func.jsonb_array_length(departments_jsonb)),
                else_=0
            ),
            0
        )

    def _camp_report_camp_nos_subquery(self):
        return (
            select(CampReport.camp_no)
            .where(CampReport.camp_no.isnot(None))
            .distinct()
            .subquery()
        )

    def _camps_grouped_query(
        self,
        *,
        search: str | None = None,
        organization_id: int | None = None,
        initialized_only: bool = True,
    ):
        query = (
            select(
                Engagement.camp_no,
                Engagement.organization_id,
                Organization.name.label("organization_name"),
                Organization.logo.label("organization_logo"),
                func.min(Engagement.start_date).label("start_date"),
                func.count(Engagement.engagement_id).label("engagement_count"),
            )
            .select_from(Engagement)
            .join(Organization, Organization.organization_id == Engagement.organization_id)
            .where(Engagement.camp_no.isnot(None))
            .group_by(
                Engagement.camp_no,
                Engagement.organization_id,
                Organization.name,
                Organization.logo,
            )
        )
        if initialized_only:
            reported = self._camp_report_camp_nos_subquery()
            query = query.join(reported, reported.c.camp_no == Engagement.camp_no)
        if organization_id is not None:
            query = query.where(Engagement.organization_id == organization_id)
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            conditions = [Organization.name.ilike(pattern)]
            stripped = search.strip()
            if stripped.isdigit():
                conditions.append(Engagement.camp_no == int(stripped))
            query = query.where(or_(*conditions))
        return query

    async def count_camps(
        self,
        db: AsyncSession,
        *,
        search: str | None = None,
        organization_id: int | None = None,
        initialized_only: bool = True,
    ) -> int:
        subq = self._camps_grouped_query(
            search=search,
            organization_id=organization_id,
            initialized_only=initialized_only,
        ).subquery()
        result = await db.execute(select(func.count()).select_from(subq))
        return int(result.scalar_one())

    async def list_camps(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        search: str | None = None,
        organization_id: int | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
        initialized_only: bool = True,
    ) -> list[tuple]:
        query = self._camps_grouped_query(
            search=search,
            organization_id=organization_id,
            initialized_only=initialized_only,
        )
        normalized_sort = (sort_by or "camp_no").strip().lower()
        descending = (sort_dir or "desc").strip().lower() == "desc"

        if normalized_sort in {"engagement_count", "engagement_ids"}:
            order_col = func.count(Engagement.engagement_id)
        elif normalized_sort == "camp_name":
            order_col = Organization.name
        elif normalized_sort in {"department_count", "departments"}:
            # Approximate: org department config size (reported depts enriched in service).
            order_col = func.max(self._department_count_expr())
        elif normalized_sort == "year":
            order_col = func.min(Engagement.start_date)
        else:
            order_col = Engagement.camp_no

        query = query.order_by(order_col.desc() if descending else order_col.asc())
        offset = (page - 1) * limit
        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.all())

    async def list_engagement_ids_by_camp_nos(
        self,
        db: AsyncSession,
        *,
        camp_nos: list[int],
    ) -> dict[int, list[int]]:
        if not camp_nos:
            return {}
        result = await db.execute(
            select(Engagement.camp_no, Engagement.engagement_id)
            .where(Engagement.camp_no.in_(camp_nos))
            .order_by(Engagement.camp_no.asc(), Engagement.engagement_id.asc())
        )
        by_camp: dict[int, list[int]] = {int(c): [] for c in camp_nos}
        for camp_no, engagement_id in result.all():
            if camp_no is None or engagement_id is None:
                continue
            by_camp.setdefault(int(camp_no), []).append(int(engagement_id))
        return by_camp

    async def list_reported_department_slugs_by_camp_nos(
        self,
        db: AsyncSession,
        *,
        camp_nos: list[int],
    ) -> dict[int, list[str]]:
        """Distinct non-null department slugs that have camp_reports rows for each camp."""
        if not camp_nos:
            return {}
        result = await db.execute(
            select(CampReport.camp_no, CampReport.department)
            .where(
                CampReport.camp_no.in_(camp_nos),
                CampReport.department.isnot(None),
                func.trim(CampReport.department) != "",
            )
            .distinct()
            .order_by(CampReport.camp_no.asc(), CampReport.department.asc())
        )
        by_camp: dict[int, list[str]] = {int(c): [] for c in camp_nos}
        seen: dict[int, set[str]] = {int(c): set() for c in camp_nos}
        for camp_no, department in result.all():
            if camp_no is None or department is None:
                continue
            cid = int(camp_no)
            slug = str(department).strip()
            if not slug:
                continue
            key = slug.lower()
            if key in seen.setdefault(cid, set()):
                continue
            seen[cid].add(key)
            by_camp.setdefault(cid, []).append(slug)
        return by_camp

    async def list_reported_department_slugs_by_organization_ids(
        self,
        db: AsyncSession,
        *,
        organization_ids: list[int],
    ) -> dict[int, list[str]]:
        """Distinct non-null department slugs from camp_reports rows per organization."""
        if not organization_ids:
            return {}
        result = await db.execute(
            select(CampReport.organization_id, CampReport.department)
            .where(
                CampReport.organization_id.in_(organization_ids),
                CampReport.department.isnot(None),
                func.trim(CampReport.department) != "",
            )
            .distinct()
            .order_by(CampReport.organization_id.asc(), CampReport.department.asc())
        )
        by_org: dict[int, list[str]] = {int(oid): [] for oid in organization_ids}
        seen: dict[int, set[str]] = {int(oid): set() for oid in organization_ids}
        for organization_id, department in result.all():
            if organization_id is None or department is None:
                continue
            oid = int(organization_id)
            slug = str(department).strip()
            if not slug:
                continue
            key = slug.lower()
            if key in seen.setdefault(oid, set()):
                continue
            seen[oid].add(key)
            by_org.setdefault(oid, []).append(slug)
        return by_org

    async def list_distinct_cities_by_camp_nos(
        self,
        db: AsyncSession,
        *,
        camp_nos: list[int],
    ) -> dict[int, list[str]]:
        """Distinct non-empty engagement.city values per camp_no."""
        if not camp_nos:
            return {}
        result = await db.execute(
            select(Engagement.camp_no, Engagement.city)
            .where(
                Engagement.camp_no.in_(camp_nos),
                Engagement.city.isnot(None),
                func.trim(Engagement.city) != "",
            )
            .distinct()
            .order_by(Engagement.camp_no.asc(), Engagement.city.asc())
        )
        by_camp: dict[int, list[str]] = {int(c): [] for c in camp_nos}
        seen: dict[int, set[str]] = {int(c): set() for c in camp_nos}
        for camp_no, city in result.all():
            if camp_no is None or city is None:
                continue
            cid = int(camp_no)
            trimmed = str(city).strip()
            if not trimmed:
                continue
            key = trimmed.casefold()
            if key in seen.setdefault(cid, set()):
                continue
            seen[cid].add(key)
            by_camp.setdefault(cid, []).append(trimmed)
        return by_camp

    async def list_organization_departments_by_ids(
        self,
        db: AsyncSession,
        *,
        organization_ids: list[int],
    ) -> dict[int, list[dict[str, str]]]:
        if not organization_ids:
            return {}
        result = await db.execute(
            select(Organization.organization_id, Organization.departments).where(
                Organization.organization_id.in_(organization_ids)
            )
        )
        out: dict[int, list[dict[str, str]]] = {}
        for organization_id, departments in result.all():
            items: list[dict[str, str]] = []
            if isinstance(departments, list):
                for item in departments:
                    if not isinstance(item, dict):
                        continue
                    slug = str(item.get("slug") or "").strip()
                    name = str(item.get("department") or item.get("name") or "").strip()
                    if not slug:
                        continue
                    items.append({"slug": slug, "name": name or slug})
            out[int(organization_id)] = items
        return out

    async def list_distinct_engagement_cities_by_org_ids(
        self,
        db: AsyncSession,
        *,
        organization_ids: list[int],
    ) -> dict[int, list[str]]:
        if not organization_ids:
            return {}
        result = await db.execute(
            select(Engagement.organization_id, Engagement.city)
            .where(
                Engagement.organization_id.in_(organization_ids),
                Engagement.city.isnot(None),
                func.trim(Engagement.city) != "",
            )
            .distinct()
            .order_by(Engagement.organization_id.asc(), Engagement.city.asc())
        )
        cities_by_org: dict[int, list[str]] = {int(oid): [] for oid in organization_ids}
        seen: dict[int, set[str]] = {int(oid): set() for oid in organization_ids}
        for org_id, city in result.all():
            if org_id is None or city is None:
                continue
            oid = int(org_id)
            trimmed = str(city).strip()
            if not trimmed:
                continue
            key = trimmed.lower()
            if key in seen.get(oid, set()):
                continue
            seen.setdefault(oid, set()).add(key)
            cities_by_org.setdefault(oid, []).append(trimmed)
        return cities_by_org

    async def count_engagements_by_camp_no(self, db: AsyncSession, *, camp_no: int) -> int:
        result = await db.execute(
            select(func.count()).select_from(Engagement).where(Engagement.camp_no == camp_no)
        )
        return int(result.scalar_one() or 0)

    async def remap_engagement_camp_no(
        self,
        db: AsyncSession,
        *,
        from_camp_no: int,
        to_camp_no: int,
    ) -> int:
        result = await db.execute(
            select(Engagement).where(Engagement.camp_no == from_camp_no)
        )
        rows = list(result.scalars().all())
        for row in rows:
            row.camp_no = to_camp_no
        if rows:
            await db.flush()
        return len(rows)
