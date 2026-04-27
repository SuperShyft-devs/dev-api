"""Organizations repository.

Only database queries live here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.organizations.models import Organization


class OrganizationsRepository:
    """Organization database queries."""

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
    ) -> int:
        query = select(func.count()).select_from(Organization)

        if status is not None:
            query = query.where(Organization.status == status)
        if organization_type is not None:
            query = query.where(Organization.organization_type == organization_type)
        if bd_employee_id is not None:
            query = query.where(Organization.bd_employee_id == bd_employee_id)

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
    ) -> list[Organization]:
        offset = (page - 1) * limit
        query = select(Organization)

        if status is not None:
            query = query.where(Organization.status == status)
        if organization_type is not None:
            query = query.where(Organization.organization_type == organization_type)
        if bd_employee_id is not None:
            query = query.where(Organization.bd_employee_id == bd_employee_id)

        query = query.order_by(Organization.organization_id.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

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
                User.city,
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
