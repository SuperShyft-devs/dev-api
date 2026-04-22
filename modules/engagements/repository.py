"""Engagements repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import Engagement, EngagementTimeSlot, OnboardingAssistantAssignment


class EngagementsRepository:
    """Engagement database queries."""

    async def get_engagement_by_code(self, db: AsyncSession, engagement_code: str) -> Engagement | None:
        result = await db.execute(select(Engagement).where(Engagement.engagement_code == engagement_code))
        return result.scalar_one_or_none()

    async def list_occupied_slots_by_engagement_id(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[tuple]:
        """Return occupied slots for one engagement.

        Each row is (engagement_date, slot_start_time).
        """

        query = (
            select(EngagementTimeSlot.engagement_date, EngagementTimeSlot.slot_start_time)
            .where(EngagementTimeSlot.engagement_id == engagement_id)
            .order_by(EngagementTimeSlot.engagement_date.asc(), EngagementTimeSlot.slot_start_time.asc())
        )
        result = await db.execute(query)
        return list(result.all())

    async def list_occupied_slots_for_active_b2c_engagements(self, db: AsyncSession) -> list[tuple]:
        """Return occupied slots for all active B2C engagements.

        B2C engagements are engagements that do not belong to an organization.
        Each row is (engagement_date, slot_start_time).
        """

        query = (
            select(EngagementTimeSlot.engagement_date, EngagementTimeSlot.slot_start_time)
            .join(Engagement, Engagement.engagement_id == EngagementTimeSlot.engagement_id)
            .where(Engagement.status == "active")
            .where(Engagement.organization_id.is_(None))
            .order_by(EngagementTimeSlot.engagement_date.asc(), EngagementTimeSlot.slot_start_time.asc())
        )
        result = await db.execute(query)
        return list(result.all())

    async def get_engagement_by_id(self, db: AsyncSession, engagement_id: int) -> Engagement | None:
        result = await db.execute(select(Engagement).where(Engagement.engagement_id == engagement_id))
        return result.scalar_one_or_none()

    async def count_engagements(
        self,
        db: AsyncSession,
        *,
        organization_id: int | None = None,
        status: str | None = None,
        city: str | None = None,
        on_date=None,
    ) -> int:
        from sqlalchemy import func

        query = select(func.count()).select_from(Engagement)

        if organization_id is not None:
            query = query.where(Engagement.organization_id == organization_id)
        if status is not None:
            query = query.where(Engagement.status == status)
        if city is not None:
            query = query.where(Engagement.city == city)
        if on_date is not None:
            query = query.where(Engagement.start_date <= on_date).where(Engagement.end_date >= on_date)

        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_engagements(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        organization_id: int | None = None,
        status: str | None = None,
        city: str | None = None,
        on_date=None,
    ) -> list[Engagement]:
        offset = (page - 1) * limit
        query = select(Engagement)

        if organization_id is not None:
            query = query.where(Engagement.organization_id == organization_id)
        if status is not None:
            query = query.where(Engagement.status == status)
        if city is not None:
            query = query.where(Engagement.city == city)
        if on_date is not None:
            query = query.where(Engagement.start_date <= on_date).where(Engagement.end_date >= on_date)

        query = query.order_by(Engagement.engagement_id.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def create_engagement(self, db: AsyncSession, engagement: Engagement) -> Engagement:
        db.add(engagement)
        await db.flush()
        return engagement

    async def update_engagement(self, db: AsyncSession, engagement: Engagement) -> Engagement:
        db.add(engagement)
        await db.flush()
        return engagement

    async def create_time_slot(self, db: AsyncSession, slot: EngagementTimeSlot) -> EngagementTimeSlot:
        db.add(slot)
        await db.flush()
        return slot

    async def has_slot_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> bool:
        result = await db.execute(
            select(EngagementTimeSlot.time_slot_id)
            .where(EngagementTimeSlot.user_id == user_id)
            .where(EngagementTimeSlot.engagement_id == engagement_id)
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def list_distinct_participant_ids_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[int]:
        """Return distinct user_ids of participants enrolled in an engagement.

        Pulls from ``engagement_time_slots`` — same source of truth used by
        other participant lookups in this repo.
        """

        result = await db.execute(
            select(EngagementTimeSlot.user_id)
            .distinct()
            .where(EngagementTimeSlot.engagement_id == engagement_id)
            .order_by(EngagementTimeSlot.user_id.asc())
        )
        return [int(v) for v in result.scalars().all()]

    async def add_onboarding_assistant(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        employee_id: int,
    ) -> OnboardingAssistantAssignment:
        """Assign an onboarding assistant to an engagement.
        
        The unique constraint prevents duplicate assignments.
        """
        assignment = OnboardingAssistantAssignment(
            engagement_id=engagement_id,
            employee_id=employee_id,
        )
        db.add(assignment)
        await db.flush()
        return assignment

    async def remove_onboarding_assistant(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        employee_id: int,
    ) -> bool:
        """Remove an onboarding assistant from an engagement.
        
        Returns True if an assignment was deleted, False if not found.
        """
        query = select(OnboardingAssistantAssignment).where(
            OnboardingAssistantAssignment.engagement_id == engagement_id,
            OnboardingAssistantAssignment.employee_id == employee_id,
        )
        result = await db.execute(query)
        assignment = result.scalar_one_or_none()
        
        if assignment is None:
            return False
        
        await db.delete(assignment)
        await db.flush()
        return True

    async def list_onboarding_assistants(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[OnboardingAssistantAssignment]:
        """Get all onboarding assistants assigned to an engagement."""
        query = select(OnboardingAssistantAssignment).where(
            OnboardingAssistantAssignment.engagement_id == engagement_id
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_onboarding_assistant_assignment(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        employee_id: int,
    ) -> OnboardingAssistantAssignment | None:
        """Check if a specific employee is assigned to an engagement."""
        query = select(OnboardingAssistantAssignment).where(
            OnboardingAssistantAssignment.engagement_id == engagement_id,
            OnboardingAssistantAssignment.employee_id == employee_id,
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def list_onboarding_assistant_assignments(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[OnboardingAssistantAssignment]:
        """Get all onboarding assistant assignments for an engagement."""
        query = (
            select(OnboardingAssistantAssignment)
            .where(OnboardingAssistantAssignment.engagement_id == engagement_id)
            .order_by(OnboardingAssistantAssignment.onboarding_assistant_id.asc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def create_onboarding_assistant_assignment(
        self,
        db: AsyncSession,
        assignment: OnboardingAssistantAssignment,
    ) -> OnboardingAssistantAssignment:
        """Create a new onboarding assistant assignment."""
        db.add(assignment)
        await db.flush()
        return assignment

    async def delete_onboarding_assistant_assignment(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        employee_id: int,
    ) -> int:
        """Delete an onboarding assistant assignment.
        
        Returns the number of rows deleted (0 or 1).
        """
        from sqlalchemy import delete as sql_delete

        result = await db.execute(
            sql_delete(OnboardingAssistantAssignment)
            .where(OnboardingAssistantAssignment.engagement_id == engagement_id)
            .where(OnboardingAssistantAssignment.employee_id == employee_id)
        )
        return int(result.rowcount or 0)

    async def count_participants_by_engagement_code(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
    ) -> int:
        """Count distinct users enrolled in a specific engagement by code."""
        from sqlalchemy import func

        query = (
            select(func.count(func.distinct(EngagementTimeSlot.user_id)))
            .select_from(Engagement)
            .join(EngagementTimeSlot, EngagementTimeSlot.engagement_id == Engagement.engagement_id)
            .where(Engagement.engagement_code == engagement_code)
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_participants_by_engagement_code(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
        page: int,
        limit: int,
    ) -> list[tuple]:
        """Fetch distinct users enrolled in a specific engagement by code."""
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
            .join(EngagementTimeSlot, EngagementTimeSlot.engagement_id == Engagement.engagement_id)
            .join(User, User.user_id == EngagementTimeSlot.user_id)
            .where(Engagement.engagement_code == engagement_code)
            .order_by(User.user_id.asc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.all())

    async def count_participants_for_b2c_engagements(
        self,
        db: AsyncSession,
    ) -> int:
        """Count distinct users enrolled in all B2C engagements.
        
        B2C engagements are engagements with no organization_id.
        """
        from sqlalchemy import func

        query = (
            select(func.count(func.distinct(EngagementTimeSlot.user_id)))
            .select_from(Engagement)
            .join(EngagementTimeSlot, EngagementTimeSlot.engagement_id == Engagement.engagement_id)
            .where(Engagement.organization_id.is_(None))
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_participants_for_b2c_engagements(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
    ) -> list[tuple]:
        """Fetch distinct users enrolled in all B2C engagements.
        
        B2C engagements are engagements with no organization_id.
        """
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
            .join(EngagementTimeSlot, EngagementTimeSlot.engagement_id == Engagement.engagement_id)
            .join(User, User.user_id == EngagementTimeSlot.user_id)
            .where(Engagement.organization_id.is_(None))
            .order_by(User.user_id.asc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.all())
