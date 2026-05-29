"""Engagements repository.

Only database queries live here.
"""

from __future__ import annotations

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.engagements.models import Engagement, EngagementParticipant, OnboardingAssistantAssignment


class EngagementsRepository:
    """Engagement database queries."""

    _ENGAGEMENT_SORT_COLUMNS = {
        "engagement_id": Engagement.engagement_id,
        "engagement_name": Engagement.engagement_name,
        "engagement_code": Engagement.engagement_code,
        "engagement_type": Engagement.engagement_type,
        "city": Engagement.city,
        "status": Engagement.status,
        "start_date": Engagement.start_date,
        "end_date": Engagement.end_date,
    }

    def _apply_engagement_list_filters(
        self,
        query,
        *,
        organization_id: int | None = None,
        status: str | None = None,
        city: str | None = None,
        on_date=None,
        search: str | None = None,
        engagement_type: str | None = None,
    ):
        if organization_id is not None:
            query = query.where(Engagement.organization_id == organization_id)
        if status is not None:
            query = query.where(Engagement.status == status)
        if city is not None and city.strip():
            query = query.where(func.lower(func.trim(Engagement.city)) == city.strip().lower())
        if on_date is not None:
            query = query.where(Engagement.start_date <= on_date).where(Engagement.end_date >= on_date)
        if engagement_type is not None and engagement_type.strip():
            type_text = func.lower(func.trim(cast(Engagement.engagement_type, String)))
            query = query.where(type_text == engagement_type.strip().lower())
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    Engagement.engagement_name.ilike(pattern),
                    Engagement.engagement_code.ilike(pattern),
                    Engagement.city.ilike(pattern),
                )
            )
        return query

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
            select(EngagementParticipant.engagement_date, EngagementParticipant.slot_start_time)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .order_by(EngagementParticipant.engagement_date.asc(), EngagementParticipant.slot_start_time.asc())
        )
        result = await db.execute(query)
        return list(result.all())

    async def list_occupied_slots_for_active_b2c_engagements(self, db: AsyncSession) -> list[tuple]:
        """Return occupied slots for all running B2C engagements.

        B2C engagements are engagements that do not belong to an organization.
        Each row is (engagement_date, slot_start_time).
        """

        query = (
            select(EngagementParticipant.engagement_date, EngagementParticipant.slot_start_time)
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .where(Engagement.status == "running")
            .where(Engagement.organization_id.is_(None))
            .order_by(EngagementParticipant.engagement_date.asc(), EngagementParticipant.slot_start_time.asc())
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
        search: str | None = None,
        engagement_type: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Engagement)
        query = self._apply_engagement_list_filters(
            query,
            organization_id=organization_id,
            status=status,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
        )

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
        search: str | None = None,
        engagement_type: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[Engagement]:
        offset = (page - 1) * limit
        query = select(Engagement)
        query = self._apply_engagement_list_filters(
            query,
            organization_id=organization_id,
            status=status,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
        )
        query = apply_sort(
            query,
            sort_by=sort_by,
            sort_dir=sort_dir,
            columns=self._ENGAGEMENT_SORT_COLUMNS,
            default_column=Engagement.engagement_id,
        )
        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_distinct_engagement_types_and_cities(self, db: AsyncSession) -> tuple[list[str], list[str]]:
        engagement_type_text = cast(Engagement.engagement_type, String)
        type_result = await db.execute(
            select(func.distinct(func.trim(engagement_type_text)))
            .where(Engagement.engagement_type.isnot(None))
            .where(func.trim(engagement_type_text) != "")
            .order_by(func.trim(engagement_type_text).asc())
        )
        city_result = await db.execute(
            select(func.distinct(func.trim(Engagement.city)))
            .where(Engagement.city.isnot(None))
            .where(func.trim(Engagement.city) != "")
            .order_by(func.trim(Engagement.city).asc())
        )
        types = [str(v) for v in type_result.scalars().all() if v]
        cities = [str(v) for v in city_result.scalars().all() if v]
        return types, cities

    async def create_engagement(self, db: AsyncSession, engagement: Engagement) -> Engagement:
        db.add(engagement)
        await db.flush()
        return engagement

    async def update_engagement(self, db: AsyncSession, engagement: Engagement) -> Engagement:
        db.add(engagement)
        await db.flush()
        return engagement

    async def create_participant(self, db: AsyncSession, slot: EngagementParticipant) -> EngagementParticipant:
        db.add(slot)
        await db.flush()
        return slot

    async def update_participant(self, db: AsyncSession, participant: EngagementParticipant) -> EngagementParticipant:
        db.add(participant)
        await db.flush()
        return participant

    async def has_participant_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> bool:
        result = await db.execute(
            select(EngagementParticipant.engagement_participant_id)
            .where(EngagementParticipant.user_id == user_id)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def get_participant_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> EngagementParticipant | None:
        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.user_id == user_id)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_enrolled_user_ids_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        user_ids: list[int],
    ) -> set[int]:
        ids = list({int(uid) for uid in user_ids if uid})
        if not ids:
            return set()
        result = await db.execute(
            select(EngagementParticipant.user_id)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id.in_(ids))
        )
        return {int(row) for row in result.scalars().all()}

    async def get_participants_map_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        user_ids: list[int],
    ) -> dict[int, EngagementParticipant]:
        ids = list({int(uid) for uid in user_ids if uid})
        if not ids:
            return {}
        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.user_id.in_(ids))
            .order_by(EngagementParticipant.engagement_participant_id.desc())
        )
        out: dict[int, EngagementParticipant] = {}
        for row in result.scalars().all():
            uid = int(row.user_id)
            if uid not in out:
                out[uid] = row
        return out

    async def delete_participants_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> int:
        from sqlalchemy import delete as sql_delete

        result = await db.execute(
            sql_delete(EngagementParticipant)
            .where(EngagementParticipant.user_id == user_id)
            .where(EngagementParticipant.engagement_id == engagement_id)
        )
        return int(result.rowcount or 0)

    async def count_distinct_participants_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> int:
        result = await db.execute(
            select(func.count(func.distinct(EngagementParticipant.user_id))).where(
                EngagementParticipant.engagement_id == engagement_id
            )
        )
        return int(result.scalar_one() or 0)

    async def count_distinct_participants_by_engagement_ids(
        self,
        db: AsyncSession,
        *,
        engagement_ids: list[int],
    ) -> dict[int, int]:
        if not engagement_ids:
            return {}

        result = await db.execute(
            select(
                EngagementParticipant.engagement_id,
                func.count(func.distinct(EngagementParticipant.user_id)),
            )
            .where(EngagementParticipant.engagement_id.in_(engagement_ids))
            .group_by(EngagementParticipant.engagement_id)
        )
        counts = {int(row[0]): int(row[1]) for row in result.all()}
        return {eid: counts.get(eid, 0) for eid in engagement_ids}

    async def list_distinct_participant_ids_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[int]:
        """Return distinct user_ids of participants enrolled in an engagement.

        Pulls from ``engagement_participants`` — same source of truth used by
        other participant lookups in this repo.
        """

        result = await db.execute(
            select(EngagementParticipant.user_id)
            .distinct()
            .where(EngagementParticipant.engagement_id == engagement_id)
            .order_by(EngagementParticipant.user_id.asc())
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

    async def list_onboarding_assistant_user_ids(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[int]:
        """Return user_ids for all onboarding assistants assigned to an engagement."""
        from modules.employee.models import Employee

        query = (
            select(Employee.user_id)
            .join(
                OnboardingAssistantAssignment,
                OnboardingAssistantAssignment.employee_id == Employee.employee_id,
            )
            .where(OnboardingAssistantAssignment.engagement_id == engagement_id)
        )
        result = await db.execute(query)
        return [int(uid) for uid in result.scalars().all()]

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
        engagement_id: int,
    ) -> int:
        """Count distinct users enrolled for a specific engagement."""

        _ = engagement_code
        return await self.count_distinct_participants_for_engagement(db, engagement_id=engagement_id)

    async def list_participants_by_engagement_id(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        page: int,
        limit: int,
    ) -> list[tuple]:
        """Fetch participant enrollment rows for a specific engagement."""
        from modules.users.models import User

        offset = (page - 1) * limit

        ranked_rows = (
            select(
                EngagementParticipant.engagement_participant_id,
                EngagementParticipant.engagement_id,
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
                EngagementParticipant.slot_start_time,
                EngagementParticipant.engagement_date,
                EngagementParticipant.participants_employee_id,
                EngagementParticipant.participant_department,
                EngagementParticipant.participant_blood_group,
                EngagementParticipant.want_doctor_consultation,
                EngagementParticipant.want_nutritionist_consultation,
                EngagementParticipant.want_doctor_and_nutritionist_consultation,
                EngagementParticipant.is_profile_created_on_metsights,
                EngagementParticipant.is_primary_record_id_synced,
                EngagementParticipant.is_fitprint_record_id_synced,
                func.row_number()
                .over(
                    partition_by=EngagementParticipant.user_id,
                    order_by=EngagementParticipant.engagement_participant_id.desc(),
                )
                .label("rn"),
            )
            .select_from(EngagementParticipant)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(EngagementParticipant.engagement_id == engagement_id)
        ).subquery()

        query = (
            select(
                ranked_rows.c.engagement_participant_id,
                ranked_rows.c.engagement_id,
                ranked_rows.c.user_id,
                ranked_rows.c.first_name,
                ranked_rows.c.last_name,
                ranked_rows.c.phone,
                ranked_rows.c.email,
                ranked_rows.c.address,
                ranked_rows.c.pin_code,
                ranked_rows.c.city,
                ranked_rows.c.state,
                ranked_rows.c.country,
                ranked_rows.c.status,
                ranked_rows.c.slot_start_time,
                ranked_rows.c.engagement_date,
                ranked_rows.c.participants_employee_id,
                ranked_rows.c.participant_department,
                ranked_rows.c.participant_blood_group,
                ranked_rows.c.want_doctor_consultation,
                ranked_rows.c.want_nutritionist_consultation,
                ranked_rows.c.want_doctor_and_nutritionist_consultation,
                ranked_rows.c.is_profile_created_on_metsights,
                ranked_rows.c.is_primary_record_id_synced,
                ranked_rows.c.is_fitprint_record_id_synced,
            )
            .where(ranked_rows.c.rn == 1)
            .order_by(ranked_rows.c.engagement_participant_id.asc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.all())

    async def list_participants_by_engagement_code(
        self,
        db: AsyncSession,
        *,
        engagement_code: str,
        engagement_id: int,
        page: int,
        limit: int,
    ) -> list[tuple]:
        """Fetch participant enrollment rows for a specific engagement by code."""
        _ = engagement_code
        return await self.list_participants_by_engagement_id(
            db,
            engagement_id=engagement_id,
            page=page,
            limit=limit,
        )

    async def count_participants_for_b2c_engagements(
        self,
        db: AsyncSession,
    ) -> int:
        """Count distinct users enrolled in all B2C engagements.
        
        B2C engagements are engagements with no organization_id.
        """
        query = (
            select(func.count(func.distinct(EngagementParticipant.user_id)))
            .select_from(Engagement)
            .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
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
        """Fetch participant enrollment rows in all B2C engagements.
        
        B2C engagements are engagements with no organization_id.
        """
        from modules.users.models import User

        offset = (page - 1) * limit

        ranked_rows = (
            select(
                EngagementParticipant.engagement_participant_id,
                EngagementParticipant.engagement_id,
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
                EngagementParticipant.slot_start_time,
                EngagementParticipant.engagement_date,
                EngagementParticipant.participants_employee_id,
                EngagementParticipant.participant_department,
                EngagementParticipant.participant_blood_group,
                EngagementParticipant.want_doctor_consultation,
                EngagementParticipant.want_nutritionist_consultation,
                EngagementParticipant.want_doctor_and_nutritionist_consultation,
                EngagementParticipant.is_profile_created_on_metsights,
                EngagementParticipant.is_primary_record_id_synced,
                EngagementParticipant.is_fitprint_record_id_synced,
                func.row_number()
                .over(
                    partition_by=EngagementParticipant.user_id,
                    order_by=EngagementParticipant.engagement_participant_id.desc(),
                )
                .label("rn"),
            )
            .select_from(Engagement)
            .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(Engagement.organization_id.is_(None))
        ).subquery()

        query = (
            select(
                ranked_rows.c.engagement_participant_id,
                ranked_rows.c.engagement_id,
                ranked_rows.c.user_id,
                ranked_rows.c.first_name,
                ranked_rows.c.last_name,
                ranked_rows.c.phone,
                ranked_rows.c.email,
                ranked_rows.c.address,
                ranked_rows.c.pin_code,
                ranked_rows.c.city,
                ranked_rows.c.state,
                ranked_rows.c.country,
                ranked_rows.c.status,
                ranked_rows.c.slot_start_time,
                ranked_rows.c.engagement_date,
                ranked_rows.c.participants_employee_id,
                ranked_rows.c.participant_department,
                ranked_rows.c.participant_blood_group,
                ranked_rows.c.want_doctor_consultation,
                ranked_rows.c.want_nutritionist_consultation,
                ranked_rows.c.want_doctor_and_nutritionist_consultation,
                ranked_rows.c.is_profile_created_on_metsights,
                ranked_rows.c.is_primary_record_id_synced,
                ranked_rows.c.is_fitprint_record_id_synced,
            )
            .where(ranked_rows.c.rn == 1)
            .order_by(ranked_rows.c.engagement_participant_id.asc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.all())

    async def delete_all_participants_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> int:
        from sqlalchemy import delete as sql_delete

        result = await db.execute(
            sql_delete(EngagementParticipant).where(EngagementParticipant.engagement_id == engagement_id)
        )
        return int(result.rowcount or 0)

    async def delete_all_onboarding_assignments_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> int:
        from sqlalchemy import delete as sql_delete

        result = await db.execute(
            sql_delete(OnboardingAssistantAssignment).where(
                OnboardingAssistantAssignment.engagement_id == engagement_id
            )
        )
        return int(result.rowcount or 0)

    async def delete_engagement_by_id(self, db: AsyncSession, *, engagement_id: int) -> bool:
        from sqlalchemy import delete as sql_delete

        result = await db.execute(sql_delete(Engagement).where(Engagement.engagement_id == engagement_id))
        return int(result.rowcount or 0) > 0
