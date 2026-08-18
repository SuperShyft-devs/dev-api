"""Engagements repository.

Only database queries live here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time

from sqlalchemy import String, and_, cast, func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.engagements.models import (
    AutoNotificationEvent,
    Engagement,
    EngagementNotification,
    EngagementParticipant,
    EngagementType,
    OnboardingAssistantAssignment,
)
from modules.organizations.models import Organization
from modules.reports.models import IndividualHealthReport


@dataclass(frozen=True)
class ConsultationRemainderParticipant:
    user_id: int
    engagement_id: int
    service_keys: list[str]
    want: bool
    consultation_date: date
    consultation_slot: str | None
    expert_type: str


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
        camp_no: int | None = None,
        statuses: list[str] | None = None,
        city: str | None = None,
        on_date=None,
        search: str | None = None,
        engagement_type: str | None = None,
        audience: str | None = None,
    ):
        if organization_id is not None:
            query = query.where(Engagement.organization_id == organization_id)
        if audience == "b2b":
            query = query.where(Engagement.organization_id.isnot(None))
        elif audience == "b2c":
            query = query.where(Engagement.organization_id.is_(None))
        if camp_no is not None:
            query = query.where(Engagement.camp_no == camp_no)
        if statuses:
            query = query.where(Engagement.status.in_(statuses))
        if city is not None and city.strip():
            query = query.where(func.lower(func.trim(Engagement.city)) == city.strip().lower())
        if on_date is not None:
            query = query.where(Engagement.start_date <= on_date).where(Engagement.end_date >= on_date)
        if engagement_type is not None and engagement_type.strip():
            et_val = engagement_type.strip()
            if et_val.isdigit():
                query = query.where(Engagement.engagement_type == int(et_val))
            else:
                query = query.where(
                    Engagement.engagement_type.in_(
                        select(EngagementType.id).where(
                            func.lower(EngagementType.code) == et_val.lower()
                        )
                    )
                )
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

    async def get_engagement_with_org_by_code(self, db: AsyncSession, engagement_code: str):
        query = (
            select(
                Engagement,
                Organization.name.label("organization_name"),
                Organization.logo.label("organization_logo_url"),
                EngagementType.code.label("engagement_type_code"),
            )
            .outerjoin(Organization, Engagement.organization_id == Organization.organization_id)
            .outerjoin(EngagementType, Engagement.engagement_type == EngagementType.id)
            .where(Engagement.engagement_code == engagement_code)
        )
        result = await db.execute(query)
        return result.one_or_none()

    async def count_engagements(
        self,
        db: AsyncSession,
        *,
        organization_id: int | None = None,
        camp_no: int | None = None,
        statuses: list[str] | None = None,
        city: str | None = None,
        on_date=None,
        search: str | None = None,
        engagement_type: str | None = None,
        audience: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Engagement)
        query = self._apply_engagement_list_filters(
            query,
            organization_id=organization_id,
            camp_no=camp_no,
            statuses=statuses,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
            audience=audience,
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def count_engagements_with_null_organization(self, db: AsyncSession) -> int:
        query = (
            select(func.count())
            .select_from(Engagement)
            .where(Engagement.organization_id.is_(None))
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
        camp_no: int | None = None,
        statuses: list[str] | None = None,
        city: str | None = None,
        on_date=None,
        search: str | None = None,
        engagement_type: str | None = None,
        audience: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[Engagement]:
        offset = (page - 1) * limit
        query = select(Engagement)
        query = self._apply_engagement_list_filters(
            query,
            organization_id=organization_id,
            camp_no=camp_no,
            statuses=statuses,
            city=city,
            on_date=on_date,
            search=search,
            engagement_type=engagement_type,
            audience=audience,
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
        type_result = await db.execute(
            select(func.distinct(EngagementType.code))
            .select_from(Engagement)
            .join(EngagementType, EngagementType.id == Engagement.engagement_type)
            .where(Engagement.engagement_type.isnot(None))
            .order_by(EngagementType.code.asc())
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

    async def count_cabin_slot_participants(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        blood_collection_cabin: str,
        engagement_date: date,
        slot_start_time: time,
    ) -> int:
        query = (
            select(func.count())
            .select_from(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.blood_collection_cabin == blood_collection_cabin)
            .where(EngagementParticipant.engagement_date == engagement_date)
            .where(EngagementParticipant.slot_start_time == slot_start_time)
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_cabin_slot_occupancy(self, db: AsyncSession, *, engagement_id: int) -> list[tuple]:
        query = (
            select(
                EngagementParticipant.blood_collection_cabin,
                EngagementParticipant.engagement_date,
                EngagementParticipant.slot_start_time,
                func.count(),
            )
            .where(EngagementParticipant.engagement_id == engagement_id)
            .where(EngagementParticipant.blood_collection_cabin.isnot(None))
            .where(EngagementParticipant.engagement_date.isnot(None))
            .where(EngagementParticipant.slot_start_time.isnot(None))
            .group_by(
                EngagementParticipant.blood_collection_cabin,
                EngagementParticipant.engagement_date,
                EngagementParticipant.slot_start_time,
            )
        )
        result = await db.execute(query)
        return list(result.all())

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

    async def get_participant_for_user_camp_no(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        camp_no: int,
        exclude_engagement_id: int | None = None,
    ) -> EngagementParticipant | None:
        query = (
            select(EngagementParticipant)
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .where(EngagementParticipant.user_id == user_id)
            .where(Engagement.camp_no == camp_no)
        )
        if exclude_engagement_id is not None:
            query = query.where(EngagementParticipant.engagement_id != exclude_engagement_id)
        result = await db.execute(
            query.order_by(EngagementParticipant.engagement_participant_id.desc()).limit(1)
        )
        return result.scalar_one_or_none()

    async def get_participant_by_booking_id(
        self,
        db: AsyncSession,
        *,
        booking_id: str,
    ) -> EngagementParticipant | None:
        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.booking_id == booking_id)
            .order_by(EngagementParticipant.engagement_participant_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def update_participant_healthians_booking(
        self,
        db: AsyncSession,
        *,
        engagement_participant_id: int,
        barcode: str,
        booking_id: str,
    ) -> None:
        result = await db.execute(
            select(EngagementParticipant).where(
                EngagementParticipant.engagement_participant_id == engagement_participant_id
            )
        )
        participant = result.scalar_one_or_none()
        if participant is None:
            return
        participant.barcode = barcode
        participant.booking_id = booking_id
        db.add(participant)
        await db.flush()

    async def clear_participant_healthians_booking(
        self,
        db: AsyncSession,
        *,
        engagement_participant_id: int,
    ) -> None:
        result = await db.execute(
            select(EngagementParticipant).where(
                EngagementParticipant.engagement_participant_id == engagement_participant_id
            )
        )
        participant = result.scalar_one_or_none()
        if participant is None:
            return
        participant.booking_id = None
        participant.barcode = None
        db.add(participant)
        await db.flush()

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
        participant_department_slugs: set[str] | None = None,
    ) -> int:
        query = select(func.count(func.distinct(EngagementParticipant.user_id))).where(
            EngagementParticipant.engagement_id == engagement_id
        )
        if participant_department_slugs is not None:
            if not participant_department_slugs:
                return 0
            query = query.where(
                EngagementParticipant.participant_department.in_(participant_department_slugs)
            )
        result = await db.execute(query)
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

    async def list_running_engagements(self, db: AsyncSession) -> list[Engagement]:
        """List all running engagements."""
        query = (
            select(Engagement)
            .where(Engagement.status == "running")
            .order_by(Engagement.start_date.desc(), Engagement.engagement_id.desc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_running_engagements_for_assigned_employee(
        self,
        db: AsyncSession,
        *,
        employee_id: int,
    ) -> list[Engagement]:
        """List running engagements where the employee is assigned as onboarding assistant."""
        query = (
            select(Engagement)
            .join(
                OnboardingAssistantAssignment,
                OnboardingAssistantAssignment.engagement_id == Engagement.engagement_id,
            )
            .where(
                OnboardingAssistantAssignment.employee_id == employee_id,
                Engagement.status == "running",
            )
            .order_by(Engagement.start_date.desc(), Engagement.engagement_id.desc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_engagements_for_assigned_org_contact_person(
        self,
        db: AsyncSession,
        *,
        employee_id: int,
        user_id: int,
        allowed_cities: list[str] | None = None,
    ) -> list[Engagement]:
        """List engagements assigned to employee where user is in org contact persons (any status)."""
        from modules.organizations.models import Organization
        from modules.organizations.repository import OrganizationsRepository

        query = (
            select(Engagement)
            .join(
                OnboardingAssistantAssignment,
                OnboardingAssistantAssignment.engagement_id == Engagement.engagement_id,
            )
            .join(
                Organization,
                Organization.organization_id == Engagement.organization_id,
            )
            .where(
                OnboardingAssistantAssignment.employee_id == employee_id,
                OrganizationsRepository._contact_person_user_ids_contains_user(user_id),
            )
            .order_by(Engagement.start_date.desc(), Engagement.engagement_id.desc())
        )
        if allowed_cities is not None:
            normalized_cities = [city.strip() for city in allowed_cities if city and city.strip()]
            if normalized_cities:
                query = query.where(
                    func.lower(func.trim(Engagement.city)).in_(
                        [city.casefold() for city in normalized_cities]
                    )
                )
            else:
                return []
        result = await db.execute(query)
        return list(result.scalars().all())

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
        """Return user_ids for admin-role onboarding assistants assigned to an engagement."""
        from modules.employee.models import Employee, EmployeeRole

        query = (
            select(Employee.user_id)
            .join(
                OnboardingAssistantAssignment,
                OnboardingAssistantAssignment.employee_id == Employee.employee_id,
            )
            .where(OnboardingAssistantAssignment.engagement_id == engagement_id)
            .where(Employee.role == EmployeeRole.admin)
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
        participant_department_slugs: set[str] | None = None,
    ) -> list[tuple]:
        """Fetch participant enrollment rows for a specific engagement."""
        from modules.users.models import User

        offset = (page - 1) * limit

        participant_filters = [EngagementParticipant.engagement_id == engagement_id]
        if participant_department_slugs is not None:
            if not participant_department_slugs:
                return []
            participant_filters.append(
                EngagementParticipant.participant_department.in_(participant_department_slugs)
            )

        ranked_rows = (
            select(
                EngagementParticipant.engagement_participant_id,
                EngagementParticipant.engagement_id,
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
                User.age,
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
                EngagementParticipant.consultation_booking_ids,
                EngagementParticipant.is_profile_created_on_metsights,
                EngagementParticipant.is_primary_record_id_synced,
                EngagementParticipant.is_fitprint_record_id_synced,
                EngagementParticipant.barcode,
                EngagementParticipant.booking_id,
                EngagementParticipant.blood_collection_time_slot_id,
                EngagementParticipant.booked_by_user_id,
                func.row_number()
                .over(
                    partition_by=EngagementParticipant.user_id,
                    order_by=EngagementParticipant.engagement_participant_id.desc(),
                )
                .label("rn"),
            )
            .select_from(EngagementParticipant)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(*participant_filters)
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
                ranked_rows.c.age,
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
                ranked_rows.c.consultation_booking_ids,
                ranked_rows.c.is_profile_created_on_metsights,
                ranked_rows.c.is_primary_record_id_synced,
                ranked_rows.c.is_fitprint_record_id_synced,
                ranked_rows.c.barcode,
                ranked_rows.c.booking_id,
                ranked_rows.c.blood_collection_time_slot_id,
                ranked_rows.c.booked_by_user_id,
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
                User.age,
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
                EngagementParticipant.consultation_booking_ids,
                EngagementParticipant.is_profile_created_on_metsights,
                EngagementParticipant.is_primary_record_id_synced,
                EngagementParticipant.is_fitprint_record_id_synced,
                EngagementParticipant.barcode,
                EngagementParticipant.booking_id,
                EngagementParticipant.blood_collection_time_slot_id,
                EngagementParticipant.booked_by_user_id,
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
                ranked_rows.c.age,
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
                ranked_rows.c.consultation_booking_ids,
                ranked_rows.c.is_profile_created_on_metsights,
                ranked_rows.c.is_primary_record_id_synced,
                ranked_rows.c.is_fitprint_record_id_synced,
                ranked_rows.c.barcode,
                ranked_rows.c.booking_id,
                ranked_rows.c.blood_collection_time_slot_id,
                ranked_rows.c.booked_by_user_id,
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

    @staticmethod
    def _running_engagement_status_filter():
        return func.lower(func.trim(Engagement.status)) == "running"

    @staticmethod
    def _scheduled_or_running_engagement_status_filter():
        normalized = func.lower(func.trim(Engagement.status))
        return normalized.in_(("scheduled", "running"))

    async def count_running_engagements_past_end_date(
        self,
        db: AsyncSession,
        *,
        as_of: date,
    ) -> int:
        query = (
            select(func.count())
            .select_from(Engagement)
            .where(self._running_engagement_status_filter())
            .where(Engagement.end_date < as_of)
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def bulk_complete_expired_engagements(
        self,
        db: AsyncSession,
        *,
        as_of: date,
    ) -> int:
        stmt = (
            update(Engagement)
            .where(self._running_engagement_status_filter())
            .where(Engagement.end_date < as_of)
            .values(status="completed")
        )
        result = await db.execute(stmt)
        return int(result.rowcount or 0)

    @staticmethod
    def _scheduled_engagement_status_filter():
        return func.lower(func.trim(Engagement.status)) == "scheduled"

    async def count_scheduled_engagements_past_start_date(
        self,
        db: AsyncSession,
        *,
        as_of: date,
    ) -> int:
        query = (
            select(func.count())
            .select_from(Engagement)
            .where(self._scheduled_engagement_status_filter())
            .where(Engagement.start_date <= as_of)
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def bulk_activate_scheduled_engagements(
        self,
        db: AsyncSession,
        *,
        as_of: date,
    ) -> int:
        stmt = (
            update(Engagement)
            .where(self._scheduled_engagement_status_filter())
            .where(Engagement.start_date <= as_of)
            .values(status="running")
        )
        result = await db.execute(stmt)
        return int(result.rowcount or 0)

    async def list_participants_for_pretest_reminder(
        self,
        db: AsyncSession,
        *,
        collection_date: date,
    ) -> list[tuple[int, int, list[str]]]:
        """Return (user_id, engagement_id, service_keys) for scheduled or running engagements with collection on collection_date."""
        query = (
            select(
                EngagementParticipant.user_id,
                EngagementParticipant.engagement_id,
                EngagementNotification.notification_services,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .join(
                EngagementNotification,
                EngagementNotification.engagement_id == Engagement.engagement_id,
            )
            .join(
                AutoNotificationEvent,
                AutoNotificationEvent.id == EngagementNotification.notification_event_id,
            )
            .where(self._scheduled_or_running_engagement_status_filter())
            .where(EngagementParticipant.engagement_date == collection_date)
            .where(AutoNotificationEvent.event_code == "pretest_guidelines")
            .distinct()
            .order_by(
                EngagementParticipant.engagement_id.asc(),
                EngagementParticipant.user_id.asc(),
            )
        )
        result = await db.execute(query)
        return [
            (int(row.user_id), int(row.engagement_id), list(row.notification_services or []))
            for row in result.all()
        ]

    async def list_participants_for_questionnaire_reminder(
        self,
        db: AsyncSession,
        *,
        target_dates: list[date],
    ) -> list[tuple[int, int, date, list[str], list[str]]]:
        """Return (user_id, engagement_id, engagement_date, reminder_before_services, reminder_after_services)
        for scheduled or running engagements with participants whose engagement_date is in *target_dates*."""
        from modules.engagements.models import AutoNotificationEvent, EngagementNotification

        en_before = select(
            EngagementNotification.engagement_id,
            EngagementNotification.notification_services,
        ).join(
            AutoNotificationEvent,
            AutoNotificationEvent.id == EngagementNotification.notification_event_id,
        ).where(
            AutoNotificationEvent.event_code == "questionnaire_reminder_before"
        ).subquery("en_before")

        en_after = select(
            EngagementNotification.engagement_id,
            EngagementNotification.notification_services,
        ).join(
            AutoNotificationEvent,
            AutoNotificationEvent.id == EngagementNotification.notification_event_id,
        ).where(
            AutoNotificationEvent.event_code == "questionnaire_reminder_after"
        ).subquery("en_after")

        query = (
            select(
                EngagementParticipant.user_id,
                EngagementParticipant.engagement_id,
                EngagementParticipant.engagement_date,
                en_before.c.notification_services.label("qr_before_services"),
                en_after.c.notification_services.label("qr_after_services"),
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(
                en_before,
                en_before.c.engagement_id == Engagement.engagement_id,
            )
            .outerjoin(
                en_after,
                en_after.c.engagement_id == Engagement.engagement_id,
            )
            .where(self._scheduled_or_running_engagement_status_filter())
            .where(EngagementParticipant.engagement_date.in_(target_dates))
            .distinct()
            .order_by(
                EngagementParticipant.engagement_id.asc(),
                EngagementParticipant.user_id.asc(),
            )
        )
        result = await db.execute(query)
        return [
            (
                int(row.user_id),
                int(row.engagement_id),
                row.engagement_date,
                list(row.qr_before_services or []),
                list(row.qr_after_services or []),
            )
            for row in result.all()
        ]

    async def list_participants_for_consultation_notification(
        self,
        db: AsyncSession,
    ) -> list[tuple[int, int, list[str]]]:
        """Return (user_id, engagement_id, service_keys) for eligible participants.

        Eligible when engagement is scheduled/running, has consultation_ready
        notification configured, the matching report is ready on any
        individual_health_report row for that participant, and at least one
        offered consultation type is still unbooked (either no booking row yet
        or a booking exists with want=false).
        """
        query = text(
            """
            SELECT DISTINCT
                ep.user_id,
                ep.engagement_id,
                en.notification_services
            FROM engagement_participants ep
            JOIN engagements e ON e.engagement_id = ep.engagement_id
            JOIN engagement_notifications en ON en.engagement_id = e.engagement_id
            JOIN auto_notification_events ane ON ane.id = en.notification_event_id
            JOIN engagement_types et ON et.id = e.engagement_type
            JOIN individual_health_report ihr
              ON ihr.user_id = ep.user_id
             AND ihr.engagement_id = ep.engagement_id
            WHERE lower(trim(e.status)) IN ('scheduled', 'running')
              AND ane.event_code = 'consultation_ready'
              AND et.code IN ('bio_ai_with_consultation', 'blood_test_with_consultation')
              AND (
                    (et.code = 'bio_ai_with_consultation'
                     AND ihr.reports IS NOT NULL
                     AND ihr.report_url IS NOT NULL)
                 OR (et.code = 'blood_test_with_consultation'
                     AND ihr.blood_report_raw IS NOT NULL
                     AND ihr.diagnostic_report_url IS NOT NULL)
              )
              AND e.consultations IS NOT NULL
              AND jsonb_typeof(e.consultations::jsonb) = 'object'
              AND EXISTS (
                    SELECT 1
                    FROM jsonb_each(e.consultations::jsonb) AS kv(key, value)
                    WHERE (
                            (jsonb_typeof(kv.value) = 'boolean' AND kv.value = 'true'::jsonb)
                         OR (
                                jsonb_typeof(kv.value) = 'object'
                                AND COALESCE((kv.value->>'want')::boolean, false) = true
                            )
                    )
                      AND NOT EXISTS (
                            SELECT 1
                            FROM consultation_bookings cb
                            WHERE cb.engagement_participant_id = ep.engagement_participant_id
                              AND cb.expert_type = kv.key
                              AND cb.want IS TRUE
                      )
              )
            ORDER BY ep.engagement_id ASC, ep.user_id ASC
            """
        )
        result = await db.execute(query)
        return [
            (int(row.user_id), int(row.engagement_id), list(row.notification_services or []))
            for row in result.all()
        ]

    async def list_participants_for_consultation_remainder(
        self,
        db: AsyncSession,
        *,
        consultation_date: date,
    ) -> list[ConsultationRemainderParticipant]:
        """Return participants with consultation bookings for consultation_remainder notifications.

        One row per want=true booking on *consultation_date* (via consultation_booking_ids).
        """
        query = text(
            """
            SELECT
                ep.user_id,
                ep.engagement_id,
                en.notification_services,
                cb.want,
                cb.consultation_date,
                cb.consultation_slot,
                cb.expert_type
            FROM engagement_participants ep
            JOIN engagements e ON e.engagement_id = ep.engagement_id
            JOIN engagement_notifications en ON en.engagement_id = e.engagement_id
            JOIN auto_notification_events ane ON ane.id = en.notification_event_id
            JOIN LATERAL unnest(COALESCE(ep.consultation_booking_ids, ARRAY[]::integer[])) AS u(booking_id) ON true
            JOIN consultation_bookings cb ON cb.consultation_id = u.booking_id
            WHERE lower(trim(e.status)) IN ('scheduled', 'running')
              AND ane.event_code = 'consultation_remainder'
              AND cb.consultation_date = :consultation_date
              AND cb.want IS TRUE
            ORDER BY ep.engagement_id ASC, ep.user_id ASC, cb.expert_type ASC
            """
        )
        result = await db.execute(query, {"consultation_date": consultation_date})
        return [
            ConsultationRemainderParticipant(
                user_id=int(row.user_id),
                engagement_id=int(row.engagement_id),
                service_keys=list(row.notification_services or []),
                want=bool(row.want),
                consultation_date=row.consultation_date,
                consultation_slot=row.consultation_slot,
                expert_type=str(row.expert_type),
            )
            for row in result.all()
        ]
