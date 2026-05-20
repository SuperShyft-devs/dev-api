"""Users repository.

Only database queries live here.
"""

from __future__ import annotations

from typing import Optional

from datetime import date, datetime, timezone

from sqlalchemy import asc, delete, desc, func, or_, regexp_replace, right, select, update

from common.listing import apply_sort, ilike_pattern, normalize_sort_dir
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from modules.users.models import User, UserPreference
from modules.engagements.models import Engagement, EngagementParticipant
from modules.organizations.models import Organization
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.auth.models import AuthOtpSession, AuthToken
from modules.audit.models import DataAuditLog
from modules.employee.models import Employee
from modules.engagements.models import OnboardingAssistantAssignment
from modules.questionnaire.models import QuestionnaireResponse
from modules.payments.models import Booking, Order, OrderBooking, Payment
from modules.reports.models import IndividualHealthReport, OrganizationHealthReport, ReportsUserSyncState
from modules.support.models import SupportTicket


class UsersRepository:
    """User database queries."""

    _USER_SORT_COLUMNS = {
        "user_id": User.user_id,
        "first_name": User.first_name,
        "last_name": User.last_name,
        "phone": User.phone,
        "email": User.email,
        "status": User.status,
        "city": User.city,
        "age": User.age,
        "created_at": User.created_at,
        "updated_at": User.updated_at,
    }

    def _apply_user_list_filters(
        self,
        query,
        *,
        phone: str | None = None,
        email: str | None = None,
        status: str | None = None,
        is_participant: bool | None = None,
        search: str | None = None,
    ):
        if phone is not None:
            query = query.where(User.phone == phone)
        if email is not None:
            query = query.where(User.email == email)
        if status is not None:
            query = query.where(User.status == status)
        if is_participant is not None:
            query = query.where(User.is_participant == is_participant)
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            full_name = func.trim(func.concat(func.coalesce(User.first_name, ""), " ", func.coalesce(User.last_name, "")))
            query = query.where(
                or_(
                    User.first_name.ilike(pattern),
                    User.last_name.ilike(pattern),
                    full_name.ilike(pattern),
                    User.phone.ilike(pattern),
                    User.email.ilike(pattern),
                )
            )
        return query

    async def count_users(
        self,
        db: AsyncSession,
        *,
        phone: str | None = None,
        email: str | None = None,
        status: str | None = None,
        is_participant: bool | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(User)
        query = self._apply_user_list_filters(
            query,
            phone=phone,
            email=email,
            status=status,
            is_participant=is_participant,
            search=search,
        )

        result = await db.execute(query)
        return int(result.scalar_one())

    async def count_participant_metsights_stats(self, db: AsyncSession) -> tuple[int, int]:
        """Return (participants_with_metsights_profile, total_participants)."""
        base = select(func.count()).select_from(User).where(User.is_participant.is_(True))
        total_result = await db.execute(base)
        total_participants = int(total_result.scalar_one())

        with_profile_query = (
            select(func.count())
            .select_from(User)
            .where(User.is_participant.is_(True))
            .where(User.metsights_profile_id.isnot(None))
            .where(User.metsights_profile_id != "")
        )
        with_profile_result = await db.execute(with_profile_query)
        with_profile = int(with_profile_result.scalar_one())
        return with_profile, total_participants

    async def count_users_with_metsights_profile_id(self, db: AsyncSession) -> int:
        query = (
            select(func.count())
            .select_from(User)
            .where(User.metsights_profile_id.isnot(None))
            .where(User.metsights_profile_id != "")
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def get_users_by_metsights_profile_ids(
        self, db: AsyncSession, metsights_profile_ids: list[str]
    ) -> dict[str, User]:
        normalized = list({(mid or "").strip() for mid in metsights_profile_ids if (mid or "").strip()})
        if not normalized:
            return {}
        result = await db.execute(select(User).where(User.metsights_profile_id.in_(normalized)))
        users = list(result.scalars().all())
        out: dict[str, User] = {}
        for user in users:
            key = (user.metsights_profile_id or "").strip()
            if key:
                out[key] = user
        return out

    async def list_users(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        phone: str | None = None,
        email: str | None = None,
        status: str | None = None,
        is_participant: bool | None = None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[User]:
        offset = (page - 1) * limit
        query = select(User)
        query = self._apply_user_list_filters(
            query,
            phone=phone,
            email=email,
            status=status,
            is_participant=is_participant,
            search=search,
        )

        sort_columns = dict(self._USER_SORT_COLUMNS)
        if (sort_by or "").strip() == "name":
            direction = asc if normalize_sort_dir(sort_dir) == "asc" else desc
            query = query.order_by(
                direction(User.first_name),
                direction(User.last_name),
                direction(User.user_id),
            )
        else:
            query = apply_sort(
                query,
                sort_by=sort_by,
                sort_dir=sort_dir,
                columns=sort_columns,
                default_column=User.user_id,
            )

        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def list_duplicate_phone_groups(self, db: AsyncSession) -> list[list[User]]:
        """Users grouped by last-10 phone digits when more than one user shares that key."""
        digits_only = regexp_replace(User.phone, "[^0-9]", "", "g")
        phone_key = right(digits_only, 10)
        dup_keys_subq = (
            select(phone_key.label("phone_key"))
            .where(func.length(digits_only) >= 10)
            .group_by(phone_key)
            .having(func.count() > 1)
        ).subquery()

        result = await db.execute(
            select(User)
            .where(
                func.length(regexp_replace(User.phone, "[^0-9]", "", "g")) >= 10,
                right(regexp_replace(User.phone, "[^0-9]", "", "g"), 10).in_(
                    select(dup_keys_subq.c.phone_key)
                ),
            )
            .order_by(User.user_id.asc())
        )
        users = list(result.scalars().all())
        groups: dict[str, list[User]] = {}
        for user in users:
            digits = "".join(ch for ch in (user.phone or "") if ch.isdigit())
            if len(digits) < 10:
                continue
            key = digits[-10:]
            groups.setdefault(key, []).append(user)
        return [sorted(group, key=lambda u: u.user_id) for group in groups.values() if len(group) > 1]

    async def update_user_full(self, db: AsyncSession, *, user: User, data: dict) -> User:
        for field_name, value in data.items():
            setattr(user, field_name, value)

        user.updated_at = datetime.now(timezone.utc)
        db.add(user)
        await db.flush()
        return user

    async def get_user_by_phone(self, db: AsyncSession, phone: str) -> Optional[User]:
        """Return one user for an exact phone match (prefers primary over sub-profile)."""

        normalized = (phone or "").strip()
        if not normalized:
            return None
        result = await db.execute(select(User).where(User.phone == normalized))
        rows = list(result.scalars().all())
        if not rows:
            return None
        if len(rows) == 1:
            return rows[0]
        primaries = [u for u in rows if u.parent_id is None]
        if len(primaries) == 1:
            return primaries[0]
        if primaries:
            return primaries[0]
        return rows[0]

    async def list_users_by_phone_exact(self, db: AsyncSession, phone: str) -> list[User]:
        normalized = (phone or "").strip()
        if not normalized:
            return []
        result = await db.execute(select(User).where(User.phone == normalized))
        return list(result.scalars().all())

    async def list_users_by_phones(self, db: AsyncSession, phones: list[str]) -> list[User]:
        normalized = list({(p or "").strip() for p in phones if (p or "").strip()})
        if not normalized:
            return []
        result = await db.execute(select(User).where(User.phone.in_(normalized)))
        return list(result.scalars().all())

    async def list_users_by_emails(self, db: AsyncSession, emails: list[str]) -> list[User]:
        normalized = list({(e or "").strip() for e in emails if (e or "").strip()})
        if not normalized:
            return []
        result = await db.execute(select(User).where(User.email.in_(normalized)))
        return list(result.scalars().all())

    async def get_user_by_id(self, db: AsyncSession, user_id: int) -> Optional[User]:
        result = await db.execute(select(User).where(User.user_id == user_id))
        return result.scalar_one_or_none()

    async def get_user_by_email(self, db: AsyncSession, email: str) -> Optional[User]:
        result = await db.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()

    async def get_user_by_metsights_profile_id(self, db: AsyncSession, metsights_profile_id: str) -> Optional[User]:
        normalized = (metsights_profile_id or "").strip()
        if not normalized:
            return None
        result = await db.execute(select(User).where(User.metsights_profile_id == normalized))
        return result.scalar_one_or_none()

    async def get_profiles_as_primary(self, db: AsyncSession, user_id: int) -> list[User]:
        result = await db.execute(select(User).where(User.parent_id == user_id).order_by(User.user_id.asc()))
        return list(result.scalars().all())

    async def get_profiles_as_sub(self, db: AsyncSession, parent_id: int) -> list[User]:
        result = await db.execute(select(User).where(User.user_id == parent_id).limit(1))
        row = result.scalar_one_or_none()
        return [row] if row is not None else []

    async def create_sub_profile(self, db: AsyncSession, parent_user: User, data: dict) -> User:
        sub_profile = User(
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            age=data.get("age"),
            phone=data.get("phone") or parent_user.phone,
            email=data.get("email"),
            date_of_birth=data.get("date_of_birth"),
            gender=data.get("gender"),
            city=data.get("city"),
            status="active",
            parent_id=parent_user.user_id,
            relationship=data.get("relationship"),
        )
        db.add(sub_profile)
        await db.flush()
        return sub_profile

    async def update_user_partial(self, db: AsyncSession, user_id: int, fields: dict) -> Optional[User]:
        row = await self.get_user_by_id(db, user_id)
        if row is None:
            return None

        for field_name, value in fields.items():
            setattr(row, field_name, value)

        row.updated_at = datetime.now(timezone.utc)
        db.add(row)
        await db.flush()
        return row

    async def get_upcoming_slots(self, db: AsyncSession, user_id: int):
        today = date.today()
        query = (
            select(
                EngagementParticipant.slot_start_time.label("slot_start_time"),
                EngagementParticipant.engagement_date.label("engagement_date"),
                Engagement.engagement_type.label("engagement_type"),
                Engagement.slot_duration.label("slot_duration"),
                Engagement.city.label("engagement_city"),
                Engagement.address.label("engagement_address"),
                Engagement.pincode.label("engagement_pincode"),
                Engagement.organization_id.label("organization_id"),
                Organization.name.label("organization_name"),
                User.address.label("user_address"),
                User.city.label("user_city"),
            )
            .select_from(EngagementParticipant)
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(Organization, Organization.organization_id == Engagement.organization_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(
                EngagementParticipant.user_id == user_id,
                EngagementParticipant.engagement_date >= today,
            )
            .order_by(EngagementParticipant.engagement_date.asc(), EngagementParticipant.slot_start_time.asc())
        )
        result = await db.execute(query)
        return result.all()

    async def get_preferences(self, db: AsyncSession, user_id: int) -> Optional[UserPreference]:
        result = await db.execute(select(UserPreference).where(UserPreference.user_id == user_id))
        return result.scalar_one_or_none()

    async def upsert_preferences(self, db: AsyncSession, user_id: int, data: dict) -> UserPreference:
        now = datetime.now(timezone.utc)
        upsert_data = dict(data)

        # Nutrition fields are optional and should only be persisted when explicitly non-null.
        for field_name in ("diet_preference", "allergies"):
            if upsert_data.get(field_name) is None:
                upsert_data.pop(field_name, None)

        update_values = {**upsert_data, "updated_at": now}

        statement = (
            insert(UserPreference)
            .values(user_id=user_id, **upsert_data)
            .on_conflict_do_update(
                index_elements=[UserPreference.user_id],
                set_=update_values,
            )
            .returning(UserPreference.preference_id)
        )
        result = await db.execute(statement)
        preference_id = result.scalar_one()

        row = await db.execute(
            select(UserPreference).where(UserPreference.preference_id == preference_id)
        )
        return row.scalar_one()

    async def update_user_profile(self, db: AsyncSession, *, user: User, payload) -> User:
        data = payload.model_dump(exclude_unset=True)

        for field_name, value in data.items():
            setattr(user, field_name, value)

        # Ensure updated_at changes even on SQLite or when DB doesn't apply server onupdate.
        user.updated_at = datetime.now(timezone.utc)

        db.add(user)
        await db.flush()
        return user

    async def create_user(self, db: AsyncSession, user: User) -> User:
        db.add(user)
        await db.flush()
        return user

    async def patch_missing_fields(self, db: AsyncSession, *, user: User, data: dict) -> User:
        """Update only fields that are currently empty.

        Empty is defined as:
        - None
        - "" (after stripping)

        This method never overwrites a non-empty value.
        """

        for field_name, new_value in data.items():
            if new_value is None:
                continue

            existing_value = getattr(user, field_name)

            if existing_value is None:
                setattr(user, field_name, new_value)
                continue

            if isinstance(existing_value, str) and existing_value.strip() == "":
                setattr(user, field_name, new_value)

        user.updated_at = datetime.now(timezone.utc)
        db.add(user)
        await db.flush()
        return user

    async def get_employee_by_user_id(self, db: AsyncSession, user_id: int) -> Employee | None:
        result = await db.execute(select(Employee).where(Employee.user_id == user_id).limit(1))
        return result.scalar_one_or_none()

    async def list_descendant_user_ids(self, db: AsyncSession, root_user_id: int) -> list[int]:
        """Return root user id + all descendants through users.parent_id."""
        all_ids: set[int] = {root_user_id}
        frontier: list[int] = [root_user_id]

        while frontier:
            result = await db.execute(select(User.user_id).where(User.parent_id.in_(frontier)))
            child_ids = [int(v) for v in result.scalars().all()]
            unseen = [uid for uid in child_ids if uid not in all_ids]
            if not unseen:
                break
            all_ids.update(unseen)
            frontier = unseen

        return sorted(all_ids)

    async def delete_user_related_data(self, db: AsyncSession, user_ids: list[int]) -> None:
        if not user_ids:
            return

        engagement_ids = list(
            (
                await db.execute(
                    select(EngagementParticipant.engagement_id).where(EngagementParticipant.user_id.in_(user_ids)).distinct()
                )
            )
            .scalars()
            .all()
        )

        assessment_instance_ids = list(
            (
                await db.execute(select(AssessmentInstance.assessment_instance_id).where(AssessmentInstance.user_id.in_(user_ids)))
            )
            .scalars()
            .all()
        )

        if assessment_instance_ids:
            await db.execute(
                delete(QuestionnaireResponse).where(
                    QuestionnaireResponse.assessment_instance_id.in_(assessment_instance_ids)
                )
            )
            await db.execute(
                delete(AssessmentCategoryProgress).where(
                    AssessmentCategoryProgress.assessment_instance_id.in_(assessment_instance_ids)
                )
            )
            await db.execute(
                delete(IndividualHealthReport).where(
                    IndividualHealthReport.assessment_instance_id.in_(assessment_instance_ids)
                )
            )

        await db.execute(delete(ReportsUserSyncState).where(ReportsUserSyncState.user_id.in_(user_ids)))

        # Payments / checkout: bookings belong to this user, but orders.user_id may be another
        # payer (e.g. family checkout). Remove by booking_id and related order_ids first.
        user_booking_ids = select(Booking.booking_id).where(Booking.user_id.in_(user_ids))
        order_ids_linked_via_order_bookings = select(OrderBooking.order_id).where(
            OrderBooking.booking_id.in_(user_booking_ids)
        )

        await db.execute(
            delete(Payment).where(
                or_(
                    Payment.user_id.in_(user_ids),
                    Payment.booking_id.in_(user_booking_ids),
                    Payment.order_id.in_(select(Order.order_id).where(Order.booking_id.in_(user_booking_ids))),
                    Payment.order_id.in_(order_ids_linked_via_order_bookings),
                )
            )
        )
        # Delete orders before bookings: orders.booking_id FKs bookings; multi-line checkout may use
        # order_bookings only. CASCADE clears order_bookings for removed orders.
        await db.execute(
            delete(Order).where(
                or_(
                    Order.user_id.in_(user_ids),
                    Order.booking_id.in_(user_booking_ids),
                    Order.order_id.in_(order_ids_linked_via_order_bookings),
                )
            )
        )
        await db.execute(delete(OrderBooking).where(OrderBooking.booking_id.in_(user_booking_ids)))
        await db.execute(delete(Booking).where(Booking.user_id.in_(user_ids)))
        await db.execute(delete(SupportTicket).where(SupportTicket.user_id.in_(user_ids)))
        await db.execute(delete(AssessmentInstance).where(AssessmentInstance.user_id.in_(user_ids)))
        await db.execute(delete(AuthToken).where(AuthToken.user_id.in_(user_ids)))
        await db.execute(delete(AuthOtpSession).where(AuthOtpSession.user_id.in_(user_ids)))
        await db.execute(delete(DataAuditLog).where(DataAuditLog.user_id.in_(user_ids)))
        await db.execute(delete(UserPreference).where(UserPreference.user_id.in_(user_ids)))
        await db.execute(delete(EngagementParticipant).where(EngagementParticipant.user_id.in_(user_ids)))

        if engagement_ids:
            orphan_engagement_ids = list(
                (
                    await db.execute(
                        select(Engagement.engagement_id)
                        .where(Engagement.engagement_id.in_(engagement_ids))
                        .where(
                            ~Engagement.engagement_id.in_(
                                select(EngagementParticipant.engagement_id).distinct()
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )

            if orphan_engagement_ids:
                orphan_assessment_ids = list(
                    (
                        await db.execute(
                            select(AssessmentInstance.assessment_instance_id).where(
                                AssessmentInstance.engagement_id.in_(orphan_engagement_ids)
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                if orphan_assessment_ids:
                    await db.execute(
                        delete(QuestionnaireResponse).where(
                            QuestionnaireResponse.assessment_instance_id.in_(orphan_assessment_ids)
                        )
                    )
                    await db.execute(
                        delete(AssessmentCategoryProgress).where(
                            AssessmentCategoryProgress.assessment_instance_id.in_(orphan_assessment_ids)
                        )
                    )
                    await db.execute(
                        delete(IndividualHealthReport).where(
                            IndividualHealthReport.assessment_instance_id.in_(orphan_assessment_ids)
                        )
                    )
                    await db.execute(
                        delete(AssessmentInstance).where(AssessmentInstance.assessment_instance_id.in_(orphan_assessment_ids))
                    )

                await db.execute(
                    delete(OrganizationHealthReport).where(
                        OrganizationHealthReport.engagement_id.in_(orphan_engagement_ids)
                    )
                )
                await db.execute(
                    delete(OnboardingAssistantAssignment).where(
                        OnboardingAssistantAssignment.engagement_id.in_(orphan_engagement_ids)
                    )
                )
                await db.execute(delete(Engagement).where(Engagement.engagement_id.in_(orphan_engagement_ids)))

        # Employee rows reference users; organizations and onboarding assignments reference employee.
        employee_ids_subq = select(Employee.employee_id).where(Employee.user_id.in_(user_ids))
        await db.execute(
            delete(OnboardingAssistantAssignment).where(OnboardingAssistantAssignment.employee_id.in_(employee_ids_subq))
        )
        await db.execute(
            update(Organization)
            .where(Organization.bd_employee_id.in_(employee_ids_subq))
            .values(bd_employee_id=None)
        )
        await db.execute(
            update(Organization)
            .where(Organization.created_employee_id.in_(employee_ids_subq))
            .values(created_employee_id=None)
        )
        await db.execute(
            update(Organization)
            .where(Organization.updated_employee_id.in_(employee_ids_subq))
            .values(updated_employee_id=None)
        )
        await db.execute(delete(Employee).where(Employee.user_id.in_(user_ids)))

    async def delete_users_by_ids(self, db: AsyncSession, user_ids: list[int]) -> int:
        if not user_ids:
            return 0
        result = await db.execute(delete(User).where(User.user_id.in_(user_ids)))
        return int(result.rowcount or 0)
