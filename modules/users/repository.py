"""Users repository.

Only database queries live here.
"""

from __future__ import annotations

from typing import Optional

from datetime import date, datetime, timezone

from sqlalchemy import Integer, and_, asc, cast, delete, desc, extract, func, or_, select, type_coerce, update

from common.listing import apply_sort, ilike_pattern, normalize_sort_dir
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession

from modules.users.models import User, UserAddress, UserPreference
from modules.engagements.blood_booking_enums import BloodBookingStatus
from modules.engagements.models import (
    Engagement,
    EngagementParticipant,
    EngagementSlotInfo,
    EngagementType,
    ParticipantBloodBooking,
)
from modules.experts.models import ConsultationBooking
from modules.organizations.models import Organization
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.auth.models import AuthOtpSession, AuthToken
from modules.audit.models import DataAuditLog
from modules.employee.models import Employee
from modules.engagements.models import OnboardingAssistantAssignment
from modules.notifications.models import Notification
from modules.questionnaire.models import QuestionnaireResponse
from modules.payments.models import Booking, Order, OrderBooking, Payment
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
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

    async def count_users_created_by_year(self, db: AsyncSession) -> list[tuple[int, int]]:
        created_year = func.coalesce(User.created_at, User.updated_at)
        year_expr = cast(extract("year", created_year), Integer)
        rows = (
            await db.execute(
                select(year_expr.label("y"), func.count().label("count"))
                .group_by(year_expr)
                .order_by(year_expr.asc())
            )
        ).all()
        out: list[tuple[int, int]] = []
        for row in rows:
            y = row.y
            if y is None:
                continue
            out.append((int(y), int(row.count)))
        return out

    async def count_users_created_by_month(self, db: AsyncSession, *, year: int) -> list[tuple[int, int]]:
        created_ts = func.coalesce(User.created_at, User.updated_at)
        month_expr = cast(extract("month", created_ts), Integer)
        year_expr = cast(extract("year", created_ts), Integer)
        rows = (
            await db.execute(
                select(month_expr.label("m"), func.count().label("count"))
                .where(year_expr == year)
                .group_by(month_expr)
                .order_by(month_expr.asc())
            )
        ).all()
        out: list[tuple[int, int]] = []
        for row in rows:
            month = row.m
            if month is None:
                continue
            out.append((int(month), int(row.count)))
        return out

    async def count_users_with_metsights_profile_id(self, db: AsyncSession) -> int:
        query = (
            select(func.count())
            .select_from(User)
            .where(User.metsights_profile_id.isnot(None))
            .where(User.metsights_profile_id != "")
        )
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_users_with_metsights_profile_id(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
    ) -> list[User]:
        offset = (page - 1) * limit
        query = (
            select(User)
            .where(User.metsights_profile_id.isnot(None))
            .where(User.metsights_profile_id != "")
            .order_by(User.user_id.asc())
            .offset(offset)
            .limit(limit)
        )
        result = await db.execute(query)
        return list(result.scalars().all())

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
        digits_only = func.regexp_replace(User.phone, "[^0-9]", "", "g")
        phone_key = func.right(digits_only, 10)
        dup_keys_subq = (
            select(phone_key.label("phone_key"))
            .where(func.length(digits_only) >= 10)
            .group_by(phone_key)
            .having(func.count() > 1)
        ).subquery()

        result = await db.execute(
            select(User)
            .where(
                func.length(func.regexp_replace(User.phone, "[^0-9]", "", "g")) >= 10,
                func.right(func.regexp_replace(User.phone, "[^0-9]", "", "g"), 10).in_(
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
        from modules.engagements.blood_bookings_repository import current_blood_booking_subquery

        today = date.today()
        curr_pbb = current_blood_booking_subquery()
        participant_count = (
            select(func.count())
            .select_from(EngagementParticipant)
            .where(EngagementParticipant.engagement_id == Engagement.engagement_id)
            .correlate(Engagement)
            .scalar_subquery()
        )
        query = (
            select(
                curr_pbb.c.collection_time.label("slot_start_time"),
                curr_pbb.c.collection_date.label("engagement_date"),
                curr_pbb.c.booking_id.label("booking_id"),
                curr_pbb.c.collection_cabin.label("blood_collection_cabin"),
                Engagement.engagement_id.label("engagement_id"),
                Engagement.engagement_type.label("engagement_type"),
                Engagement.engagement_code.label("engagement_code"),
                Engagement.blood_collection_type.label("blood_collection_type"),
                Engagement.slot_duration.label("slot_duration"),
                Engagement.slot_detail_id.label("slot_detail_id"),
                EngagementSlotInfo.slot_detail.label("slot_detail"),
                Engagement.city.label("engagement_city"),
                Engagement.address.label("engagement_address"),
                Engagement.pincode.label("engagement_pincode"),
                Engagement.organization_id.label("organization_id"),
                Organization.name.label("organization_name"),
                EngagementParticipant.address.label("participant_address"),
                participant_count.label("participant_count"),
                User.address.label("user_address"),
                User.city.label("user_city"),
            )
            .select_from(EngagementParticipant)
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .outerjoin(Organization, Organization.organization_id == Engagement.organization_id)
            .outerjoin(EngagementSlotInfo, EngagementSlotInfo.slot_detail_id == Engagement.slot_detail_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(
                EngagementParticipant.user_id == user_id,
                curr_pbb.c.collection_date.isnot(None),
                curr_pbb.c.collection_date >= today,
            )
            .order_by(
                curr_pbb.c.collection_date.asc(),
                curr_pbb.c.collection_time.asc(),
            )
        )
        result = await db.execute(query)
        return result.all()

    def _user_consultations_query(self, user_id: int, *, status: str | None = None):
        query = (
            select(ConsultationBooking, Engagement, EngagementType)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ConsultationBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(EngagementType, EngagementType.id == Engagement.engagement_type)
            .where(EngagementParticipant.user_id == user_id)
        )
        cancelled_cond = or_(
            Engagement.status == "cancelled",
            ConsultationBooking.want.is_(False),
        )
        if status == "completed":
            query = query.where(
                ConsultationBooking.done.is_(True),
                ConsultationBooking.want.is_(True),
                Engagement.status != "cancelled",
            )
        elif status == "cancelled":
            query = query.where(cancelled_cond)
        elif status == "scheduled":
            query = query.where(
                ConsultationBooking.done.is_(False),
                ConsultationBooking.want.is_(True),
                Engagement.status != "cancelled",
            )
        return query

    async def count_user_consultations(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        status: str | None = None,
    ) -> int:
        result = await db.execute(
            self._user_consultations_query(user_id, status=status).with_only_columns(
                func.count(ConsultationBooking.consultation_id)
            )
        )
        return int(result.scalar_one() or 0)

    async def list_user_consultations(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        status: str | None = None,
        page: int = 1,
        limit: int = 20,
    ) -> list[tuple[ConsultationBooking, Engagement, EngagementType | None]]:
        offset = (page - 1) * limit
        query = (
            self._user_consultations_query(user_id, status=status)
            .order_by(
                ConsultationBooking.consultation_date.desc().nulls_last(),
                ConsultationBooking.consultation_slot.desc().nulls_last(),
                ConsultationBooking.consultation_id.desc(),
            )
            .offset(offset)
            .limit(limit)
        )
        result = await db.execute(query)
        return list(result.all())

    def _user_consultations_for_appointments_query(
        self, user_id: int, *, status: str | None = None
    ):
        """Consultation rows for /users/me/appointments (want=NULL treated as scheduled)."""
        query = (
            select(ConsultationBooking, Engagement, EngagementType)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ConsultationBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(EngagementType, EngagementType.id == Engagement.engagement_type)
            .where(EngagementParticipant.user_id == user_id)
            .where(Engagement.status != "draft")
        )
        cancelled_cond = or_(
            Engagement.status == "cancelled",
            ConsultationBooking.want.is_(False),
        )
        want_wanted = or_(
            ConsultationBooking.want.is_(True),
            ConsultationBooking.want.is_(None),
        )
        if status == "completed":
            query = query.where(
                ConsultationBooking.done.is_(True),
                want_wanted,
                Engagement.status != "cancelled",
            )
        elif status == "cancelled":
            query = query.where(cancelled_cond)
        elif status == "scheduled":
            query = query.where(
                ConsultationBooking.done.is_(False),
                want_wanted,
                Engagement.status != "cancelled",
            )
        return query

    async def list_user_consultations_for_appointments(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        status: str | None = None,
    ) -> list[tuple[ConsultationBooking, Engagement, EngagementType | None]]:
        query = self._user_consultations_for_appointments_query(user_id, status=status)
        result = await db.execute(query)
        return list(result.all())

    def _user_blood_bookings_query(self, user_id: int, *, status: str | None = None):
        query = (
            select(ParticipantBloodBooking, Engagement, EngagementType)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ParticipantBloodBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(EngagementType, EngagementType.id == Engagement.engagement_type)
            .where(EngagementParticipant.user_id == user_id)
            .where(Engagement.status != "draft")
            .where(ParticipantBloodBooking.status != BloodBookingStatus.superseded.value)
        )
        cancelled_cond = or_(
            Engagement.status == "cancelled",
            ParticipantBloodBooking.status == BloodBookingStatus.cancelled.value,
        )
        completed_cond = and_(
            ParticipantBloodBooking.collected_at.isnot(None),
            ParticipantBloodBooking.status != BloodBookingStatus.cancelled.value,
            Engagement.status != "cancelled",
        )
        scheduled_cond = and_(
            ParticipantBloodBooking.collected_at.is_(None),
            ParticipantBloodBooking.status == BloodBookingStatus.active.value,
            Engagement.status != "cancelled",
        )
        if status == "completed":
            query = query.where(completed_cond)
        elif status == "cancelled":
            query = query.where(cancelled_cond)
        elif status == "scheduled":
            query = query.where(scheduled_cond)
        return query

    async def list_user_blood_bookings_for_appointments(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        status: str | None = None,
    ) -> list[tuple[ParticipantBloodBooking, Engagement, EngagementType | None]]:
        query = self._user_blood_bookings_query(user_id, status=status)
        result = await db.execute(query)
        return list(result.all())

    async def get_preferences(self, db: AsyncSession, user_id: int) -> Optional[UserPreference]:
        result = await db.execute(select(UserPreference).where(UserPreference.user_id == user_id))
        return result.scalar_one_or_none()

    async def upsert_preferences(self, db: AsyncSession, user_id: int, data: dict) -> UserPreference:
        now = datetime.now(timezone.utc)
        upsert_data = dict(data)

        # Nutrition fields are optional and should only be persisted when explicitly non-null.
        for field_name in ("diet_preference", "allergies", "sports_playlists"):
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

    async def list_addresses_for_user(self, db: AsyncSession, user_id: int) -> list[UserAddress]:
        result = await db.execute(
            select(UserAddress)
            .where(UserAddress.user_id == user_id)
            .order_by(UserAddress.is_default.desc(), UserAddress.created_at.asc(), UserAddress.user_address_id.asc())
        )
        return list(result.scalars().all())

    async def count_addresses_for_user(self, db: AsyncSession, user_id: int) -> int:
        result = await db.execute(
            select(func.count()).select_from(UserAddress).where(UserAddress.user_id == user_id)
        )
        return int(result.scalar_one() or 0)

    async def get_address_by_id(self, db: AsyncSession, user_address_id: int) -> Optional[UserAddress]:
        result = await db.execute(
            select(UserAddress).where(UserAddress.user_address_id == user_address_id)
        )
        return result.scalar_one_or_none()

    async def get_default_address(self, db: AsyncSession, user_id: int) -> Optional[UserAddress]:
        result = await db.execute(
            select(UserAddress)
            .where(UserAddress.user_id == user_id, UserAddress.is_default.is_(True))
        )
        return result.scalar_one_or_none()

    async def get_oldest_address(self, db: AsyncSession, user_id: int) -> Optional[UserAddress]:
        result = await db.execute(
            select(UserAddress)
            .where(UserAddress.user_id == user_id)
            .order_by(UserAddress.created_at.asc(), UserAddress.user_address_id.asc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create_address(self, db: AsyncSession, address: UserAddress) -> UserAddress:
        now = datetime.now(timezone.utc)
        address.created_at = now
        address.updated_at = now
        db.add(address)
        await db.flush()
        return address

    async def clear_default_addresses(self, db: AsyncSession, user_id: int, *, exclude_id: int | None = None) -> None:
        query = (
            update(UserAddress)
            .where(UserAddress.user_id == user_id, UserAddress.is_default.is_(True))
            .values(is_default=False, updated_at=datetime.now(timezone.utc))
        )
        if exclude_id is not None:
            query = query.where(UserAddress.user_address_id != exclude_id)
        await db.execute(query)

    async def delete_address(self, db: AsyncSession, address: UserAddress) -> None:
        await db.delete(address)
        await db.flush()

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
        # Employees are no longer linked to users.
        return None

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

    async def list_engagements_fully_owned_by_users(
        self, db: AsyncSession, user_ids: list[int]
    ) -> list[Engagement]:
        """Engagements where every participant row belongs to one of user_ids."""
        if not user_ids:
            return []

        total_by_engagement = (
            select(
                EngagementParticipant.engagement_id.label("engagement_id"),
                func.count(EngagementParticipant.engagement_participant_id).label("total"),
            )
            .group_by(EngagementParticipant.engagement_id)
            .subquery()
        )
        owned_by_users = (
            select(
                EngagementParticipant.engagement_id.label("engagement_id"),
                func.count(EngagementParticipant.engagement_participant_id).label("owned"),
            )
            .where(EngagementParticipant.user_id.in_(user_ids))
            .group_by(EngagementParticipant.engagement_id)
            .subquery()
        )

        result = await db.execute(
            select(Engagement)
            .join(owned_by_users, Engagement.engagement_id == owned_by_users.c.engagement_id)
            .join(
                total_by_engagement,
                Engagement.engagement_id == total_by_engagement.c.engagement_id,
            )
            .where(owned_by_users.c.owned == total_by_engagement.c.total)
            .order_by(Engagement.engagement_id.asc())
        )
        return list(result.scalars().all())

    async def delete_user_related_data(
        self,
        db: AsyncSession,
        user_ids: list[int],
        *,
        delete_orphan_engagements: bool = False,
    ) -> None:
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
            # Match engagements purge: detach notification FKs before deleting instances.
            await db.execute(
                update(Notification)
                .where(Notification.assessment_instance_id.in_(assessment_instance_ids))
                .values(assessment_instance_id=None)
            )
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
            delete(IndividualHealthReport).where(IndividualHealthReport.user_id.in_(user_ids))
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
        await db.execute(delete(UserAddress).where(UserAddress.user_id.in_(user_ids)))
        await db.execute(delete(UserPreference).where(UserPreference.user_id.in_(user_ids)))
        await db.execute(delete(EngagementParticipant).where(EngagementParticipant.user_id.in_(user_ids)))

        if delete_orphan_engagements and engagement_ids:
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
                orphan_camp_nos = list(
                    (
                        await db.execute(
                            select(Engagement.camp_no).where(
                                Engagement.engagement_id.in_(orphan_engagement_ids),
                                Engagement.camp_no.isnot(None),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )

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
                        update(Notification)
                        .where(Notification.assessment_instance_id.in_(orphan_assessment_ids))
                        .values(assessment_instance_id=None)
                    )
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
                            IndividualHealthReport.engagement_id.in_(orphan_engagement_ids)
                        )
                    )
                    await db.execute(
                        delete(AssessmentInstance).where(AssessmentInstance.assessment_instance_id.in_(orphan_assessment_ids))
                    )

                await db.execute(
                    update(Notification)
                    .where(Notification.engagement_id.in_(orphan_engagement_ids))
                    .values(engagement_id=None)
                )
                await db.execute(
                    delete(OnboardingAssistantAssignment).where(
                        OnboardingAssistantAssignment.engagement_id.in_(orphan_engagement_ids)
                    )
                )
                await db.execute(delete(Engagement).where(Engagement.engagement_id.in_(orphan_engagement_ids)))

                if orphan_camp_nos:
                    from modules.reports.camp_reports_repository import CampReportsRepository

                    camp_reports_repo = CampReportsRepository()
                    for camp_no in set(orphan_camp_nos):
                        remaining = await camp_reports_repo.count_engagements_for_camp_no(db, camp_no=int(camp_no))
                        if remaining == 0:
                            await camp_reports_repo.delete_all_for_camp_no(db, camp_no=int(camp_no))

        # Notifications may reference the user as dispatcher; clear before user row delete.
        await db.execute(
            update(Notification)
            .where(Notification.triggered_by_user_id.in_(user_ids))
            .values(triggered_by_user_id=None)
        )
        org_rows = await db.execute(
            select(Organization).where(Organization.contact_person_user_ids.isnot(None))
        )
        # Contact JSON now stores employee_ids (staff). Removing user_ids from contact
        # persons is a no-op for staff identity; still scrub any leftover matching ints.
        from modules.organizations.contact_person import remove_user_from_contact_person_user_ids

        for organization in org_rows.scalars().all():
            for user_id in user_ids:
                updated = remove_user_from_contact_person_user_ids(
                    organization.contact_person_user_ids,
                    user_id,
                )
                if updated != organization.contact_person_user_ids:
                    organization.contact_person_user_ids = updated

        # Employees are independent of users — do not cascade-delete employees or
        # partner assignments when deleting end-user accounts.

    async def delete_users_by_ids(self, db: AsyncSession, user_ids: list[int]) -> int:
        if not user_ids:
            return 0
        result = await db.execute(delete(User).where(User.user_id.in_(user_ids)))
        return int(result.rowcount or 0)

    @staticmethod
    def _sports_playlists_jsonb(column):
        return func.coalesce(cast(column, JSONB), cast("{}", JSONB))

    @classmethod
    def _superclub_has_playlist_clause(cls, sports_jsonb):
        return or_(
            and_(
                func.jsonb_typeof(sports_jsonb["sportIds"]) == "array",
                func.jsonb_array_length(sports_jsonb["sportIds"]) > 0,
            ),
            sports_jsonb["otherSelected"].astext == "true",
        )

    def _superclub_base_query(self, *, search: str | None = None):
        sports_jsonb = self._sports_playlists_jsonb(UserPreference.sports_playlists)
        query = (
            select(User, UserPreference)
            .join(UserPreference, User.user_id == UserPreference.user_id)
            .where(self._superclub_has_playlist_clause(sports_jsonb))
        )
        query = self._apply_user_list_filters(query, search=search)
        return query, sports_jsonb

    def _apply_superclub_sport_filter(self, query, sports_jsonb, sport_ids: list[str] | None):
        if not sport_ids:
            return query
        parts = []
        for sid in sport_ids:
            normalized = (sid or "").strip()
            if not normalized:
                continue
            if normalized == "other":
                parts.append(sports_jsonb["otherSelected"].astext == "true")
            else:
                parts.append(
                    and_(
                        func.jsonb_typeof(sports_jsonb["sportIds"]) == "array",
                        sports_jsonb["sportIds"].contains(type_coerce([normalized], JSONB)),
                    )
                )
        if not parts:
            return query
        return query.where(or_(*parts))

    def _apply_superclub_other_selected(self, query, sports_jsonb, other_selected: bool | None):
        if other_selected is None:
            return query
        if other_selected:
            return query.where(sports_jsonb["otherSelected"].astext == "true")
        return query.where(
            or_(
                sports_jsonb["otherSelected"].is_(None),
                sports_jsonb["otherSelected"].astext != "true",
            )
        )

    async def count_superclub_playlists(
        self,
        db: AsyncSession,
        *,
        search: str | None = None,
        sport_ids: list[str] | None = None,
        other_selected: bool | None = None,
    ) -> int:
        query, sports_jsonb = self._superclub_base_query(search=search)
        query = self._apply_superclub_sport_filter(query, sports_jsonb, sport_ids)
        query = self._apply_superclub_other_selected(query, sports_jsonb, other_selected)
        count_query = select(func.count()).select_from(query.subquery())
        result = await db.execute(count_query)
        return int(result.scalar_one())

    async def list_superclub_playlists(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        search: str | None = None,
        sport_ids: list[str] | None = None,
        other_selected: bool | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[tuple[User, UserPreference]]:
        offset = (page - 1) * limit
        query, sports_jsonb = self._superclub_base_query(search=search)
        query = self._apply_superclub_sport_filter(query, sports_jsonb, sport_ids)
        query = self._apply_superclub_other_selected(query, sports_jsonb, other_selected)

        sort_key = (sort_by or "").strip()
        direction = asc if normalize_sort_dir(sort_dir) == "asc" else desc
        if sort_key == "name":
            query = query.order_by(direction(User.first_name), direction(User.last_name), direction(User.user_id))
        elif sort_key == "city":
            query = query.order_by(direction(User.city), direction(User.user_id))
        elif sort_key == "updated_at":
            query = query.order_by(direction(UserPreference.updated_at), direction(User.user_id))
        else:
            query = query.order_by(direction(User.user_id))

        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.all())

    async def list_superclub_playlist_payloads_for_search(
        self,
        db: AsyncSession,
        *,
        search: str | None = None,
    ) -> list[object | None]:
        query, _sports_jsonb = self._superclub_base_query(search=search)
        result = await db.execute(query)
        return [pref.sports_playlists for _user, pref in result.all()]
