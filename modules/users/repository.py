"""Users repository.

Only database queries live here.
"""

from __future__ import annotations

from typing import Optional

from datetime import date, datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from modules.users.models import User, UserPreference
from modules.engagements.models import Engagement, EngagementTimeSlot
from modules.organizations.models import Organization


class UsersRepository:
    """User database queries."""

    async def count_users(
        self,
        db: AsyncSession,
        *,
        phone: str | None = None,
        email: str | None = None,
        status: str | None = None,
        is_participant: bool | None = None,
    ) -> int:
        query = select(func.count()).select_from(User)

        if phone is not None:
            query = query.where(User.phone == phone)
        if email is not None:
            query = query.where(User.email == email)
        if status is not None:
            query = query.where(User.status == status)
        if is_participant is not None:
            query = query.where(User.is_participant == is_participant)

        result = await db.execute(query)
        return int(result.scalar_one())

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
    ) -> list[User]:
        offset = (page - 1) * limit
        query = select(User)

        if phone is not None:
            query = query.where(User.phone == phone)
        if email is not None:
            query = query.where(User.email == email)
        if status is not None:
            query = query.where(User.status == status)
        if is_participant is not None:
            query = query.where(User.is_participant == is_participant)

        query = query.order_by(User.user_id.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def update_user_full(self, db: AsyncSession, *, user: User, data: dict) -> User:
        for field_name, value in data.items():
            setattr(user, field_name, value)

        user.updated_at = datetime.now(timezone.utc)
        db.add(user)
        await db.flush()
        return user

    async def get_user_by_phone(self, db: AsyncSession, phone: str) -> Optional[User]:
        result = await db.execute(select(User).where(User.phone == phone))
        return result.scalar_one_or_none()

    async def get_user_by_id(self, db: AsyncSession, user_id: int) -> Optional[User]:
        result = await db.execute(select(User).where(User.user_id == user_id))
        return result.scalar_one_or_none()

    async def get_user_by_email(self, db: AsyncSession, email: str) -> Optional[User]:
        result = await db.execute(select(User).where(User.email == email))
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
            phone=parent_user.phone,
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
                EngagementTimeSlot.slot_start_time.label("slot_start_time"),
                EngagementTimeSlot.engagement_date.label("engagement_date"),
                Engagement.engagement_type.label("engagement_type"),
                Engagement.slot_duration.label("slot_duration"),
                Engagement.city.label("engagement_city"),
                Engagement.organization_id.label("organization_id"),
                Organization.name.label("organization_name"),
                User.address.label("user_address"),
                User.city.label("user_city"),
            )
            .select_from(EngagementTimeSlot)
            .join(Engagement, Engagement.engagement_id == EngagementTimeSlot.engagement_id)
            .outerjoin(Organization, Organization.organization_id == Engagement.organization_id)
            .join(User, User.user_id == EngagementTimeSlot.user_id)
            .where(
                EngagementTimeSlot.user_id == user_id,
                EngagementTimeSlot.engagement_date >= today,
            )
            .order_by(EngagementTimeSlot.engagement_date.asc(), EngagementTimeSlot.slot_start_time.asc())
        )
        result = await db.execute(query)
        return result.all()

    async def get_preferences(self, db: AsyncSession, user_id: int) -> Optional[UserPreference]:
        result = await db.execute(select(UserPreference).where(UserPreference.user_id == user_id))
        return result.scalar_one_or_none()

    async def upsert_preferences(self, db: AsyncSession, user_id: int, data: dict) -> UserPreference:
        now = datetime.now(timezone.utc)
        update_values = {**data, "updated_at": now}

        statement = (
            insert(UserPreference)
            .values(user_id=user_id, **data)
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
