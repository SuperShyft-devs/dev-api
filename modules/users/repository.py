"""Users repository.

Only database queries live here.
"""

from __future__ import annotations

from typing import Optional

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.users.models import User


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
