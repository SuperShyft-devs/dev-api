"""Auth repository.

Only DB queries and persistence live here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from modules.auth.models import AuthOtpSession, AuthToken
from modules.users.models import User


class AuthRepository:
    """Database operations for auth."""

    async def create_otp_session(self, db: AsyncSession, session: AuthOtpSession) -> AuthOtpSession:
        db.add(session)
        await db.flush()
        return session

    async def get_latest_otp_session(self, db: AsyncSession, user_id: int) -> Optional[AuthOtpSession]:
        result = await db.execute(
            select(AuthOtpSession)
            .where(AuthOtpSession.user_id == user_id)
            .order_by(AuthOtpSession.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def delete_otp_session(self, db: AsyncSession, session_id: int) -> None:
        await db.execute(delete(AuthOtpSession).where(AuthOtpSession.session_id == session_id))

    async def delete_expired_otp_sessions(self, db: AsyncSession) -> None:
        now = datetime.now(timezone.utc)
        await db.execute(delete(AuthOtpSession).where(AuthOtpSession.otp_expires_at <= now))

    async def delete_all_otp_sessions_for_user(self, db: AsyncSession, user_id: int) -> None:
        await db.execute(delete(AuthOtpSession).where(AuthOtpSession.user_id == user_id))

    async def create_refresh_token(self, db: AsyncSession, token: AuthToken) -> AuthToken:
        db.add(token)
        await db.flush()
        return token

    async def get_refresh_token_record(self, db: AsyncSession, token_id: int) -> Optional[AuthToken]:
        result = await db.execute(select(AuthToken).where(AuthToken.token_id == token_id))
        return result.scalar_one_or_none()

    async def delete_refresh_token_record(self, db: AsyncSession, token_id: int) -> None:
        await db.execute(delete(AuthToken).where(AuthToken.token_id == token_id))

    async def delete_all_refresh_tokens_for_user(self, db: AsyncSession, user_id: int) -> None:
        await db.execute(delete(AuthToken).where(AuthToken.user_id == user_id))

    async def update_refresh_token_hash(self, db: AsyncSession, token_id: int, refresh_token_hash: str) -> None:
        await db.execute(
            update(AuthToken)
            .where(AuthToken.token_id == token_id)
            .values(refresh_token_hash=refresh_token_hash, issued_at=datetime.now(timezone.utc))
        )

    async def get_primary_user_by_phone(self, db: AsyncSession, phone: str) -> Optional[User]:
        result = await db.execute(
            select(User).where(
                User.phone == phone,
                User.parent_id.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def get_user_by_id(self, db: AsyncSession, user_id: int) -> Optional[User]:
        result = await db.execute(select(User).where(User.user_id == user_id))
        return result.scalar_one_or_none()
