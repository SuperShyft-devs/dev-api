"""Partners repository."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import apply_sort, ilike_pattern
from modules.partners.models import Partner, PartnerAuthOtpSession, PartnerAuthToken


class PartnersRepository:
    """Database operations for partners."""

    _PARTNER_SORT_COLUMNS = {
        "partner_id": Partner.partner_id,
        "name": Partner.name,
        "role": Partner.role,
        "status": Partner.status,
        "phone": Partner.phone,
        "email": Partner.email,
    }

    def _apply_list_filters(
        self,
        query,
        *,
        status: str | None = None,
        role: str | None = None,
        search: str | None = None,
    ):
        if status is not None:
            query = query.where(Partner.status == status)
        if role is not None:
            query = query.where(Partner.role == role)
        if search is not None and search.strip():
            pattern = ilike_pattern(search)
            query = query.where(
                or_(
                    Partner.name.ilike(pattern),
                    Partner.phone.ilike(pattern),
                    Partner.email.ilike(pattern),
                    Partner.role.ilike(pattern),
                )
            )
        return query

    async def get_by_id(self, db: AsyncSession, partner_id: int) -> Optional[Partner]:
        result = await db.execute(select(Partner).where(Partner.partner_id == partner_id))
        return result.scalar_one_or_none()

    async def get_by_phone(self, db: AsyncSession, phone: str) -> Optional[Partner]:
        result = await db.execute(select(Partner).where(Partner.phone == phone))
        return result.scalar_one_or_none()

    async def get_by_email(self, db: AsyncSession, email: str) -> Optional[Partner]:
        normalized = (email or "").strip().lower()
        if not normalized:
            return None
        result = await db.execute(select(Partner).where(func.lower(Partner.email) == normalized))
        return result.scalar_one_or_none()

    async def list_by_phone(self, db: AsyncSession, phone: str) -> list[Partner]:
        result = await db.execute(select(Partner).where(Partner.phone == phone))
        return list(result.scalars().all())

    async def count_partners(
        self,
        db: AsyncSession,
        *,
        status: str | None = None,
        role: str | None = None,
        search: str | None = None,
    ) -> int:
        query = select(func.count()).select_from(Partner)
        query = self._apply_list_filters(query, status=status, role=role, search=search)
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_partners(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
        status: str | None = None,
        role: str | None = None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_dir: str | None = None,
    ) -> list[Partner]:
        offset = (page - 1) * limit
        query = select(Partner)
        query = self._apply_list_filters(query, status=status, role=role, search=search)
        query = apply_sort(
            query,
            sort_by=sort_by,
            sort_dir=sort_dir,
            columns=self._PARTNER_SORT_COLUMNS,
            default_column=Partner.partner_id,
        )
        query = query.offset(offset).limit(limit)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def create(self, db: AsyncSession, partner: Partner) -> Partner:
        db.add(partner)
        await db.flush()
        return partner

    async def update(self, db: AsyncSession, partner: Partner) -> Partner:
        db.add(partner)
        await db.flush()
        return partner

    # ── Auth OTP / refresh ──────────────────────────────────────────────

    async def create_otp_session(self, db: AsyncSession, session: PartnerAuthOtpSession) -> PartnerAuthOtpSession:
        db.add(session)
        await db.flush()
        return session

    async def get_latest_otp_session(self, db: AsyncSession, partner_id: int) -> Optional[PartnerAuthOtpSession]:
        result = await db.execute(
            select(PartnerAuthOtpSession)
            .where(PartnerAuthOtpSession.partner_id == partner_id)
            .order_by(PartnerAuthOtpSession.created_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def delete_otp_session(self, db: AsyncSession, session_id: int) -> None:
        await db.execute(delete(PartnerAuthOtpSession).where(PartnerAuthOtpSession.session_id == session_id))

    async def delete_expired_otp_sessions(self, db: AsyncSession) -> None:
        now = datetime.now(timezone.utc)
        await db.execute(delete(PartnerAuthOtpSession).where(PartnerAuthOtpSession.otp_expires_at <= now))

    async def delete_all_otp_sessions_for_partner(self, db: AsyncSession, partner_id: int) -> None:
        await db.execute(delete(PartnerAuthOtpSession).where(PartnerAuthOtpSession.partner_id == partner_id))

    async def create_refresh_token(self, db: AsyncSession, token: PartnerAuthToken) -> PartnerAuthToken:
        db.add(token)
        await db.flush()
        return token

    async def get_refresh_token_record(self, db: AsyncSession, token_id: int) -> Optional[PartnerAuthToken]:
        result = await db.execute(select(PartnerAuthToken).where(PartnerAuthToken.token_id == token_id))
        return result.scalar_one_or_none()

    async def delete_refresh_token_record(self, db: AsyncSession, token_id: int) -> None:
        await db.execute(delete(PartnerAuthToken).where(PartnerAuthToken.token_id == token_id))

    async def update_refresh_token_hash(self, db: AsyncSession, token_id: int, refresh_token_hash: str) -> None:
        await db.execute(
            update(PartnerAuthToken)
            .where(PartnerAuthToken.token_id == token_id)
            .values(refresh_token_hash=refresh_token_hash, issued_at=datetime.now(timezone.utc))
        )
