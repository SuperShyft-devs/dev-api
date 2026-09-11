"""Partner OTP authentication service."""

from __future__ import annotations

import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from common.phone import phone_lookup_candidates
from common.subject_otp import (
    global_bypass_allowed,
    hash_otp,
    hash_refresh_token,
    per_phone_bypass_allowed,
    verify_otp,
)
from core.config import settings
from core.exceptions import AppError
from core.security import generate_secure_token
from core.subject_auth import create_access_token
from modules.audit.service import AuditService
from modules.notifications.service import NotificationsService
from modules.partners.models import Partner, PartnerAuthOtpSession, PartnerAuthToken
from modules.partners.repository import PartnersRepository


@dataclass(frozen=True)
class PartnerTokenPair:
    access_token: str
    refresh_token: str


@dataclass(frozen=True)
class PartnerOtpDelivery:
    partner_id: int
    name: str
    phone: str | None
    email: str | None
    otp: str
    service_key: str


class PartnerAuthService:
    """Partner OTP auth — mirrors user AuthService patterns."""

    _DONTSENDOTP_SUFFIX = "dontsendotp"
    _MAX_OTP_ATTEMPTS = 5

    def __init__(
        self,
        *,
        repository: PartnersRepository,
        audit_service: AuditService,
        notifications_service: NotificationsService,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._notifications_service = notifications_service

    def _otp_secret(self) -> str:
        secret = settings.get_otp_hmac_secret()
        if not secret:
            raise ValueError("OTP HMAC secret is missing")
        return secret

    def _issue_access_token(self, partner_id: int) -> str:
        return create_access_token(partner_id, "partner")

    def _build_refresh_token(self, token_id: int) -> str:
        return f"{token_id}.{generate_secure_token(32)}"

    def _phone_lookup_candidates(self, phone: str) -> list[str]:
        return phone_lookup_candidates(phone, strict=True)

    def _split_send_otp_phone(self, phone: str) -> tuple[list[str], bool]:
        raw_phone = (phone or "").strip()
        suffix = self._DONTSENDOTP_SUFFIX
        lowered = raw_phone.lower()

        skip_send = False
        core = raw_phone
        if lowered.endswith(suffix):
            skip_send = True
            core = raw_phone[: -len(suffix)].strip()
        elif suffix in lowered:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        return self._phone_lookup_candidates(core), skip_send

    async def _resolve_partner_by_phone(
        self, db: AsyncSession, phone_candidates: list[str]
    ) -> Optional[Partner]:
        matches: dict[int, Partner] = {}
        for phone in phone_candidates:
            for row in await self._repository.list_by_phone(db, phone):
                matches[row.partner_id] = row
        rows = list(matches.values())
        if not rows:
            return None
        if len(rows) > 1:
            raise AppError(
                status_code=409,
                error_code="AMBIGUOUS_PHONE",
                message="Multiple accounts match this phone number",
            )
        return rows[0]

    async def _resolve_partner_by_email(self, db: AsyncSession, email: str) -> Optional[Partner]:
        return await self._repository.get_by_email(db, email)

    async def _issue_refresh_token(self, db: AsyncSession, partner_id: int) -> str:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
        token_record = PartnerAuthToken(
            partner_id=partner_id,
            refresh_token_hash="",
            issued_at=now,
            expires_at=expires_at,
        )
        created = await self._repository.create_refresh_token(db, token_record)
        refresh_token = self._build_refresh_token(created.token_id)
        await self._repository.update_refresh_token_hash(
            db, created.token_id, hash_refresh_token(refresh_token)
        )
        return refresh_token

    def _parse_refresh_token_id(self, refresh_token: str) -> int:
        try:
            token_id_str, _ = refresh_token.split(".", 1)
            token_id = int(token_id_str)
        except Exception as exc:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc
        if token_id <= 0:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")
        return token_id

    async def send_otp(
        self,
        db: AsyncSession,
        *,
        phone: str | None = None,
        email: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[int, PartnerOtpDelivery | None]:
        skip_send = False
        service_key: str | None = None

        if phone is not None:
            phone_candidates, skip_send = self._split_send_otp_phone(phone)
            partner = await self._resolve_partner_by_phone(db, phone_candidates)
            if not skip_send:
                service_key = settings.OTP_PHONE_SERVICE_KEY
        else:
            partner = await self._resolve_partner_by_email(db, email or "")
            if not skip_send:
                service_key = settings.OTP_EMAIL_SERVICE_KEY

        if partner is None:
            raise AppError(status_code=404, error_code="PARTNER_NOT_FOUND", message="Partner does not exist")
        if (partner.status or "").lower() != "active":
            raise AppError(status_code=403, error_code="FORBIDDEN", message="Partner is not active")

        otp = str(secrets.randbelow(1_000_000)).zfill(6)
        otp_hash = hash_otp(otp, self._otp_secret())
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=5)

        session = PartnerAuthOtpSession(
            partner_id=partner.partner_id,
            otp_hash=otp_hash,
            otp_expires_at=expires_at,
            created_at=now,
        )
        await self._repository.delete_expired_otp_sessions(db)
        await self._repository.delete_all_otp_sessions_for_partner(db, partner.partner_id)
        created = await self._repository.create_otp_session(db, session)

        delivery: PartnerOtpDelivery | None = None
        if not skip_send and service_key:
            delivery = PartnerOtpDelivery(
                partner_id=partner.partner_id,
                name=partner.name or "",
                phone=partner.phone,
                email=partner.email,
                otp=otp,
                service_key=service_key,
            )

        await self._audit_service.log_event(
            db,
            action="PARTNER_AUTH_SEND_OTP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return created.session_id, delivery

    async def deliver_otp(self, db: AsyncSession, *, delivery: PartnerOtpDelivery) -> None:
        await self._notifications_service.dispatch_to_contacts(
            db,
            service_key=delivery.service_key,
            contacts=[
                {
                    "first_name": delivery.name,
                    "last_name": "",
                    "phone": delivery.phone or "",
                    "email": delivery.email or "",
                }
            ],
            otp=delivery.otp,
        )

    async def verify_otp(
        self,
        db: AsyncSession,
        *,
        phone: str | None = None,
        email: str | None = None,
        otp: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[Partner, PartnerTokenPair]:
        if phone is not None:
            phone_candidates = self._phone_lookup_candidates(phone)
            partner = await self._resolve_partner_by_phone(db, phone_candidates)
        else:
            phone_candidates = []
            partner = await self._resolve_partner_by_email(db, email or "")

        if partner is None or (partner.status or "").lower() != "active":
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        session = await self._repository.get_latest_otp_session(db, partner.partner_id)
        if session is None:
            raise AppError(
                status_code=401,
                error_code="AUTH_FAILED",
                message="OTP expired or not found. Please request a new code.",
            )

        now = datetime.now(timezone.utc)
        if now >= session.otp_expires_at:
            await self._repository.delete_otp_session(db, session.session_id)
            raise AppError(
                status_code=401,
                error_code="AUTH_FAILED",
                message="OTP expired or not found. Please request a new code.",
            )

        if getattr(session, "failed_attempts", 0) >= self._MAX_OTP_ATTEMPTS:
            await self._repository.delete_otp_session(db, session.session_id)
            raise AppError(status_code=429, error_code="RATE_LIMITED", message="Too many failed attempts")

        bypass_allowed = per_phone_bypass_allowed(phone_candidates, otp) or global_bypass_allowed(otp)
        if not bypass_allowed and not verify_otp(otp, session.otp_hash, self._otp_secret()):
            session.failed_attempts = getattr(session, "failed_attempts", 0) + 1
            await db.flush()
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Invalid OTP")

        refresh_token = await self._issue_refresh_token(db, partner.partner_id)
        access_token = self._issue_access_token(partner.partner_id)

        await self._audit_service.log_event(
            db,
            action="PARTNER_AUTH_LOGIN",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        await db.flush()
        await self._repository.delete_otp_session(db, session.session_id)

        return partner, PartnerTokenPair(access_token=access_token, refresh_token=refresh_token)

    async def refresh_tokens(
        self,
        db: AsyncSession,
        *,
        refresh_token: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[Partner, PartnerTokenPair]:
        token_id = self._parse_refresh_token_id(refresh_token)
        record = await self._repository.get_refresh_token_record(db, token_id)
        if record is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        now = datetime.now(timezone.utc)
        if now >= record.expires_at:
            await self._repository.delete_refresh_token_record(db, token_id)
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        if not hmac.compare_digest(hash_refresh_token(refresh_token), record.refresh_token_hash):
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        partner = await self._repository.get_by_id(db, record.partner_id)
        if partner is None or (partner.status or "").lower() != "active":
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        new_refresh_token = self._build_refresh_token(record.token_id)
        await self._repository.update_refresh_token_hash(
            db, record.token_id, hash_refresh_token(new_refresh_token)
        )

        await self._audit_service.log_event(
            db,
            action="PARTNER_AUTH_REFRESH",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
        return partner, PartnerTokenPair(
            access_token=self._issue_access_token(partner.partner_id),
            refresh_token=new_refresh_token,
        )

    async def logout(
        self,
        db: AsyncSession,
        *,
        refresh_token: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        token_id = self._parse_refresh_token_id(refresh_token)
        record = await self._repository.get_refresh_token_record(db, token_id)
        if record is None:
            return
        if not hmac.compare_digest(hash_refresh_token(refresh_token), record.refresh_token_hash):
            return

        await self._repository.delete_refresh_token_record(db, token_id)
        await self._audit_service.log_event(
            db,
            action="PARTNER_AUTH_LOGOUT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=None,
            session_id=None,
        )
