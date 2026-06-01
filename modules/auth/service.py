"""Auth service.

Business rules live here.

Rules:
- Users must already exist.
- OTP is sent via notification dispatch (phone or email).
- OTP is time bound and single use.
- OTP value is never logged.
- Refresh tokens are stored hashed.
- Refresh tokens are rotated on use.
- Logout invalidates refresh token.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from core.security import create_jwt_token, generate_secure_token

from common.phone import phone_lookup_candidates
from modules.audit.service import AuditService
from modules.auth.repository import AuthRepository
from modules.auth.models import AuthOtpSession, AuthToken
from modules.notifications.schemas import DispatchRequest
from modules.notifications.service import NotificationsService
from modules.users.models import User
from modules.users.service import UsersService


def _hash_otp(otp: str, secret: str) -> str:
    """Hash OTP using HMAC-SHA256."""
    return hmac.new(secret.encode("utf-8"), otp.encode("utf-8"), hashlib.sha256).hexdigest()


def _verify_otp(otp: str, otp_hash: str, secret: str) -> bool:
    """Constant time OTP hash verification."""
    derived = _hash_otp(otp, secret)
    return hmac.compare_digest(derived, otp_hash)


def _per_phone_bypass_allowed(phone_candidates: list[str], otp: str) -> bool:
    """True when otp matches a BYPASS_OTP_BY_PHONE entry for one of the phone candidates."""
    index = settings.get_bypass_otp_by_phone()
    if not index:
        return False
    for candidate in phone_candidates:
        expected = index.get(candidate)
        if expected and hmac.compare_digest(otp, expected):
            return True
    return False


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str


@dataclass(frozen=True)
class OtpDelivery:
    user_id: int
    otp: str
    service_key: str


class AuthService:
    """Auth service layer."""

    def __init__(
        self,
        *,
        repository: AuthRepository,
        users_service: UsersService,
        audit_service: AuditService,
        notifications_service: NotificationsService,
    ):
        self._repository = repository
        self._users_service = users_service
        self._audit_service = audit_service
        self._notifications_service = notifications_service

    def _otp_secret(self) -> str:
        secret = settings.get_otp_hmac_secret()
        if not secret:
            raise ValueError("OTP HMAC secret is missing")
        return secret

    def _issue_access_token(self, user_id: int) -> str:
        return create_jwt_token(
            {"sub": str(user_id)},
            timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES),
        )

    def _build_refresh_token(self, token_id: int) -> str:
        raw = generate_secure_token(32)
        return f"{token_id}.{raw}"

    def _hash_refresh_token(self, refresh_token: str) -> str:
        secret = settings.get_refresh_token_secret()
        if not secret:
            raise ValueError("Refresh token secret is missing")
        return hmac.new(secret.encode("utf-8"), refresh_token.encode("utf-8"), hashlib.sha256).hexdigest()

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

    async def _resolve_user_for_otp(self, db: AsyncSession, phone_candidates: list[str]) -> Optional[User]:
        """Pick the account that receives OTP for this phone.

        If any primary user has this phone, that user wins (shared family number).
        Otherwise exactly one linked profile with this phone must exist.
        """
        rows_by_user_id: dict[int, User] = {}
        for phone in phone_candidates:
            for row in await self._repository.list_users_by_phone(db, phone):
                rows_by_user_id[row.user_id] = row
        rows = list(rows_by_user_id.values())
        if not rows:
            return None
        primaries = [u for u in rows if u.parent_id is None]
        if len(primaries) > 1:
            raise AppError(
                status_code=409,
                error_code="AMBIGUOUS_PHONE",
                message="Multiple accounts match this phone number",
            )
        if len(primaries) == 1:
            return primaries[0]
        subs = [u for u in rows if u.parent_id is not None]
        if len(subs) > 1:
            raise AppError(
                status_code=409,
                error_code="AMBIGUOUS_PHONE",
                message="Multiple accounts match this phone number",
            )
        return subs[0] if subs else None

    async def _resolve_user_for_otp_by_email(self, db: AsyncSession, email: str) -> Optional[User]:
        normalized = (email or "").strip().lower()
        if not normalized:
            return None
        return await self._repository.get_user_by_email(db, normalized)

    async def _issue_refresh_token_for_user(self, db: AsyncSession, user_id: int) -> str:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(days=settings.JWT_REFRESH_TOKEN_EXPIRE_DAYS)
        token_record = AuthToken(
            user_id=user_id,
            refresh_token_hash="",
            issued_at=now,
            expires_at=expires_at,
        )
        created = await self._repository.create_refresh_token(db, token_record)
        refresh_token = self._build_refresh_token(created.token_id)
        refresh_token_hash = self._hash_refresh_token(refresh_token)
        await self._repository.update_refresh_token_hash(db, created.token_id, refresh_token_hash)
        return refresh_token

    _DONTSENDOTP_SUFFIX = "dontsendotp"

    async def send_otp(
        self,
        db: AsyncSession,
        *,
        phone: str | None = None,
        email: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[int, OtpDelivery | None]:
        skip_send = False
        service_key: str | None = None

        if phone is not None:
            phone_candidates, skip_send = self._split_send_otp_phone(phone)
            user = await self._resolve_user_for_otp(db, phone_candidates)
            if not skip_send:
                service_key = settings.OTP_PHONE_SERVICE_KEY
        else:
            user = await self._resolve_user_for_otp_by_email(db, email or "")
            if not skip_send:
                service_key = settings.OTP_EMAIL_SERVICE_KEY

        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        otp = str(secrets.randbelow(1_000_000)).zfill(6)
        otp_hash = _hash_otp(otp, self._otp_secret())

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=5)

        session = AuthOtpSession(
            user_id=user.user_id,
            otp_hash=otp_hash,
            otp_expires_at=expires_at,
            created_at=now,
        )

        await self._repository.delete_expired_otp_sessions(db)
        await self._repository.delete_all_otp_sessions_for_user(db, user.user_id)
        created = await self._repository.create_otp_session(db, session)

        delivery: OtpDelivery | None = None
        if not skip_send and service_key:
            delivery = OtpDelivery(user_id=user.user_id, otp=otp, service_key=service_key)

        await self._audit_service.log_event(
            db,
            action="AUTH_SEND_OTP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=created.session_id,
        )

        return created.session_id, delivery

    async def _resolve_user_for_resend(
        self,
        db: AsyncSession,
        *,
        phone: str | None,
        email: str | None,
    ) -> User:
        has_phone = phone is not None and phone.strip() != ""
        has_email = email is not None and email.strip() != ""

        user: User | None = None
        if has_phone:
            phone_candidates, _ = self._split_send_otp_phone(phone or "")
            user = await self._resolve_user_for_otp(db, phone_candidates)
        if user is None and has_email:
            user = await self._resolve_user_for_otp_by_email(db, email or "")

        if user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        if has_phone and has_email:
            normalized_email = (email or "").strip().lower()
            account_email = (user.email or "").strip().lower()
            if account_email and account_email != normalized_email:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="Phone and email do not match the same account",
                )

        return user

    def _build_resend_otp_deliveries(
        self,
        *,
        user: User,
        via: str | None,
        otp: str,
    ) -> list[OtpDelivery]:
        phone_service_key = settings.OTP_PHONE_SERVICE_KEY
        email_service_key = settings.OTP_EMAIL_SERVICE_KEY

        if via == "whatsapp":
            return [OtpDelivery(user_id=user.user_id, otp=otp, service_key=phone_service_key)]

        if via == "email":
            if not (user.email and user.email.strip()):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message="User does not have an email on file",
                )
            return [OtpDelivery(user_id=user.user_id, otp=otp, service_key=email_service_key)]

        deliveries = [OtpDelivery(user_id=user.user_id, otp=otp, service_key=phone_service_key)]
        if user.email and user.email.strip():
            deliveries.append(
                OtpDelivery(user_id=user.user_id, otp=otp, service_key=email_service_key)
            )
        return deliveries

    async def resend_otp(
        self,
        db: AsyncSession,
        *,
        phone: str | None = None,
        email: str | None = None,
        via: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[int, list[OtpDelivery]]:
        user = await self._resolve_user_for_resend(db, phone=phone, email=email)

        otp = str(secrets.randbelow(1_000_000)).zfill(6)
        otp_hash = _hash_otp(otp, self._otp_secret())

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=5)

        session = AuthOtpSession(
            user_id=user.user_id,
            otp_hash=otp_hash,
            otp_expires_at=expires_at,
            created_at=now,
        )

        await self._repository.delete_expired_otp_sessions(db)
        await self._repository.delete_all_otp_sessions_for_user(db, user.user_id)
        created = await self._repository.create_otp_session(db, session)

        deliveries = self._build_resend_otp_deliveries(user=user, via=via, otp=otp)

        await self._audit_service.log_event(
            db,
            action="AUTH_RESEND_OTP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=created.session_id,
        )

        return created.session_id, deliveries

    async def deliver_otp_via_notifications(
        self,
        db: AsyncSession,
        *,
        delivery: OtpDelivery,
    ) -> None:
        """Send OTP via notification dispatch."""
        await self._notifications_service.dispatch(
            db,
            payload=DispatchRequest(
                service_key=delivery.service_key,
                user_ids=[delivery.user_id],
                otp=delivery.otp,
            ),
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
    ) -> tuple[int, TokenPair]:
        if phone is not None:
            phone_candidates = self._phone_lookup_candidates(phone)
            user = await self._resolve_user_for_otp(db, phone_candidates)
        else:
            phone_candidates = []
            user = await self._resolve_user_for_otp_by_email(db, email or "")
        if user is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        session = await self._repository.get_latest_otp_session(db, user.user_id)
        if session is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        _MAX_OTP_ATTEMPTS = 5

        now = datetime.now(timezone.utc)
        if now >= session.otp_expires_at:
            await self._repository.delete_otp_session(db, session.session_id)
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        if getattr(session, "failed_attempts", 0) >= _MAX_OTP_ATTEMPTS:
            await self._repository.delete_otp_session(db, session.session_id)
            raise AppError(status_code=429, error_code="RATE_LIMITED", message="Too many failed attempts")

        bypass_allowed = _per_phone_bypass_allowed(phone_candidates, otp) or (
            settings.ALLOW_BYPASS_OTP
            and settings.BYPASS_OTP
            and hmac.compare_digest(otp, settings.BYPASS_OTP)
        )
        if not bypass_allowed and not _verify_otp(otp, session.otp_hash, self._otp_secret()):
            session.failed_attempts = getattr(session, "failed_attempts", 0) + 1
            await db.flush()
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        # Store session_id before deletion for audit log
        session_id_for_audit = session.session_id

        refresh_token = await self._issue_refresh_token_for_user(db, user.user_id)

        access_token = self._issue_access_token(user.user_id)

        # Log the audit event BEFORE deleting the session to avoid FK constraint violation
        await self._audit_service.log_event(
            db,
            action="AUTH_LOGIN",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=session_id_for_audit,
        )
        
        # Flush to ensure audit log is inserted before we delete the session
        # Otherwise SQLAlchemy may reorder operations and delete before insert
        await db.flush()

        # Delete the OTP session after audit log is created and flushed
        await self._repository.delete_otp_session(db, session.session_id)

        return user.user_id, TokenPair(access_token=access_token, refresh_token=refresh_token)

    async def switch_account(
        self,
        db: AsyncSession,
        *,
        current_user_id: int,
        target_user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TokenPair:
        current_user = await self._repository.get_user_by_id(db, current_user_id)
        if current_user is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        target_user = await self._repository.get_user_by_id(db, target_user_id)
        if target_user is None:
            raise AppError(status_code=404, error_code="USER_NOT_FOUND", message="User does not exist")

        is_self = target_user.user_id == current_user_id
        is_switch_to_child = target_user.parent_id == current_user_id
        is_switch_to_parent = current_user.parent_id is not None and target_user.user_id == current_user.parent_id

        if not (is_self or is_switch_to_child or is_switch_to_parent):
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        await self._repository.delete_all_refresh_tokens_for_user(db, current_user_id)

        new_refresh_token = await self._issue_refresh_token_for_user(db, target_user.user_id)
        new_access_token = self._issue_access_token(target_user.user_id)

        await self._audit_service.log_event(
            db,
            action="AUTH_SWITCH_ACCOUNT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user_id,
            session_id=None,
        )

        return TokenPair(access_token=new_access_token, refresh_token=new_refresh_token)

    async def refresh_tokens(
        self,
        db: AsyncSession,
        *,
        refresh_token: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TokenPair:
        token_id = self._parse_refresh_token_id(refresh_token)
        record = await self._repository.get_refresh_token_record(db, token_id)
        if record is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        now = datetime.now(timezone.utc)
        if now >= record.expires_at:
            await self._repository.delete_refresh_token_record(db, token_id)
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        candidate_hash = self._hash_refresh_token(refresh_token)
        if not hmac.compare_digest(candidate_hash, record.refresh_token_hash):
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        new_refresh_token = self._build_refresh_token(record.token_id)
        await self._repository.update_refresh_token_hash(
            db,
            record.token_id,
            self._hash_refresh_token(new_refresh_token),
        )

        await self._audit_service.log_event(
            db,
            action="AUTH_REFRESH",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=record.user_id,
            session_id=None,
        )

        return TokenPair(access_token=self._issue_access_token(record.user_id), refresh_token=new_refresh_token)

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

        candidate_hash = self._hash_refresh_token(refresh_token)
        if not hmac.compare_digest(candidate_hash, record.refresh_token_hash):
            return

        await self._repository.delete_refresh_token_record(db, token_id)

        await self._audit_service.log_event(
            db,
            action="AUTH_LOGOUT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=record.user_id,
            session_id=None,
        )

    def _parse_refresh_token_id(self, refresh_token: str) -> int:
        try:
            token_id_str, _ = refresh_token.split(".", 1)
            token_id = int(token_id_str)
        except Exception as exc:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed") from exc

        if token_id <= 0:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        return token_id
