"""Auth service.

Business rules live here.

Rules:
- Users must already exist.
- OTP is sent to phone.
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

from modules.audit.service import AuditService
from modules.auth.providers import OtpSender
from modules.auth.repository import AuthRepository
from modules.auth.models import AuthOtpSession, AuthToken
from modules.users.service import UsersService


def _hash_otp(otp: str, secret: str) -> str:
    """Hash OTP using HMAC-SHA256."""
    return hmac.new(secret.encode("utf-8"), otp.encode("utf-8"), hashlib.sha256).hexdigest()


def _verify_otp(otp: str, otp_hash: str, secret: str) -> bool:
    """Constant time OTP hash verification."""
    derived = _hash_otp(otp, secret)
    return hmac.compare_digest(derived, otp_hash)


@dataclass(frozen=True)
class TokenPair:
    access_token: str
    refresh_token: str


class AuthService:
    """Auth service layer."""

    def __init__(
        self,
        *,
        repository: AuthRepository,
        users_service: UsersService,
        audit_service: AuditService,
        otp_sender: OtpSender,
    ):
        self._repository = repository
        self._users_service = users_service
        self._audit_service = audit_service
        self._otp_sender = otp_sender

    def _otp_secret(self) -> str:
        if not settings.JWT_SECRET_KEY:
            raise ValueError("JWT secret key is missing")
        return settings.JWT_SECRET_KEY

    def _issue_access_token(self, user_id: int) -> str:
        return create_jwt_token(
            {"sub": str(user_id)},
            timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES),
        )

    def _build_refresh_token(self, token_id: int) -> str:
        raw = generate_secure_token(32)
        return f"{token_id}.{raw}"

    def _hash_refresh_token(self, refresh_token: str) -> str:
        secret = self._otp_secret()
        return hmac.new(secret.encode("utf-8"), refresh_token.encode("utf-8"), hashlib.sha256).hexdigest()

    async def _resolve_user_for_otp(self, db: AsyncSession, phone: str) -> Optional[User]:
        """Pick the account that receives OTP for this phone.

        If any primary user has this phone, that user wins (shared family number).
        Otherwise exactly one linked profile with this phone must exist.
        """
        rows = await self._repository.list_users_by_phone(db, phone)
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

    async def send_otp(
        self,
        db: AsyncSession,
        *,
        phone: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> int:
        user = await self._resolve_user_for_otp(db, phone)
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

        await self._otp_sender.send_otp(phone, otp)

        await self._audit_service.log_event(
            db,
            action="AUTH_SEND_OTP",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user.user_id,
            session_id=created.session_id,
        )

        return created.session_id

    async def verify_otp(
        self,
        db: AsyncSession,
        *,
        phone: str,
        otp: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> tuple[int, TokenPair]:
        user = await self._resolve_user_for_otp(db, phone)
        if user is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        session = await self._repository.get_latest_otp_session(db, user.user_id)
        if session is None:
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        now = datetime.now(timezone.utc)
        if now >= session.otp_expires_at:
            await self._repository.delete_otp_session(db, session.session_id)
            raise AppError(status_code=401, error_code="AUTH_FAILED", message="Authentication failed")

        bypass_allowed = settings.ALLOW_BYPASS_OTP and otp == "654321"
        if not bypass_allowed and not _verify_otp(otp, session.otp_hash, self._otp_secret()):
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
