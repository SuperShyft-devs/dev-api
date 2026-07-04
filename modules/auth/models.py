"""Auth module models.

Auth owns:
- auth_otp_sessions
- auth_tokens
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String

from db.base import Base


class AuthOtpSession(Base):
    """SQLAlchemy model for `auth_otp_sessions` table."""

    __tablename__ = "auth_otp_sessions"
    __table_args__ = (Index("ix_auth_otp_sessions_user_id", "user_id"),)

    session_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    otp_hash = Column(String, nullable=False)
    otp_expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    failed_attempts = Column(Integer, nullable=False, server_default="0", default=0)


class AuthToken(Base):
    """SQLAlchemy model for `auth_tokens` table."""

    __tablename__ = "auth_tokens"
    __table_args__ = (Index("ix_auth_tokens_user_id", "user_id"),)

    token_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    refresh_token_hash = Column(String, nullable=False)
    issued_at = Column(DateTime(timezone=True), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
