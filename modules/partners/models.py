"""Partners module models."""

from __future__ import annotations

import enum

from sqlalchemy import CheckConstraint, Column, DateTime, Index, Integer, String, func

from db.base import Base


class PartnerRole(str, enum.Enum):
    phlebo = "phlebo"
    expert = "expert"
    organization_manager = "organization_manager"


class Partner(Base):
    __tablename__ = "partners"
    __table_args__ = (
        CheckConstraint(
            "(phone IS NOT NULL AND btrim(phone) <> '') OR (email IS NOT NULL AND btrim(email) <> '')",
            name="ck_partners_phone_or_email",
        ),
        CheckConstraint(
            "role IN ('phlebo', 'expert', 'organization_manager')",
            name="ck_partners_role",
        ),
        Index("ix_partners_phone", "phone", unique=True),
        Index("ix_partners_email", "email", unique=True),
        Index("ix_partners_role", "role"),
        Index("ix_partners_status", "status"),
    )

    partner_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    phone = Column(String, nullable=True)
    email = Column(String, nullable=True)
    role = Column(String, nullable=False)
    status = Column(String, nullable=False, server_default="active", default="active")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class PartnerAuthOtpSession(Base):
    __tablename__ = "partner_auth_otp_sessions"
    __table_args__ = (Index("ix_partner_auth_otp_sessions_partner_id", "partner_id"),)

    session_id = Column(Integer, primary_key=True, autoincrement=True)
    partner_id = Column(Integer, nullable=False)
    otp_hash = Column(String, nullable=False)
    otp_expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    failed_attempts = Column(Integer, nullable=False, server_default="0", default=0)


class PartnerAuthToken(Base):
    __tablename__ = "partner_auth_tokens"
    __table_args__ = (Index("ix_partner_auth_tokens_partner_id", "partner_id"),)

    token_id = Column(Integer, primary_key=True, autoincrement=True)
    partner_id = Column(Integer, nullable=False)
    refresh_token_hash = Column(String, nullable=False)
    issued_at = Column(DateTime(timezone=True), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
