"""Audit log models.

Audit logs are immutable.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB

from db.base import Base


class DataAuditLog(Base):
    """SQLAlchemy model for `data_audit_logs` table."""

    __tablename__ = "data_audit_logs"
    __table_args__ = (Index("ix_data_audit_logs_user_id", "user_id"),)

    audit_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    session_id = Column(Integer, ForeignKey("auth_otp_sessions.session_id", ondelete="SET NULL"), nullable=True)
    action = Column(String, nullable=False)
    ip_address = Column(String)
    user_agent = Column(String)
    endpoint = Column(String)
    timestamp = Column(DateTime(timezone=True), nullable=False)


class IntegrationSyncLog(Base):
    """SQLAlchemy model for `integration_sync_logs` table."""

    __tablename__ = "integration_sync_logs"
    __table_args__ = (
        Index("ix_integration_sync_logs_provider_status_created", "provider", "status", "created_at"),
        Index("ix_integration_sync_logs_request_payload_gin", "request_payload", postgresql_using="gin"),
    )

    sync_log_id = Column(Integer, primary_key=True, autoincrement=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id", ondelete="SET NULL"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    provider = Column(String(30), nullable=False)
    api_endpoint_url = Column(Text, nullable=False)
    request_payload = Column(JSONB, nullable=True)
    response_payload = Column(JSONB, nullable=True)
    status = Column(String(20), nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
