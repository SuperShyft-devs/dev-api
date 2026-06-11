"""Audit log models.

Audit logs are immutable.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB

from db.base import Base


class DataAuditLog(Base):
    """SQLAlchemy model for `data_audit_logs` table."""

    __tablename__ = "data_audit_logs"

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
