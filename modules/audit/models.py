"""Audit log models.

Audit logs are immutable.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

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
