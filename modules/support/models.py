"""Support module models."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, func, text

from db.base import Base


class SupportTicket(Base):
    """SQLAlchemy model for `support_tickets` table."""

    __tablename__ = "support_tickets"
    __table_args__ = (
        Index("ix_support_tickets_status", "status"),
        Index("ix_support_tickets_user_id", "user_id"),
    )

    ticket_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=True)
    contact_input = Column(String, nullable=False)
    query_text = Column(Text, nullable=False)
    status = Column(String, nullable=False, server_default=text("'open'"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
