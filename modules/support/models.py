"""Support module models."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, func, text

from db.base import Base
from db.column_types import STATUS_SUPPORT_TICKET, SupportTicketStatusColumn, status_server_default


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
    status = Column(
        SupportTicketStatusColumn,
        nullable=False,
        server_default=status_server_default("open", STATUS_SUPPORT_TICKET),
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    resolved_by_employee_id = Column(
        Integer,
        ForeignKey("employee.employee_id", ondelete="SET NULL"),
        nullable=True,
    )
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolution_notes = Column(Text, nullable=True)
