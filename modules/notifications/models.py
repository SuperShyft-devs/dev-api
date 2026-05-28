"""Notifications module models."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Text, func

from db.base import Base


class NotificationService(Base):
    """SQLAlchemy model for `notification_services` table."""

    __tablename__ = "notification_services"

    notification_service_id = Column(Integer, primary_key=True, autoincrement=True)
    service_key = Column(String, nullable=False, unique=True)
    display_name = Column(String, nullable=False)
    channel = Column(String, nullable=False)
    webhook_path = Column(String, nullable=False)
    is_active = Column(Boolean, nullable=False, server_default="true")
    require_record_id = Column(Boolean, nullable=False, server_default="true")
    require_participant_detail = Column(Boolean, nullable=False, server_default="false")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Notification(Base):
    """SQLAlchemy model for `notifications` table."""

    __tablename__ = "notifications"

    notification_id = Column(Integer, primary_key=True, autoincrement=True)
    service_key = Column(String, ForeignKey("notification_services.service_key"), nullable=False)
    status = Column(String, nullable=False)
    channel = Column(String, nullable=False)
    user = Column(JSON, nullable=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=True)
    assessment_instance_id = Column(
        Integer, ForeignKey("assessment_instances.assessment_instance_id"), nullable=True
    )
    message = Column(Text, nullable=True)
    triggered_by_user_id = Column(Integer, ForeignKey("users.user_id"), nullable=True)
    dispatched_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
