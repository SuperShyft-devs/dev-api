"""Notifications module models."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.base import Base
from db.column_types import NotificationStatusColumn, notification_channel_enum


class NotificationEventBinding(Base):
    """Admin-configured notification bindings (`notification_events` table)."""

    __tablename__ = "notification_events"
    __table_args__ = (
        UniqueConstraint(
            "event_code",
            "recipient_type",
            "engagement_type_id",
            "delivery_channel",
            name="uq_notification_events_binding",
        ),
        Index("ix_notification_events_event_code", "event_code"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_code = Column(String, nullable=False)
    event_name = Column(String, nullable=False)
    recipient_type = Column(String, nullable=False)
    delivery_channel = Column(String, nullable=False)
    notification_services = Column(JSON, nullable=False, server_default=text("'[]'"))
    engagement_type_id = Column(Integer, ForeignKey("engagement_types.id"), nullable=True)
    recipient_config = Column(JSONB, nullable=True)


class NotificationService(Base):
    """SQLAlchemy model for `notification_services` table."""

    __tablename__ = "notification_services"

    notification_service_id = Column(Integer, primary_key=True, autoincrement=True)
    service_key = Column(String, nullable=False, unique=True)
    display_name = Column(String, nullable=False)
    channel = Column(notification_channel_enum, nullable=False)
    webhook_path = Column(String, nullable=False, server_default="")
    is_active = Column(Boolean, nullable=False, server_default="true")
    require_blood_report_url = Column(Boolean, nullable=False, server_default="false")
    require_bio_ai_report_url = Column(Boolean, nullable=False, server_default="false")
    require_participant_detail = Column(Boolean, nullable=False, server_default="false")
    require_otp = Column(Boolean, nullable=False, server_default="false")
    require_session_details = Column(Boolean, nullable=False, server_default="false")
    require_external_link = Column(Boolean, nullable=False, server_default="false")
    template_name = Column(String, nullable=True)
    template_variables = Column(JSONB, nullable=True)
    email_subject = Column(String, nullable=True)
    email_body = Column(Text, nullable=True)
    email_html = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Notification(Base):
    """SQLAlchemy model for `notifications` table."""

    __tablename__ = "notifications"
    __table_args__ = (
        Index(
            "ix_notifications_pending_dispatched_at",
            "dispatched_at",
            postgresql_where=text("status = 1 AND dispatched_at IS NOT NULL"),
        ),
        Index("ix_notifications_svc_eng_status", "service_key", "engagement_id", "status"),
        Index("ix_notifications_engagement_id", "engagement_id"),
        Index("ix_notifications_assessment_instance_id", "assessment_instance_id"),
        Index("ix_notifications_queue_id", "queue_id"),
    )

    notification_id = Column(Integer, primary_key=True, autoincrement=True)
    service_key = Column(String, ForeignKey("notification_services.service_key"), nullable=False)
    status = Column(NotificationStatusColumn, nullable=False)
    channel = Column(notification_channel_enum, nullable=False)
    user = Column(JSON, nullable=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=True)
    assessment_instance_id = Column(
        Integer, ForeignKey("assessment_instances.assessment_instance_id"), nullable=True
    )
    message = Column(Text, nullable=True)
    queue_id = Column(String, nullable=True)
    triggered_by_user_id = Column(Integer, ForeignKey("users.user_id"), nullable=True)
    dispatched_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
