"""Engagements module models."""

from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Enum as SAEnum, Float, ForeignKey, Index, Integer, String, Text, Time, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import validates
from sqlalchemy.sql import func
from sqlalchemy.types import JSON

from db.base import Base
from modules.engagements.enums import BloodCollectionType, ConsultationMode, EngagementKind, EngagementStatus


_engagement_kind = SAEnum(
    EngagementKind,
    name="engagement_kind",
    native_enum=True,
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)


class EngagementSlotInfo(Base):
    """Shared cabin schedule configuration for one or more engagements."""

    __tablename__ = "engagement_slot_info"

    slot_detail_id = Column(Integer, primary_key=True, autoincrement=True)
    slot_detail = Column(JSON, nullable=False)


class Engagement(Base):
    """SQLAlchemy model for `engagements` table."""

    __tablename__ = "engagements"
    __table_args__ = (
        Index("uq_engagements_engagement_code", "engagement_code", unique=True),
        Index("ix_engagements_organization_id", "organization_id"),
        Index("ix_engagements_camp_no", "camp_no"),
    )

    engagement_id = Column(Integer, primary_key=True)
    engagement_name = Column(String)
    metsights_engagement_id = Column(String, nullable=True)
    organization_id = Column(Integer, ForeignKey("organizations.organization_id"), nullable=True)
    camp_no = Column(BigInteger, nullable=True)
    engagement_code = Column(String, nullable=False)
    engagement_type = Column(Integer, ForeignKey("engagement_types.id"), nullable=True)
    consultations = Column(JSON, nullable=True)
    slot_detail_id = Column(Integer, ForeignKey("engagement_slot_info.slot_detail_id", ondelete="SET NULL"), nullable=True)
    assessment_package_id = Column(Integer, ForeignKey("assessment_packages.package_id"), nullable=True)
    diagnostic_package_id = Column(Integer, ForeignKey("diagnostic_package.diagnostic_package_id"), nullable=True)
    address = Column(String, nullable=True)
    sub_locality = Column(String, nullable=True)
    landmark = Column(String, nullable=True)
    pincode = Column(String, nullable=True)
    city = Column(String)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    slot_duration = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    status = Column(String)
    create_profile_on_metsights = Column(Boolean, nullable=False, default=False, server_default="false")
    enroll_for_fitprint_full = Column(Boolean, nullable=False, default=False, server_default="false")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    healthians_zone_id = Column(String, nullable=True)
    external_camp_id = Column(Integer, nullable=True)
    blood_collection_type = Column(
        SAEnum(BloodCollectionType, name="blood_collection_type_enum", values_callable=lambda obj: [e.value for e in obj], create_type=False),
        nullable=True,
    )
    consultation_mode = Column(
        SAEnum(ConsultationMode, name="consultation_mode_enum", values_callable=lambda obj: [e.value for e in obj], create_type=False),
        nullable=True,
    )


class OnboardingAssistantAssignment(Base):
    """SQLAlchemy model for `onboarding_assistant_assignment` table."""

    __tablename__ = "onboarding_assistant_assignment"

    onboarding_assistant_id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey("employee.employee_id"), nullable=False)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)

    __table_args__ = (
        UniqueConstraint("engagement_id", "employee_id", name="uq_onboarding_assistant_assignment"),
        Index("ix_onboarding_assistant_assignment_engagement_id", "engagement_id"),
        Index("ix_onboarding_assistant_assignment_employee_id", "employee_id"),
    )


class EngagementParticipant(Base):
    """SQLAlchemy model for `engagement_participants` table."""

    __tablename__ = "engagement_participants"
    __table_args__ = (
        UniqueConstraint("engagement_id", "user_id", name="uq_engagement_participants_engagement_user"),
        Index("ix_ep_engagement_id_user_id", "engagement_id", "user_id"),
        Index("ix_ep_user_id", "user_id"),
        Index("ix_ep_booked_by_user_id", "booked_by_user_id"),
        Index("ix_ep_engagement_date", "engagement_date"),
        Index("ix_engagement_participants_booking_id", "booking_id"),
        Index(
            "ix_ep_cabin_slot_occupancy",
            "engagement_id",
            "blood_collection_cabin",
            "engagement_date",
            "slot_start_time",
        ),
    )

    engagement_participant_id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    booked_by_user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    slot_start_time = Column(Time, nullable=True)
    engagement_date = Column(Date, nullable=True)
    participants_employee_id = Column(String, nullable=True)
    participant_department = Column(String, nullable=True)
    participant_blood_group = Column(String, nullable=True)
    consultation_booking_ids = Column(ARRAY(Integer), nullable=True)
    is_profile_created_on_metsights = Column(Boolean, nullable=False, default=False, server_default="false")
    is_primary_record_id_synced = Column(Boolean, nullable=False, default=False, server_default="false")
    is_fitprint_record_id_synced = Column(Boolean, nullable=False, default=False, server_default="false")
    barcode = Column(String, nullable=True)
    booking_id = Column(String, nullable=True)
    blood_collection_time_slot_id = Column(String, nullable=True)
    blood_collection_cabin = Column(String, nullable=True)
    face_scan_link = Column(String, nullable=True)
    address = Column(String, nullable=True)
    sub_locality = Column(String, nullable=True)
    landmark = Column(String, nullable=True)
    pincode = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    healthians_zone_id = Column(String, nullable=True)

    @validates("user_id")
    def _default_booked_by_user_id(self, _key: str, user_id: int) -> int:
        if self.booked_by_user_id is None and user_id is not None:
            self.booked_by_user_id = user_id
        return user_id


class EngagementType(Base):
    """SQLAlchemy model for `engagement_types` table."""

    __tablename__ = "engagement_types"

    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True, nullable=False)
    display_name = Column(String, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True, server_default="true")


class AutoNotificationEvent(Base):
    """SQLAlchemy model for `auto_notification_events` table."""

    __tablename__ = "auto_notification_events"

    id = Column(Integer, primary_key=True)
    engagement_type_ids = Column(ARRAY(Integer), nullable=False)
    event_code = Column(String, unique=True, nullable=False)
    display_name = Column(String, nullable=False)
    description = Column(Text, nullable=True)


class EngagementNotification(Base):
    """SQLAlchemy model for `engagement_notifications` table."""

    __tablename__ = "engagement_notifications"
    __table_args__ = (
        UniqueConstraint("engagement_id", "notification_event_id", name="uq_engagement_notification_event"),
    )

    id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    notification_event_id = Column(Integer, ForeignKey("auto_notification_events.id"), nullable=False)
    notification_services = Column(JSON, nullable=False)


class EngagementNotificationDefault(Base):
    """SQLAlchemy model for `engagement_notification_defaults` table."""

    __tablename__ = "engagement_notification_defaults"
    __table_args__ = (
        UniqueConstraint("engagement_type_id", "notification_event_id", name="uq_notification_default_type_event"),
    )

    id = Column(Integer, primary_key=True)
    engagement_type_id = Column(Integer, ForeignKey("engagement_types.id"), nullable=False)
    notification_event_id = Column(Integer, ForeignKey("auto_notification_events.id"), nullable=False)
    notification_services = Column(JSON, nullable=False)
