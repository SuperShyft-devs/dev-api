"""Engagements module models."""

from __future__ import annotations

import enum

from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Enum as SAEnum, Float, ForeignKey, Index, Integer, String, Time, UniqueConstraint
from sqlalchemy.sql import func

from db.base import Base
from modules.engagements.constants import DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY


class EngagementKind(str, enum.Enum):
    """PostgreSQL enum `engagement_kind` / column `engagements.engagement_type`."""

    bio_ai = "bio_ai"
    diagnostic = "diagnostic"
    doctor = "doctor"
    nutritionist = "nutritionist"


class BloodCollectionType(str, enum.Enum):
    """PostgreSQL enum `blood_collection_type_enum`."""

    home_collection = "home_collection"
    camp_collection = "camp_collection"


class EngagementStatus(str, enum.Enum):
    """Application-level engagement status values."""

    draft = "draft"
    scheduled = "scheduled"
    running = "running"
    completed = "completed"
    cancelled = "cancelled"


_engagement_kind = SAEnum(
    EngagementKind,
    name="engagement_kind",
    native_enum=True,
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)


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
    engagement_type = Column(_engagement_kind, nullable=True)
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
    participant_count = Column(Integer)
    create_profile_on_metsights = Column(Boolean, nullable=False, default=False, server_default="false")
    enroll_for_fitprint_full = Column(Boolean, nullable=False, default=False, server_default="false")
    notification_service_key = Column(
        String,
        ForeignKey("notification_services.service_key"),
        nullable=False,
        default=DEFAULT_ENGAGEMENT_NOTIFICATION_SERVICE_KEY,
        server_default="booking-alert-whatsapp",
    )
    pretest_guidelines_notification = Column(String, nullable=True)
    questionnaire_reminder_1 = Column(String, nullable=True)
    questionnaire_reminder_2 = Column(String, nullable=True)
    blood_report_notification = Column(String, nullable=True)
    bioai_report_notification = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    healthians_zone_id = Column(String, nullable=True)
    blood_collection_type = Column(
        SAEnum(BloodCollectionType, name="blood_collection_type_enum", values_callable=lambda obj: [e.value for e in obj], create_type=False),
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
        Index("ix_ep_engagement_id_user_id", "engagement_id", "user_id"),
        Index("ix_ep_user_id", "user_id"),
        Index("ix_ep_engagement_date", "engagement_date"),
        Index("ix_engagement_participants_booking_id", "booking_id"),
    )

    engagement_participant_id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    slot_start_time = Column(Time, nullable=False)
    engagement_date = Column(Date, nullable=False)
    participants_employee_id = Column(String, nullable=True)
    participant_department = Column(String, nullable=True)
    participant_blood_group = Column(String, nullable=True)
    want_doctor_consultation = Column(Boolean, nullable=True)
    want_nutritionist_consultation = Column(Boolean, nullable=True)
    want_doctor_and_nutritionist_consultation = Column(Boolean, nullable=True)
    is_profile_created_on_metsights = Column(Boolean, nullable=False, default=False, server_default="false")
    is_primary_record_id_synced = Column(Boolean, nullable=False, default=False, server_default="false")
    is_fitprint_record_id_synced = Column(Boolean, nullable=False, default=False, server_default="false")
    barcode = Column(String, nullable=True)
    booking_id = Column(String, nullable=True)
    blood_collection_time_slot_id = Column(String, nullable=True)
