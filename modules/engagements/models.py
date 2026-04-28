"""Engagements module models."""

from __future__ import annotations

import enum

from sqlalchemy import Boolean, Column, Date, Enum as SAEnum, ForeignKey, Integer, String, Time, UniqueConstraint, Index

from db.base import Base


class EngagementKind(str, enum.Enum):
    """PostgreSQL enum `engagement_kind` / column `engagements.engagement_type`."""

    bio_ai = "bio_ai"
    diagnostic = "diagnostic"
    doctor = "doctor"
    nutritionist = "nutritionist"


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

    engagement_id = Column(Integer, primary_key=True)
    engagement_name = Column(String)
    metsights_engagement_id = Column(String, nullable=True)
    organization_id = Column(Integer, ForeignKey("organizations.organization_id"), nullable=True)
    engagement_code = Column(String, nullable=False)
    engagement_type = Column(_engagement_kind, nullable=True)
    assessment_package_id = Column(Integer, ForeignKey("assessment_packages.package_id"), nullable=True)
    diagnostic_package_id = Column(Integer, ForeignKey("diagnostic_package.diagnostic_package_id"), nullable=True)
    address = Column(String, nullable=True)
    pincode = Column(String, nullable=True)
    city = Column(String)
    slot_duration = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    status = Column(String)
    participant_count = Column(Integer)


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
    is_metsights_profile_created = Column(Boolean, nullable=False, default=False, server_default="false")
