"""Platform-wide configuration (singleton row)."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, Enum as SAEnum, ForeignKey, Integer, String, func

from db.base import Base
from modules.engagements.models import BloodCollectionType, EngagementKind


_engagement_kind = SAEnum(
    EngagementKind,
    name="engagement_kind",
    native_enum=True,
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)

_blood_collection_type = SAEnum(
    BloodCollectionType,
    name="blood_collection_type_enum",
    native_enum=True,
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)


class PlatformSettings(Base):
    """Single-row table for platform defaults."""

    __tablename__ = "platform_settings"

    settings_id = Column(Integer, primary_key=True)
    b2c_default_assessment_package_id = Column(
        Integer,
        ForeignKey("assessment_packages.package_id"),
        nullable=False,
    )
    b2c_default_diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id"),
        nullable=False,
    )
    b2c_default_engagement_type = Column(
        _engagement_kind,
        nullable=False,
        default=EngagementKind.bio_ai,
        server_default=EngagementKind.bio_ai.value,
    )
    b2c_default_blood_collection_type = Column(_blood_collection_type, nullable=True)
    b2c_default_create_profile_on_metsights = Column(Boolean, nullable=False, default=True, server_default="true")
    b2c_default_enroll_for_fitprint_full = Column(Boolean, nullable=False, default=False, server_default="false")
    default_onboarding_notification = Column(String(500), nullable=True)
    default_pretest_guidelines_notification = Column(String(500), nullable=True)
    default_questionnaire_reminder_1 = Column(String(500), nullable=True)
    default_questionnaire_reminder_2 = Column(String(500), nullable=True)
    default_blood_report_notification = Column(String(500), nullable=True)
    default_bioai_report_notification = Column(String(500), nullable=True)
    default_onboarding_assistant_employee_ids = Column(String(500), nullable=True)
    default_support_query_notification = Column(String(500), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
