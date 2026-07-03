"""Reports module models.

Schema must match `instructions/db-schema.txt`.
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.types import JSON

from db.base import Base


class IndividualHealthReport(Base):
    """SQLAlchemy model for `individual_health_report` table."""

    __tablename__ = "individual_health_report"

    report_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    assessment_instance_id = Column(
        Integer,
        ForeignKey("assessment_instances.assessment_instance_id"),
        nullable=False,
    )
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    reports = Column(JSON)
    blood_parameters = Column(JSON)
    blood_report_raw = Column(JSON, nullable=True)
    report_url = Column(Text, nullable=True)
    diagnostic_report_url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class CampReport(Base):
    """SQLAlchemy model for `camp_reports` table."""

    __tablename__ = "camp_reports"

    report_id = Column(Integer, primary_key=True)
    report = Column(JSON)
    camp_no = Column(BigInteger, nullable=False, index=True)
    department = Column(String, nullable=True)
    organization_id = Column(Integer, ForeignKey("organizations.organization_id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class CampReportSection(Base):
    """SQLAlchemy model for `camp_report_sections` reference table."""

    __tablename__ = "camp_report_sections"

    report_sections = Column(Integer, primary_key=True)
    section = Column(String, nullable=False)
    section_key = Column(String, nullable=False, unique=True)
    description = Column(Text, nullable=True)


class ReportsUserSyncState(Base):
    """Tracks user-level reports sync state for non-blocking refresh."""

    __tablename__ = "reports_user_sync_state"

    sync_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False, unique=True)
    last_synced_assessment_instance_id = Column(Integer, nullable=True)
    sync_status = Column(String, nullable=False, server_default="idle")
    last_synced_at = Column(DateTime(timezone=True), nullable=True)
    last_sync_error = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
