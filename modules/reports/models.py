"""Reports module models.

Schema must match `instructions/db-schema.txt`.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, func
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
    metsights_output = Column(JSON)
    diagnostics_output = Column(JSON)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class OrganizationHealthReport(Base):
    """SQLAlchemy model for `organization_health_report` table."""

    __tablename__ = "organization_health_report"

    report_id = Column(Integer, primary_key=True)
    metsights_output = Column(JSON)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    organization_id = Column(Integer, ForeignKey("organizations.organization_id"), nullable=False)
