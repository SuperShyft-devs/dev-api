"""Assessments module models.

Schema must match `instructions/db-schema.txt`.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String

from db.base import Base


class AssessmentPackage(Base):
    """SQLAlchemy model for `assessment_packages` table."""

    __tablename__ = "assessment_packages"

    package_id = Column(Integer, primary_key=True)
    package_code = Column(String)
    display_name = Column(String)
    assessment_type_code = Column(String, nullable=True)
    status = Column(String)


class AssessmentInstance(Base):
    """SQLAlchemy model for `assessment_instances` table."""

    __tablename__ = "assessment_instances"

    assessment_instance_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    package_id = Column(Integer, ForeignKey("assessment_packages.package_id"), nullable=False)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    status = Column(String)
    metsights_record_id = Column(String, nullable=True)
    assigned_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True), nullable=True)


class AssessmentPackageCategory(Base):
    """SQLAlchemy model for `assessment_package_categories` table."""

    __tablename__ = "assessment_package_categories"

    id = Column(Integer, primary_key=True)
    package_id = Column(Integer, ForeignKey("assessment_packages.package_id"), nullable=False)
    category_id = Column(Integer, ForeignKey("questionnaire_categories.category_id"), nullable=False)
    display_order = Column(Integer, nullable=True)


class AssessmentCategoryProgress(Base):
    """SQLAlchemy model for `assessment_category_progress` table."""

    __tablename__ = "assessment_category_progress"

    id = Column(Integer, primary_key=True)
    assessment_instance_id = Column(
        Integer,
        ForeignKey("assessment_instances.assessment_instance_id"),
        nullable=False,
    )
    category_id = Column(Integer, ForeignKey("questionnaire_categories.category_id"), nullable=False)
    status = Column(String, nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)
