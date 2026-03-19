"""Diagnostics module SQLAlchemy models."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.types import JSON
from sqlalchemy.orm import relationship

from db.base import Base


class DiagnosticPackage(Base):
    """SQLAlchemy model for `diagnostic_package` table."""

    __tablename__ = "diagnostic_package"

    diagnostic_package_id = Column(Integer, primary_key=True)
    reference_id = Column(String)
    package_name = Column(String, nullable=False)
    diagnostic_provider = Column(String)
    no_of_tests = Column(Integer)
    report_duration_hours = Column(Integer)
    collection_type = Column(String)
    about_text = Column(Text)
    bookings_count = Column(Integer, nullable=False, default=0, server_default="0")
    price = Column(Numeric(10, 2))
    original_price = Column(Numeric(10, 2))
    is_most_popular = Column(Boolean, nullable=False, default=False, server_default="false")
    gender_suitability = Column(String)
    status = Column(String, default="active", server_default="active")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    reasons = relationship(
        "DiagnosticPackageReason",
        back_populates="package",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    tags = relationship(
        "DiagnosticPackageTag",
        back_populates="package",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    test_group_assignments = relationship(
        "DiagnosticPackageTestGroup",
        back_populates="package",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    samples = relationship(
        "DiagnosticPackageSample",
        back_populates="package",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    preparations = relationship(
        "DiagnosticPackagePreparation",
        back_populates="package",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DiagnosticPackageFilter(Base):
    """SQLAlchemy model for `diagnostic_package_filters` table."""

    __tablename__ = "diagnostic_package_filters"

    filter_id = Column(Integer, primary_key=True)
    filter_key = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    display_order = Column(Integer)
    filter_type = Column(String)
    status = Column(String, default="active", server_default="active")


class DiagnosticPackageReason(Base):
    """SQLAlchemy model for `diagnostic_package_reasons` table."""

    __tablename__ = "diagnostic_package_reasons"

    reason_id = Column(Integer, primary_key=True)
    diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"),
        nullable=False,
    )
    display_order = Column(Integer)
    reason_text = Column(Text, nullable=False)

    package = relationship("DiagnosticPackage", back_populates="reasons")


class DiagnosticPackageTag(Base):
    """SQLAlchemy model for `diagnostic_package_tags` table."""

    __tablename__ = "diagnostic_package_tags"

    tag_id = Column(Integer, primary_key=True)
    diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"),
        nullable=False,
    )
    tag_name = Column(String, nullable=False)
    display_order = Column(Integer)

    package = relationship("DiagnosticPackage", back_populates="tags")


class DiagnosticTestGroup(Base):
    """SQLAlchemy model for `diagnostic_test_groups` table."""

    __tablename__ = "diagnostic_test_groups"

    group_id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    display_order = Column(Integer)

    tests = relationship(
        "DiagnosticTestGroupTest",
        back_populates="group",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    package_assignments = relationship(
        "DiagnosticPackageTestGroup",
        back_populates="group",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DiagnosticTest(Base):
    """SQLAlchemy model for `diagnostic_tests` table."""

    __tablename__ = "diagnostic_tests"

    test_id = Column(Integer, primary_key=True)
    test_name = Column(String, nullable=False)
    is_available = Column(Boolean, nullable=False, default=True, server_default="true")
    display_order = Column(Integer)

    group_assignments = relationship(
        "DiagnosticTestGroupTest",
        back_populates="test",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DiagnosticTestGroupTest(Base):
    """SQLAlchemy model for `diagnostic_test_group_tests` table."""

    __tablename__ = "diagnostic_test_group_tests"
    __table_args__ = (UniqueConstraint("group_id", "test_id", name="uq_diagnostic_test_group_tests_group_test"),)

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey("diagnostic_test_groups.group_id", ondelete="CASCADE"), nullable=False)
    test_id = Column(Integer, ForeignKey("diagnostic_tests.test_id", ondelete="CASCADE"), nullable=False)
    display_order = Column(Integer)

    group = relationship("DiagnosticTestGroup", back_populates="tests")
    test = relationship("DiagnosticTest", back_populates="group_assignments")


class DiagnosticPackageTestGroup(Base):
    """SQLAlchemy model for `diagnostic_package_test_groups` table."""

    __tablename__ = "diagnostic_package_test_groups"
    __table_args__ = (
        UniqueConstraint(
            "diagnostic_package_id",
            "group_id",
            name="uq_diagnostic_package_test_groups_package_group",
        ),
    )

    id = Column(Integer, primary_key=True)
    diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"),
        nullable=False,
    )
    group_id = Column(Integer, ForeignKey("diagnostic_test_groups.group_id", ondelete="CASCADE"), nullable=False)
    display_order = Column(Integer)

    package = relationship("DiagnosticPackage", back_populates="test_group_assignments")
    group = relationship("DiagnosticTestGroup", back_populates="package_assignments")


class DiagnosticPackageSample(Base):
    """SQLAlchemy model for `diagnostic_package_samples` table."""

    __tablename__ = "diagnostic_package_samples"

    sample_id = Column(Integer, primary_key=True)
    diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"),
        nullable=False,
    )
    sample_type = Column(String, nullable=False)
    description = Column(Text)
    display_order = Column(Integer)

    package = relationship("DiagnosticPackage", back_populates="samples")


class DiagnosticPackagePreparation(Base):
    """SQLAlchemy model for `diagnostic_package_preparations` table."""

    __tablename__ = "diagnostic_package_preparations"

    preparation_id = Column(Integer, primary_key=True)
    diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"),
        nullable=False,
    )
    preparation_title = Column(String, nullable=False)
    steps = Column(JSON)
    display_order = Column(Integer)

    package = relationship("DiagnosticPackage", back_populates="preparations")
