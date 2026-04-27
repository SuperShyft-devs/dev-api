"""Diagnostics module SQLAlchemy models."""

from __future__ import annotations

import enum

from sqlalchemy import Boolean, Column, DateTime, Enum, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.types import JSON
from sqlalchemy.orm import relationship

from db.base import Base


class ParameterType(str, enum.Enum):
    TEST = "test"
    METRIC = "metric"


class DiagnosticPackage(Base):
    """SQLAlchemy model for `diagnostic_package` table."""

    __tablename__ = "diagnostic_package"

    diagnostic_package_id = Column(Integer, primary_key=True)
    reference_id = Column(String)
    package_name = Column(String, nullable=False)
    diagnostic_provider = Column(String)
    created_by_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    report_duration_hours = Column(Integer)
    collection_type = Column(String)
    about_text = Column(Text)
    bookings_count = Column(Integer, nullable=False, default=0, server_default="0")
    price = Column(Numeric(10, 2))
    original_price = Column(Numeric(10, 2))
    is_most_popular = Column(Boolean, nullable=False, default=False, server_default="false")
    gender_suitability = Column(String)
    status = Column(String, default="active", server_default="active")
    package_for = Column(String, nullable=False, default="public", server_default="public")
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
    filter_chip_links = relationship(
        "DiagnosticPackageFilterChipLink",
        back_populates="package",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DiagnosticPackageFilterChip(Base):
    """SQLAlchemy model for `diagnostic_package_filters_chips` catalog table."""

    __tablename__ = "diagnostic_package_filters_chips"

    filter_chip_id = Column(Integer, primary_key=True)
    chip_key = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    display_order = Column(Integer)
    chip_for = Column(String, nullable=False, default="public_package", server_default="public_package")
    status = Column(String, default="active", server_default="active")

    package_links = relationship(
        "DiagnosticPackageFilterChipLink",
        back_populates="filter_chip",
        passive_deletes=True,
    )


class DiagnosticPackageFilterChipLink(Base):
    """Junction: filter chips on packages or test groups (exactly one target)."""

    __tablename__ = "diagnostic_package_filter_chip_links"

    link_id = Column(Integer, primary_key=True)
    diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"),
        nullable=True,
    )
    group_id = Column(
        Integer,
        ForeignKey("diagnostic_test_groups.group_id", ondelete="CASCADE"),
        nullable=True,
    )
    filter_chip_id = Column(
        Integer,
        ForeignKey("diagnostic_package_filters_chips.filter_chip_id", ondelete="CASCADE"),
        nullable=False,
    )
    display_order = Column(Integer)

    package = relationship("DiagnosticPackage", back_populates="filter_chip_links")
    group = relationship("DiagnosticTestGroup", back_populates="filter_chip_links")
    filter_chip = relationship("DiagnosticPackageFilterChip", back_populates="package_links")


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
    price = Column(Numeric(10, 2))
    original_price = Column(Numeric(10, 2))
    is_most_popular = Column(Boolean, nullable=False, default=False, server_default="false")
    gender_suitability = Column(String)
    package_for = Column(String, nullable=False, default="public", server_default="public")

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
    filter_chip_links = relationship(
        "DiagnosticPackageFilterChipLink",
        back_populates="group",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class HealthParameter(Base):
    """SQLAlchemy model for `health_parameters` table."""

    __tablename__ = "health_parameters"

    test_id = Column(Integer, primary_key=True)
    parameter_type = Column(
        Enum(
            ParameterType,
            name="health_parameter_type",
            create_constraint=True,
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=ParameterType.TEST,
        server_default=text("'test'::health_parameter_type"),
    )
    test_name = Column(String, nullable=False)
    # Maps to the corresponding key in `blood_parameters` JSON (e.g. "haemoglobin").
    parameter_key = Column(String, nullable=True)
    unit = Column(String, nullable=True)
    meaning = Column(Text, nullable=True)
    is_available = Column(Boolean, nullable=False, default=True, server_default="true")
    display_order = Column(Integer)
    price = Column(Numeric(10, 2))
    original_price = Column(Numeric(10, 2))
    is_most_popular = Column(Boolean, nullable=False, default=False, server_default="false")
    gender_suitability = Column(String)

    low_risk_lower_range_male = Column(Numeric(12, 4), nullable=True)
    low_risk_higher_range_male = Column(Numeric(12, 4), nullable=True)
    moderate_risk_lower_range_male = Column(Numeric(12, 4), nullable=True)
    moderate_risk_higher_range_male = Column(Numeric(12, 4), nullable=True)
    high_risk_lower_range_male = Column(Numeric(12, 4), nullable=True)
    high_risk_higher_range_male = Column(Numeric(12, 4), nullable=True)

    low_risk_lower_range_female = Column(Numeric(12, 4), nullable=True)
    low_risk_higher_range_female = Column(Numeric(12, 4), nullable=True)
    moderate_risk_lower_range_female = Column(Numeric(12, 4), nullable=True)
    moderate_risk_higher_range_female = Column(Numeric(12, 4), nullable=True)
    high_risk_lower_range_female = Column(Numeric(12, 4), nullable=True)
    high_risk_higher_range_female = Column(Numeric(12, 4), nullable=True)

    causes_when_high = Column(Text, nullable=True)
    causes_when_low = Column(Text, nullable=True)
    effects_when_high = Column(Text, nullable=True)
    effects_when_low = Column(Text, nullable=True)
    what_to_do_when_low = Column(Text, nullable=True)
    what_to_do_when_high = Column(Text, nullable=True)

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
    test_id = Column(Integer, ForeignKey("health_parameters.test_id", ondelete="CASCADE"), nullable=False)
    display_order = Column(Integer)

    group = relationship("DiagnosticTestGroup", back_populates="tests")
    test = relationship("HealthParameter", back_populates="group_assignments")


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
