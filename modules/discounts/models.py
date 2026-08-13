"""SQLAlchemy models for the discount / coupon engine."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from db.base import Base


class DiscountCode(Base):
    __tablename__ = "discount_codes"

    discount_code_id = Column(Integer, primary_key=True)
    code = Column(String(64), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String(32), nullable=False, server_default=text("'draft'"))
    discount_type = Column(String(32), nullable=False)
    percent_value = Column(Numeric(5, 2), nullable=True)
    fixed_amount_paise = Column(Integer, nullable=True)
    max_discount_paise = Column(Integer, nullable=True)
    hard_ceiling_paise = Column(Integer, nullable=True)
    min_bill_paise = Column(Integer, nullable=True)
    combine_with_others = Column(Boolean, nullable=False, server_default=text("false"))
    auto_apply = Column(Boolean, nullable=False, server_default=text("false"))
    audience = Column(String(32), nullable=False, server_default=text("'everyone'"))
    first_purchase_only = Column(Boolean, nullable=False, server_default=text("false"))
    scope_mode = Column(String(32), nullable=False, server_default=text("'general'"))
    package_apply_mode = Column(String(32), nullable=False, server_default=text("'all'"))
    include_addons = Column(Boolean, nullable=False, server_default=text("true"))
    valid_from = Column(DateTime(timezone=True), nullable=True)
    valid_to = Column(DateTime(timezone=True), nullable=True)
    time_of_day_start = Column(String(8), nullable=True)
    time_of_day_end = Column(String(8), nullable=True)
    days_of_week = Column(JSON, nullable=True)
    camp_valid_days_after = Column(Integer, nullable=True)
    max_total_uses = Column(Integer, nullable=True)
    max_uses_per_user = Column(Integer, nullable=True)
    per_user_frequency = Column(String(16), nullable=False, server_default=text("'none'"))
    max_uses_per_camp = Column(Integer, nullable=True)
    max_uses_per_order = Column(Integer, nullable=True)
    max_total_discount_paise = Column(Integer, nullable=True)
    total_discount_given_paise = Column(Integer, nullable=False, server_default=text("0"))
    code_kind = Column(String(32), nullable=False, server_default=text("'shared'"))
    referral_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    min_price_protection = Column(Boolean, nullable=False, server_default=text("true"))
    created_by = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    updated_by = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    scopes = relationship("DiscountCodeScope", back_populates="discount_code", cascade="all, delete-orphan")
    packages = relationship("DiscountCodePackage", back_populates="discount_code", cascade="all, delete-orphan")
    cities = relationship("DiscountCodeCity", back_populates="discount_code", cascade="all, delete-orphan")
    allowlist = relationship("DiscountAllowlistUser", back_populates="discount_code", cascade="all, delete-orphan")
    instances = relationship("DiscountCodeInstance", back_populates="discount_code", cascade="all, delete-orphan")
    usages = relationship("DiscountUsage", back_populates="discount_code")


class DiscountCodeScope(Base):
    __tablename__ = "discount_code_scopes"
    __table_args__ = (
        UniqueConstraint("discount_code_id", "scope_type", "scope_key", name="uq_discount_code_scopes"),
    )

    id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    scope_type = Column(String(32), nullable=False)
    scope_key = Column(String(64), nullable=False)

    discount_code = relationship("DiscountCode", back_populates="scopes")


class DiscountCodePackage(Base):
    __tablename__ = "discount_code_packages"
    __table_args__ = (
        UniqueConstraint(
            "discount_code_id", "diagnostic_package_id", "mode", name="uq_discount_code_packages"
        ),
    )

    id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    diagnostic_package_id = Column(
        Integer, ForeignKey("diagnostic_package.diagnostic_package_id", ondelete="CASCADE"), nullable=False
    )
    mode = Column(String(16), nullable=False)

    discount_code = relationship("DiscountCode", back_populates="packages")


class DiscountCodeCity(Base):
    __tablename__ = "discount_code_cities"
    __table_args__ = (UniqueConstraint("discount_code_id", "city", name="uq_discount_code_cities"),)

    id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    city = Column(String(128), nullable=False)

    discount_code = relationship("DiscountCode", back_populates="cities")


class DiscountAllowlistUser(Base):
    __tablename__ = "discount_allowlist_users"

    id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    phone = Column(String(32), nullable=True)
    email = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    discount_code = relationship("DiscountCode", back_populates="allowlist")


class DiscountCodeInstance(Base):
    __tablename__ = "discount_code_instances"

    instance_id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    code = Column(String(64), nullable=False, unique=True)
    assigned_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    status = Column(String(32), nullable=False, server_default=text("'available'"))
    used_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    discount_code = relationship("DiscountCode", back_populates="instances")


class DiscountUsage(Base):
    __tablename__ = "discount_usages"

    usage_id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    instance_id = Column(
        Integer, ForeignKey("discount_code_instances.instance_id", ondelete="SET NULL"), nullable=True
    )
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    order_id = Column(Integer, ForeignKey("orders.order_id", ondelete="SET NULL"), nullable=True)
    booking_ids = Column(JSON, nullable=True)
    original_paise = Column(Integer, nullable=False)
    discount_paise = Column(Integer, nullable=False)
    final_paise = Column(Integer, nullable=False)
    organization_id = Column(Integer, nullable=True)
    camp_no = Column(String(64), nullable=True)
    engagement_id = Column(Integer, nullable=True)
    status = Column(String(32), nullable=False, server_default=text("'reserved'"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    discount_code = relationship("DiscountCode", back_populates="usages")


class DiscountCodeAudit(Base):
    __tablename__ = "discount_code_audit"

    audit_id = Column(Integer, primary_key=True)
    discount_code_id = Column(
        Integer, ForeignKey("discount_codes.discount_code_id", ondelete="CASCADE"), nullable=False
    )
    actor_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    action = Column(String(64), nullable=False)
    diff = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DiscountValidationAttempt(Base):
    __tablename__ = "discount_validation_attempts"

    attempt_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    client_ip = Column(String(64), nullable=True)
    code_submitted = Column(String(64), nullable=True)
    outcome = Column(String(32), nullable=False)
    endpoint = Column(String(128), nullable=True)
    detail = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
