"""Employee module models.

This module owns the `employee` table.
"""

from __future__ import annotations

import enum

from sqlalchemy import Boolean, CheckConstraint, Column, DateTime, Enum as SAEnum, ForeignKey, Index, Integer, String, func

from db.base import Base


class EmployeeRole(str, enum.Enum):
    """PostgreSQL enum `employee_role` / column `employee.role`."""

    admin = "admin"
    inferior_admin = "inferior_admin"
    onboarding_assistant = "onboarding_assistant"
    organization_manager = "organization_manager"
    expert = "expert"


_employee_role = SAEnum(
    EmployeeRole,
    name="employee_role",
    native_enum=True,
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)


class Employee(Base):
    """SQLAlchemy model for `employee` table."""

    __tablename__ = "employee"
    __table_args__ = (Index("ix_employee_user_id", "user_id"),)

    employee_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False, unique=True)
    role = Column(_employee_role, nullable=False)
    status = Column(String, nullable=False)
    permissions_version = Column(Integer, nullable=False, default=1, server_default="1")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class PermissionCategory(Base):
    """Fixed catalog of delegable admin areas."""

    __tablename__ = "permission_categories"

    category_key = Column(String(50), primary_key=True)
    display_name = Column(String(100), nullable=False)
    description = Column(String(500), nullable=False)
    display_order = Column(Integer, nullable=False, unique=True)
    is_active = Column(Boolean, nullable=False, default=True, server_default="true")


class EmployeeCategoryPermission(Base):
    """Normalized View/Edit grant for an Inferior Admin."""

    __tablename__ = "employee_category_permissions"
    __table_args__ = (
        CheckConstraint("NOT can_edit OR can_view", name="ck_employee_category_permissions_edit_requires_view"),
        Index("ix_employee_category_permissions_employee_id", "employee_id"),
    )

    employee_id = Column(
        Integer,
        ForeignKey("employee.employee_id", ondelete="CASCADE"),
        primary_key=True,
    )
    category_key = Column(
        String(50),
        ForeignKey("permission_categories.category_key", ondelete="RESTRICT"),
        primary_key=True,
    )
    can_view = Column(Boolean, nullable=False, default=False, server_default="false")
    can_edit = Column(Boolean, nullable=False, default=False, server_default="false")
    granted_by_employee_id = Column(
        Integer,
        ForeignKey("employee.employee_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class EmployeeTaskPermission(Base):
    """Optional task-level overrides within an Inferior Admin category."""

    __tablename__ = "employee_task_permissions"
    __table_args__ = (
        CheckConstraint("NOT can_edit OR can_view", name="ck_employee_task_permissions_edit_requires_view"),
        Index("ix_employee_task_permissions_employee_id", "employee_id"),
    )

    employee_id = Column(
        Integer,
        ForeignKey("employee.employee_id", ondelete="CASCADE"),
        primary_key=True,
    )
    category_key = Column(
        String(50),
        ForeignKey("permission_categories.category_key", ondelete="RESTRICT"),
        primary_key=True,
    )
    task_key = Column(String(80), primary_key=True)
    can_view = Column(Boolean, nullable=False, default=False, server_default="false")
    can_edit = Column(Boolean, nullable=False, default=False, server_default="false")
    granted_by_employee_id = Column(
        Integer,
        ForeignKey("employee.employee_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
