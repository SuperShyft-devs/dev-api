"""Employee module models.

This module owns the `employee` table.
"""

from __future__ import annotations

import enum

from sqlalchemy import Column, DateTime, Enum as SAEnum, ForeignKey, Index, Integer, String, func

from db.base import Base


class EmployeeRole(str, enum.Enum):
    """PostgreSQL enum `employee_role` / column `employee.role`."""

    admin = "admin"
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
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    role = Column(_employee_role, nullable=False)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
