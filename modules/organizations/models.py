"""Organizations module models.

This module owns the `organizations` table.

Schema must match `instructions/db-schema.txt`.
"""

from __future__ import annotations

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, String, Text, func

from db.base import Base


class Industry(Base):
    """SQLAlchemy model for `industries` table."""

    __tablename__ = "industries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry_key = Column(String(100), nullable=False, unique=True)
    industry = Column(String(100), nullable=False)


class Organization(Base):
    """SQLAlchemy model for `organizations` table."""

    __tablename__ = "organizations"

    organization_id = Column(Integer, primary_key=True)
    name = Column(String)
    organization_type = Column(String)
    logo = Column(String)
    website_url = Column(String)
    address = Column(Text)
    pin_code = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)

    contact_person_user_id = Column(Integer, ForeignKey("users.user_id"))

    bd_employee_id = Column(Integer, ForeignKey("employee.employee_id"))
    departments = Column(JSON, nullable=True)
    industry_key = Column(String(100), ForeignKey("industries.industry_key"), nullable=True)
    status = Column(String)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_employee_id = Column(Integer, ForeignKey("employee.employee_id"))
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    updated_employee_id = Column(Integer, ForeignKey("employee.employee_id"))
