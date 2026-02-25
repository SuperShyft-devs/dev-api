"""Diagnostics module models.

This module owns the `diagnostic_package` table.

Rules:
- This table stores metadata only.
- This module must not implement medical logic.
- JSON columns store external data as received.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, String, func
from sqlalchemy.types import JSON

from db.base import Base


class DiagnosticPackage(Base):
    """SQLAlchemy model for `diagnostic_package` table."""

    __tablename__ = "diagnostic_package"

    diagnostic_package_id = Column(Integer, primary_key=True)
    reference_id = Column(String)
    package_name = Column(String)
    diagnostic_provider = Column(String)
    package_info = Column(JSON)
    no_of_tests = Column(Integer)
    status = Column(String)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
