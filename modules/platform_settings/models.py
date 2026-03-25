"""Platform-wide configuration (singleton row)."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, func

from db.base import Base


class PlatformSettings(Base):
    """Single-row table for platform defaults."""

    __tablename__ = "platform_settings"

    settings_id = Column(Integer, primary_key=True)
    b2c_default_assessment_package_id = Column(
        Integer,
        ForeignKey("assessment_packages.package_id"),
        nullable=False,
    )
    b2c_default_diagnostic_package_id = Column(
        Integer,
        ForeignKey("diagnostic_package.diagnostic_package_id"),
        nullable=False,
    )
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    updated_by_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
