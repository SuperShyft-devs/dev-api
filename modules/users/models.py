"""Users module models.

Auth depends on this module for read-only existence checks.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, JSON, String, func, text

from db.base import Base


class User(Base):
    """SQLAlchemy model for `users` table."""

    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    age = Column(Integer, nullable=False)
    phone = Column(String, nullable=False)
    email = Column(String)
    profile_photo = Column(String)
    date_of_birth = Column(Date, nullable=True)
    gender = Column(String)
    address = Column(String)
    pin_code = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    referred_by = Column(String)
    is_participant = Column(Boolean)
    status = Column(String)
    parent_id = Column(Integer, ForeignKey("users.user_id"), nullable=True)
    relationship = Column(String, nullable=False, server_default=text("'self'"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class UserPreference(Base):
    """SQLAlchemy model for `user_preferences` table."""

    __tablename__ = "user_preferences"

    preference_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False, unique=True)
    push_enabled = Column(Boolean, nullable=False, server_default=text("true"))
    email_enabled = Column(Boolean, nullable=False, server_default=text("true"))
    sms_enabled = Column(Boolean, nullable=False, server_default=text("false"))
    access_to_files = Column(Boolean, nullable=False, server_default=text("true"))
    store_downloaded_files = Column(Boolean, nullable=False, server_default=text("true"))
    diet_preference = Column(String, nullable=True)
    allergies = Column(JSON, nullable=True, server_default=text("'[]'"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
