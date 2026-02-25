"""Users module models.

Auth depends on this module for read-only existence checks.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, func

from db.base import Base


class User(Base):
    """SQLAlchemy model for `users` table."""

    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    phone = Column(String, nullable=False)
    email = Column(String)
    date_of_birth = Column(Date)
    gender = Column(String)
    address = Column(String)
    pin_code = Column(String)
    city = Column(String)
    state = Column(String)
    country = Column(String)
    referred_by = Column(String)
    is_participant = Column(Boolean)
    status = Column(String)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
