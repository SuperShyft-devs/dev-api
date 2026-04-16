"""SQLAlchemy models for experts tables."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, Numeric, String, Text, func
from sqlalchemy.orm import relationship

from db.base import Base


class Expert(Base):
    __tablename__ = "experts"

    expert_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL"), nullable=True)
    expert_type = Column(String, nullable=False)
    specialization = Column(String, nullable=False)
    profile_photo = Column(String, nullable=True)
    rating = Column(Numeric(3, 2), nullable=False, server_default="0")
    review_count = Column(Integer, nullable=False, server_default="0")
    patient_count = Column(Integer, nullable=False, server_default="0")
    experience_years = Column(Integer, nullable=True)
    qualifications = Column(String, nullable=True)
    about_text = Column(Text, nullable=True)
    consultation_modes = Column(JSON, nullable=True)
    languages = Column(JSON, nullable=True)
    session_duration_mins = Column(Integer, nullable=True)
    appointment_fee_paise = Column(Integer, nullable=True)
    original_fee_paise = Column(Integer, nullable=True)
    status = Column(String, nullable=False, server_default="active")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    expertise_tags = relationship("ExpertExpertiseTag", back_populates="expert", cascade="all, delete-orphan")


class ExpertExpertiseTag(Base):
    __tablename__ = "expert_expertise_tags"

    tag_id = Column(Integer, primary_key=True, autoincrement=True)
    expert_id = Column(Integer, ForeignKey("experts.expert_id", ondelete="CASCADE"), nullable=False)
    tag_name = Column(String, nullable=False)
    display_order = Column(Integer, nullable=True)

    expert = relationship("Expert", back_populates="expertise_tags")


class ExpertReview(Base):
    __tablename__ = "expert_reviews"

    review_id = Column(Integer, primary_key=True, autoincrement=True)
    expert_id = Column(Integer, ForeignKey("experts.expert_id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    rating = Column(Numeric(2, 1), nullable=False)
    review_text = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
