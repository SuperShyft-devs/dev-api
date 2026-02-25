"""Questionnaire module models.

This module owns the `questionnaire_definitions` table.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.types import JSON

from db.base import Base


class QuestionnaireDefinition(Base):
    """SQLAlchemy model for `questionnaire_definitions` table."""

    __tablename__ = "questionnaire_definitions"

    question_id = Column(Integer, primary_key=True)
    question_text = Column(Text, nullable=False)
    question_type = Column(String, nullable=False)
    options = Column(JSON, nullable=True)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class QuestionnaireResponse(Base):
    """SQLAlchemy model for `questionnaire_responses` table."""

    __tablename__ = "questionnaire_responses"

    response_id = Column(Integer, primary_key=True)
    assessment_instance_id = Column(
        Integer,
        ForeignKey("assessment_instances.assessment_instance_id"),
        nullable=False,
    )
    question_id = Column(
        Integer,
        ForeignKey("questionnaire_definitions.question_id"),
        nullable=False,
    )
    answer = Column(JSON)
    submitted_at = Column(DateTime(timezone=True))
