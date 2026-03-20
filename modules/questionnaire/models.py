"""Questionnaire module models.

This module owns the `questionnaire_definitions` table.
"""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, func, text
from sqlalchemy.types import JSON

from db.base import Base


class QuestionnaireDefinition(Base):
    """SQLAlchemy model for `questionnaire_definitions` table."""

    __tablename__ = "questionnaire_definitions"

    question_id = Column(Integer, primary_key=True)
    question_key = Column(String, nullable=False, unique=True)
    question_text = Column(Text, nullable=False)
    question_type = Column(String, nullable=False)
    is_required = Column(Boolean, nullable=False, server_default=text("false"))
    is_read_only = Column(Boolean, nullable=False, server_default=text("false"))
    help_text = Column(Text, nullable=True)
    visibility_rules = Column(JSON, nullable=True)
    prefill_from = Column(JSON, nullable=True)
    status = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class QuestionnaireOption(Base):
    """SQLAlchemy model for `questionnaire_options` table."""

    __tablename__ = "questionnaire_options"

    option_id = Column(Integer, primary_key=True)
    question_id = Column(Integer, ForeignKey("questionnaire_definitions.question_id"), nullable=False)
    option_value = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    tooltip_text = Column(Text, nullable=True)


class QuestionnaireCategory(Base):
    """SQLAlchemy model for `questionnaire_categories` table."""

    __tablename__ = "questionnaire_categories"

    category_id = Column(Integer, primary_key=True)
    category_key = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    status = Column(String, nullable=False, server_default=text("'active'"))


class QuestionnaireCategoryQuestion(Base):
    """SQLAlchemy model for `questionnaire_category_questions` table."""

    __tablename__ = "questionnaire_category_questions"

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("questionnaire_categories.category_id"), nullable=False)
    question_id = Column(Integer, ForeignKey("questionnaire_definitions.question_id"), nullable=False)
    display_order = Column(Integer, nullable=True)


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
    category_id = Column(
        Integer,
        ForeignKey("questionnaire_categories.category_id"),
        nullable=False,
    )
    answer = Column(JSON)
    submitted_at = Column(DateTime(timezone=True))
