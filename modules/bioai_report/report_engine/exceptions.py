"""Exceptions raised by the Bio-AI report content engine."""

from __future__ import annotations


class ReportEngineError(Exception):
    """Base error for the content assembly engine."""


class KnowledgeBaseError(ReportEngineError):
    """Raised when a disease KB file or score band cannot be resolved."""


class AssessmentDataError(ReportEngineError):
    """Raised when assessment JSON is missing required fields."""
