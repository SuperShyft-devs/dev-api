"""Questionnaire request/response schemas."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


def _strip(value: str | None) -> str:
    return (value or "").strip()


class QuestionnaireQuestionCreateRequest(BaseModel):
    question_text: str = Field(..., min_length=1, max_length=2000)
    question_type: str = Field(..., min_length=1, max_length=50)
    options: Optional[list[str]] = Field(default=None)
    status: Optional[str] = Field(default="active", min_length=1, max_length=20)

    def normalized_question_text(self) -> str:
        return _strip(self.question_text)

    def normalized_question_type(self) -> str:
        return _strip(self.question_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class QuestionnaireQuestionUpdateRequest(BaseModel):
    question_text: str = Field(..., min_length=1, max_length=2000)
    question_type: str = Field(..., min_length=1, max_length=50)
    options: Optional[list[str]] = Field(default=None)

    def normalized_question_text(self) -> str:
        return _strip(self.question_text)

    def normalized_question_type(self) -> str:
        return _strip(self.question_type).lower()


class QuestionnaireQuestionStatusUpdateRequest(BaseModel):
    status: str = Field(..., min_length=1, max_length=20)

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class QuestionnaireQuestionResponse(BaseModel):
    question_id: int
    question_text: str
    question_type: str
    options: Any | None
    status: str
    created_at: Any


# User-facing schemas for questionnaire responses

class QuestionnaireQuestionWithAnswer(BaseModel):
    """Question with optional draft answer for user display."""
    question_id: int
    question_text: str
    question_type: str
    options: Any | None
    answer: Any | None


class QuestionnaireGetResponse(BaseModel):
    """Response for GET /questionnaires/{assessment_instance_id}."""
    assessment_instance_id: int
    status: str
    questions: list[QuestionnaireQuestionWithAnswer]


class ResponseItem(BaseModel):
    """Single question-answer pair for upsert."""
    question_id: int
    answer: Any

    def normalized_answer(self) -> Any:
        """Return the answer as-is. Validation happens in service layer."""
        return self.answer


class QuestionnaireResponsesUpsertRequest(BaseModel):
    """Request for PUT /questionnaires/{assessment_instance_id}/responses."""
    responses: list[ResponseItem] = Field(..., min_length=1, max_length=500)
