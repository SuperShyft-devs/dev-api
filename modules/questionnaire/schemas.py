"""Questionnaire request/response schemas."""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


def _strip(value: str | None) -> str:
    return (value or "").strip()


class QuestionnaireQuestionCreateRequest(BaseModel):
    question_key: str = Field(..., min_length=1, max_length=100)
    question_text: str = Field(..., min_length=1, max_length=2000)
    question_type: str = Field(..., min_length=1, max_length=50)
    category_id: int | None = None
    is_required: bool = False
    is_read_only: bool = False
    help_text: Optional[str] = Field(default=None, max_length=2000)
    options: Optional[list[dict[str, str | None]]] = Field(default=None)
    status: Optional[str] = Field(default="active", min_length=1, max_length=20)

    def normalized_question_key(self) -> str:
        return _strip(self.question_key).lower()

    def normalized_question_text(self) -> str:
        return _strip(self.question_text)

    def normalized_question_type(self) -> str:
        return _strip(self.question_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class QuestionnaireQuestionUpdateRequest(BaseModel):
    question_key: str = Field(..., min_length=1, max_length=100)
    question_text: str = Field(..., min_length=1, max_length=2000)
    question_type: str = Field(..., min_length=1, max_length=50)
    category_id: int | None = None
    is_required: bool = False
    is_read_only: bool = False
    help_text: Optional[str] = Field(default=None, max_length=2000)
    options: Optional[list[dict[str, str | None]]] = Field(default=None)

    def normalized_question_key(self) -> str:
        return _strip(self.question_key).lower()

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
    question_key: str | None
    question_text: str
    question_type: str
    category_id: int | None
    is_required: bool
    is_read_only: bool
    help_text: str | None
    options: Any | None
    status: str
    created_at: Any


# User-facing schemas for questionnaire responses

class QuestionnaireQuestionWithAnswer(BaseModel):
    """Question with optional draft answer for user display."""
    question_key: str | None
    question_id: int
    question_text: str
    question_type: str
    category_id: int | None
    is_required: bool
    is_read_only: bool
    help_text: str | None
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


class QuestionnaireCategoryCreateRequest(BaseModel):
    category_key: str = Field(..., min_length=1, max_length=100)
    display_name: str = Field(..., min_length=1, max_length=200)

    def normalized_category_key(self) -> str:
        return _strip(self.category_key).lower()

    def normalized_display_name(self) -> str:
        return _strip(self.display_name)


class QuestionnaireCategoryUpdateRequest(BaseModel):
    category_key: str = Field(..., min_length=1, max_length=100)
    display_name: str = Field(..., min_length=1, max_length=200)

    def normalized_category_key(self) -> str:
        return _strip(self.category_key).lower()

    def normalized_display_name(self) -> str:
        return _strip(self.display_name)


class QuestionnaireCategoryQuestionsAssignRequest(BaseModel):
    question_ids: list[int] = Field(..., min_length=1)
