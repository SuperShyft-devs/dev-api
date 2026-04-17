"""Questionnaire request/response schemas."""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


def _strip(value: str | None) -> str:
    return (value or "").strip()


class QuestionnaireQuestionCreateRequest(BaseModel):
    question_key: str = Field(..., min_length=1, max_length=100)
    question_text: str = Field(..., min_length=1, max_length=2000)
    question_type: str = Field(..., min_length=1, max_length=50)
    is_required: bool = False
    is_read_only: bool = False
    help_text: Optional[str] = Field(default=None, max_length=2000)
    options: Optional[list[dict[str, str | None]]] = Field(default=None)
    visibility_rules: Optional[dict[str, Any]] = Field(default=None)
    prefill_from: Optional[dict[str, Any]] = Field(default=None)
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
    is_required: bool = False
    is_read_only: bool = False
    help_text: Optional[str] = Field(default=None, max_length=2000)
    options: Optional[list[dict[str, str | None]]] = Field(default=None)
    visibility_rules: Optional[dict[str, Any]] = Field(default=None)
    prefill_from: Optional[dict[str, Any]] = Field(default=None)

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
    is_required: bool
    is_read_only: bool
    help_text: str | None
    options: Any | None
    visibility_rules: dict[str, Any] | None
    prefill_from: dict[str, Any] | None
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
    visibility_rules: dict[str, Any] | None
    prefill_from: dict[str, Any] | None
    is_visible: bool = True
    visibility_reason: str | None = None
    answer_source: Literal["draft", "prefill", "none"] = "none"
    answer: Any | None


class QuestionnaireGetResponse(BaseModel):
    """Response for GET /questionnaire/{assessment_instance_id}/category/{category_id}."""
    assessment_instance_id: int
    assessment_package: str
    category: str
    assessment_status: str
    category_status: str
    questions: list[QuestionnaireQuestionWithAnswer]


class ResponseItem(BaseModel):
    """Single question-answer pair for upsert."""
    question_id: int
    answer: Any

    def normalized_answer(self) -> Any:
        """Return the answer as-is. Validation happens in service layer."""
        return self.answer


class QuestionnaireResponsesUpsertRequest(BaseModel):
    """Request for PUT /questionnaire/{assessment_instance_id}/category/{category_id}/responses."""
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


class QuestionnaireCategoryStatusUpdateRequest(BaseModel):
    status: str = Field(..., min_length=1, max_length=20)

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class QuestionnaireCategoryQuestionsAssignRequest(BaseModel):
    question_ids: list[int] = Field(..., min_length=1)


class QuestionnaireCategoryQuestionsReorderRequest(BaseModel):
    question_ids: list[int] = Field(..., min_length=1)


class HealthyHabitRuleCreateRequest(BaseModel):
    habit_key: Optional[str] = Field(default=None, max_length=200)
    habit_label: str = Field(..., min_length=1, max_length=500)
    display_order: Optional[int] = None
    condition_type: str = Field(..., min_length=1, max_length=50)
    matched_option_values: Optional[list[str]] = None
    scale_min: Optional[float] = None
    scale_max: Optional[float] = None
    scale_unit: Optional[str] = Field(default=None, max_length=200)
    status: str = Field(default="active", min_length=1, max_length=20)

    def normalized_condition_type(self) -> str:
        return _strip(self.condition_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class HealthyHabitRuleUpdateRequest(BaseModel):
    habit_key: Optional[str] = Field(default=None, max_length=200)
    habit_label: str = Field(..., min_length=1, max_length=500)
    display_order: Optional[int] = None
    condition_type: str = Field(..., min_length=1, max_length=50)
    matched_option_values: Optional[list[str]] = None
    scale_min: Optional[float] = None
    scale_max: Optional[float] = None
    scale_unit: Optional[str] = Field(default=None, max_length=200)
    status: str = Field(default="active", min_length=1, max_length=20)

    def normalized_condition_type(self) -> str:
        return _strip(self.condition_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()
