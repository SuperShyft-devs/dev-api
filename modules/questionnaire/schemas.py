"""Questionnaire request/response schemas."""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from common.validation import (
    OptionalQuestionText,
    OptionalSafeText,
    OptionalSlugKey,
    QuestionText,
    SafeDisplayName,
    SlugKey,
    StatusStr,
    validate_nested_strings,
)


def _strip(value: str | None) -> str:
    return (value or "").strip()


class QuestionnaireQuestionCreateRequest(BaseModel):
    question_key: SlugKey
    question_text: QuestionText
    question_type: SlugKey
    is_required: bool = False
    is_read_only: bool = False
    help_text: OptionalQuestionText = None
    sub_text: OptionalQuestionText = None
    options: Optional[list[dict[str, str | None]]] = Field(default=None)
    visibility_rules: Optional[dict[str, Any]] = Field(default=None)
    prefill_from: Optional[dict[str, Any]] = Field(default=None)
    metsights_sync: Optional[dict[str, Any]] = Field(default=None)
    status: StatusStr = "active"

    @model_validator(mode="after")
    def sanitize_nested_fields(self) -> "QuestionnaireQuestionCreateRequest":
        if self.visibility_rules is not None:
            validate_nested_strings(self.visibility_rules)
        if self.prefill_from is not None:
            validate_nested_strings(self.prefill_from)
        if self.metsights_sync is not None:
            validate_nested_strings(self.metsights_sync)
        return self

    def normalized_question_key(self) -> str:
        return _strip(self.question_key).lower()

    def normalized_question_text(self) -> str:
        return _strip(self.question_text)

    def normalized_question_type(self) -> str:
        return _strip(self.question_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class QuestionnaireQuestionUpdateRequest(BaseModel):
    question_key: SlugKey
    question_text: QuestionText
    question_type: SlugKey
    is_required: bool = False
    is_read_only: bool = False
    help_text: OptionalQuestionText = None
    sub_text: OptionalQuestionText = None
    options: Optional[list[dict[str, str | None]]] = Field(default=None)
    visibility_rules: Optional[dict[str, Any]] = Field(default=None)
    prefill_from: Optional[dict[str, Any]] = Field(default=None)
    metsights_sync: Optional[dict[str, Any]] = Field(default=None)

    @model_validator(mode="after")
    def sanitize_nested_fields(self) -> "QuestionnaireQuestionUpdateRequest":
        if self.visibility_rules is not None:
            validate_nested_strings(self.visibility_rules)
        if self.prefill_from is not None:
            validate_nested_strings(self.prefill_from)
        if self.metsights_sync is not None:
            validate_nested_strings(self.metsights_sync)
        return self

    def normalized_question_key(self) -> str:
        return _strip(self.question_key).lower()

    def normalized_question_text(self) -> str:
        return _strip(self.question_text)

    def normalized_question_type(self) -> str:
        return _strip(self.question_type).lower()


class MetsightsSyncUpdateRequest(BaseModel):
    """Dedicated schema for updating metsights_sync on a question definition."""
    metsights_sync: dict[str, Any] = Field(...)

    @model_validator(mode="after")
    def sanitize_metsights_sync(self) -> "MetsightsSyncUpdateRequest":
        validate_nested_strings(self.metsights_sync)
        return self


class MetsightsSyncGapsCategoryRef(BaseModel):
    category_id: int
    category_key: str | None = None
    display_name: str | None = None


class MetsightsSyncGapsItem(BaseModel):
    question_id: int
    question_key: str | None = None
    question_text: str | None = None
    metsights_categories: list[MetsightsSyncGapsCategoryRef]
    sync_gaps: dict[str, bool]


class MetsightsSyncGapsSummary(BaseModel):
    not_configured: int
    pull_disabled: int
    push_disabled: int


class MetsightsSyncGapsResponse(BaseModel):
    count: int
    summary: MetsightsSyncGapsSummary
    questions: list[MetsightsSyncGapsItem]


class BloodParametersReloadResponse(BaseModel):
    questions_deleted: int
    responses_deleted: int
    questions_created: int
    categories_created: int
    categories_updated: int
    question_links_total: int
    links_added: int
    package_links_added: int
    package_links_total: int
    missing_package_codes: list[str]


class QuestionnaireQuestionStatusUpdateRequest(BaseModel):
    status: StatusStr

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
    sub_text: str | None
    options: Any | None
    visibility_rules: dict[str, Any] | None
    prefill_from: dict[str, Any] | None
    metsights_sync: dict[str, Any] | None = None
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
    sub_text: str | None
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

    @model_validator(mode="after")
    def sanitize_answer(self) -> "ResponseItem":
        if self.answer is not None:
            validate_nested_strings(self.answer)
        return self

    def normalized_answer(self) -> Any:
        """Return the answer as-is. Validation happens in service layer."""
        return self.answer


class QuestionnaireResponsesUpsertRequest(BaseModel):
    """Request for PUT /questionnaire/{assessment_instance_id}/category/{category_id}/responses."""
    responses: list[ResponseItem] = Field(..., min_length=1, max_length=500)


_VALID_CATEGORY_OF = {"supershyft", "metsights"}


class QuestionnaireCategoryCreateRequest(BaseModel):
    category_key: SlugKey
    display_name: SafeDisplayName
    category_of: SlugKey = "supershyft"

    def normalized_category_key(self) -> str:
        return _strip(self.category_key).lower()

    def normalized_display_name(self) -> str:
        return _strip(self.display_name)

    def normalized_category_of(self) -> str:
        return _strip(self.category_of).lower()


class QuestionnaireCategoryUpdateRequest(BaseModel):
    category_key: SlugKey
    display_name: SafeDisplayName
    category_of: SlugKey = "supershyft"

    def normalized_category_key(self) -> str:
        return _strip(self.category_key).lower()

    def normalized_display_name(self) -> str:
        return _strip(self.display_name)

    def normalized_category_of(self) -> str:
        return _strip(self.category_of).lower()


class QuestionnaireCategoryStatusUpdateRequest(BaseModel):
    status: StatusStr

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class QuestionnaireCategoryQuestionsAssignRequest(BaseModel):
    question_ids: list[int] = Field(..., min_length=1)


class QuestionnaireCategoryQuestionsReorderRequest(BaseModel):
    question_ids: list[int] = Field(..., min_length=1)


class HealthyHabitRuleCreateRequest(BaseModel):
    habit_key: OptionalSlugKey = None
    habit_label: SafeDisplayName
    display_order: Optional[int] = None
    condition_type: SlugKey
    matched_option_values: Optional[list[str]] = None
    scale_min: Optional[float] = None
    scale_max: Optional[float] = None
    scale_unit: OptionalSafeText = None
    status: StatusStr = "active"

    def normalized_condition_type(self) -> str:
        return _strip(self.condition_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()


class HealthyHabitRuleUpdateRequest(BaseModel):
    habit_key: OptionalSlugKey = None
    habit_label: SafeDisplayName
    display_order: Optional[int] = None
    condition_type: SlugKey
    matched_option_values: Optional[list[str]] = None
    scale_min: Optional[float] = None
    scale_max: Optional[float] = None
    scale_unit: OptionalSafeText = None
    status: StatusStr = "active"

    def normalized_condition_type(self) -> str:
        return _strip(self.condition_type).lower()

    def normalized_status(self) -> str:
        return _strip(self.status).lower()
