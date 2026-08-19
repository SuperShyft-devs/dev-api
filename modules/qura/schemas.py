"""Typed contracts for the Qura orchestration pipeline."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class QuraSchema(BaseModel):
    """Reject unexpected fields, particularly accidental PHI."""

    model_config = ConfigDict(extra="forbid")


class ChatRequest(QuraSchema):
    conversation_id: UUID | None = None
    message: str = Field(min_length=1, max_length=4_000)
    language_preference: Literal["english", "hindi", "hinglish"] | None = None


class IntentCandidate(QuraSchema):
    name: str
    confidence: float = Field(ge=0, le=1)


class IntentResult(QuraSchema):
    primary: str
    intents: list[IntentCandidate] = Field(default_factory=list)
    needs_clarification: bool = False


class QueryPlan(QuraSchema):
    intent: str
    required_markers: list[str] = Field(default_factory=list)
    include_current_report_markers: bool = False
    include_bio_ai_risks: bool = False
    include_marker_deltas: bool = False
    include_lifestyle: bool = False
    include_habits: bool = False
    missing_data: list[str] = Field(default_factory=list)


class Marker(QuraSchema):
    code: str
    name: str
    value: float | None = None
    unit: str | None = None
    ref_low: float | None = None
    ref_high: float | None = None
    # Phase 1 must only accept an existing canonical flag. It never calculates one.
    flag: str | None = "unavailable"
    category: str | None = None


class RiskScore(QuraSchema):
    code: str
    name: str
    score: float | None = None
    status: str | None = None
    unit: str | None = None


class MarkerDelta(QuraSchema):
    code: str
    current_value: float | None = None
    previous_value: float | None = None
    unit: str | None = None
    observed_at: str | None = None


class Profile(QuraSchema):
    age_band: str | None = None
    gender: str | None = None


class Lifestyle(QuraSchema):
    physical_activity: str | None = None
    sleep: str | None = None
    smoking: str | None = None
    alcohol: str | None = None


class HabitsSummary(QuraSchema):
    healthy_habits: list[str] = Field(default_factory=list)


class ContextMeta(QuraSchema):
    source: str
    data_available: bool = False
    requested_data: list[str] = Field(default_factory=list)
    found_data: list[str] = Field(default_factory=list)
    missing_data: list[str] = Field(default_factory=list)


class HealthContext(QuraSchema):
    """The only health-data structure that may later be supplied to an LLM."""

    profile: Profile = Field(default_factory=Profile)
    selected_markers: list[Marker] = Field(default_factory=list)
    risk_scores: list[RiskScore] = Field(default_factory=list)
    deltas: list[MarkerDelta] = Field(default_factory=list)
    lifestyle: Lifestyle | None = None
    habits: HabitsSummary | None = None
    metadata: ContextMeta


class Recommendation(QuraSchema):
    text: str
    category: str | None = None


class SafetyFlags(QuraSchema):
    blocked: bool = False
    categories: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)


class LLMStructuredOutput(QuraSchema):
    answer: str
    grounding: list[str] = Field(default_factory=list)
    confidence: Literal["low", "medium", "high"] = "low"
    safety_flags: list[str] = Field(default_factory=list)
    escalation: str = "none"
    recommendations: list[Recommendation] = Field(default_factory=list)


class BuiltPrompt(QuraSchema):
    """Deterministic provider-neutral prompt payload."""

    version: str
    system_instructions: str
    task: str
    health_context_json: str
    user_question: str
    safety_rules: str
    output_format: str
    language: str


class Evidence(QuraSchema):
    markers: list[Marker] = Field(default_factory=list)
    risks: list[RiskScore] = Field(default_factory=list)
    history: list[MarkerDelta] = Field(default_factory=list)
    lifestyle: list[str] = Field(default_factory=list)


class ChatResponse(QuraSchema):
    answer: str
    evidence: Evidence = Field(default_factory=Evidence)
    safety: SafetyFlags = Field(default_factory=SafetyFlags)
    escalation: str = "none"
    recommendations: list[Recommendation] = Field(default_factory=list)
    conversation_id: UUID | None = None
