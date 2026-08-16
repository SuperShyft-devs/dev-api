"""Assessment payload models (normalized from MetSights report JSON)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AssessmentDisease(BaseModel):
    """One disease entry from the assessment / MetSights report."""

    model_config = ConfigDict(extra="ignore")

    code: str
    name: str | None = None
    risk_status: str | None = None
    risk_score_scaled: float | int | None = None
    healthy_percentile: float | int | None = None
    lifestyle_contribution: float | int | None = None
    disease_percentile: float | int | None = None
    risk_status_message: str | None = None
    lifestyle_contribution_message: str | None = None


class AssessmentPayload(BaseModel):
    """Normalized assessment JSON used by report builders."""

    model_config = ConfigDict(extra="ignore")

    record_id: str | None = None
    name: str | None = None
    age: float | int | None = None
    sex: str | None = None
    gender: str | None = None
    date_of_birth: str | None = None
    height: float | int | None = None
    weight: float | int | None = None
    bmi: float | int | None = None
    profile_id: str | None = None
    metabolic_age: float | int | None = None
    metabolic_score: float | int | None = None
    metabolic_health_status: str | None = None
    assessment_code: str | None = None
    assessment_date: str | None = None
    created_at: str | None = None
    diseases: list[AssessmentDisease] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict, exclude=True)
