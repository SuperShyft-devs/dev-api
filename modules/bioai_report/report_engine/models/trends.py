"""Pydantic response models for Bio-AI historical health trends."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BioAITrendAssessment(BaseModel):
    """One completed assessment included in the trend series."""

    model_config = ConfigDict(extra="ignore")

    assessment_instance_id: int
    assessment_date: str | None = None


class BioAITrendPoint(BaseModel):
    """One disease score at one assessment date.

    ``score`` is ``None`` when the disease is absent from that assessment.
    A genuine healthy score of ``0`` is preserved as ``0``.
    """

    model_config = ConfigDict(extra="ignore")

    date: str | None = None
    score: int | None = None
    risk_status: str | None = None
    risk_band: str | None = None
    assessment_instance_id: int


class BioAITrendsByDisease(BaseModel):
    """Per-disease chronological series using Bio-AI canonical disease ids."""

    model_config = ConfigDict(extra="ignore")

    metabolic_syndrome: list[BioAITrendPoint] = Field(default_factory=list)
    dyslipidemia: list[BioAITrendPoint] = Field(default_factory=list)
    pcos: list[BioAITrendPoint] = Field(default_factory=list)
    oxidative_stress: list[BioAITrendPoint] = Field(default_factory=list)
    nafld: list[BioAITrendPoint] = Field(default_factory=list)
    hypertension: list[BioAITrendPoint] = Field(default_factory=list)
    obesity: list[BioAITrendPoint] = Field(default_factory=list)
    thyroid_health: list[BioAITrendPoint] = Field(default_factory=list)
    type2_diabetes: list[BioAITrendPoint] = Field(default_factory=list)
    cardiac_health: list[BioAITrendPoint] = Field(default_factory=list)


class BioAITrendResponse(BaseModel):
    """Frontend-ready historical Bio-AI risk scores for one user."""

    model_config = ConfigDict(extra="ignore")

    user_id: int
    assessment_count: int
    trend_available: bool
    assessments: list[BioAITrendAssessment] = Field(default_factory=list)
    trends: BioAITrendsByDisease = Field(default_factory=BioAITrendsByDisease)

    def to_report_field(self) -> dict[str, Any]:
        """Shape used when embedding trends in the Bio-AI report JSON.

        Uses ``diseases`` so the frontend does not get a nested ``trends.trends``
        object. When fewer than two assessments qualify, every canonical disease
        key is present with an empty array.
        """
        data = self.model_dump(mode="json")
        series = data.pop("trends")
        if not data.get("trend_available"):
            data["diseases"] = BioAITrendsByDisease().model_dump(mode="json")
        else:
            data["diseases"] = series
        return data
