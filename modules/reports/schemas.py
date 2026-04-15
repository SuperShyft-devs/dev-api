"""Reports module schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BloodParameterTestInReportResponse(BaseModel):
    test_id: int
    test_name: str
    parameter_key: str | None = None
    unit: str | None = None
    value: float | None = None
    lower_range: float | None = None
    higher_range: float | None = None


class BloodParameterGroupInReportResponse(BaseModel):
    group_name: str
    test_count: int
    tests: list[BloodParameterTestInReportResponse]


class BloodParameterTrendPoint(BaseModel):
    date: str
    value: float
    engagement_id: int


class BloodParameterTrendResponse(BaseModel):
    parameter: str
    unit: str | None = None
    data_points: list[BloodParameterTrendPoint]


class DiseaseOverview(BaseModel):
    code: str
    name: str
    risk_status: str
    risk_score_scaled: int


class RiskAnalysisItem(BaseModel):
    code: str
    name: str
    risk_status: str
    risk_score_scaled: int
    healthy_percentile: int


class HealthyHabitItem(BaseModel):
    habit_key: str | None = None
    habit_label: str


class PositiveWins(BaseModel):
    low_risk: list[DiseaseOverview]
    healthy_habits: list[HealthyHabitItem] = Field(default_factory=list)
    healthy_profiles: list[str] = Field(default_factory=list)


class OverviewReportResponse(BaseModel):
    assessment_id: int
    metabolic_age: float | None
    positive_wins: PositiveWins
    risk_analysis: list[RiskAnalysisItem]


class DiseaseListItem(BaseModel):
    code: str
    name: str
    risk_score_scaled: int


class RiskAnalysisListResponse(BaseModel):
    assessment_id: int
    metabolic_score: float | None
    diseases: list[DiseaseListItem]


class BioAiPdfResponse(BaseModel):
    assessment_id: int
    report_url: str


class DiseaseDetailResponse(BaseModel):
    code: str
    name: str
    meaning: str | None = None
    unit: str | None = None
    risk_score_scaled: int
    lifestyle_contribution: int | None
    disease_percentile: int | None
    lower_range_male: float | None = None
    higher_range_male: float | None = None
    lower_range_female: float | None = None
    higher_range_female: float | None = None
    causes_when_high: str | None = None
    causes_when_low: str | None = None
    effects_when_high: str | None = None
    effects_when_low: str | None = None
    what_to_do_when_low: str | None = None
    what_to_do_when_high: str | None = None
