"""Reports module schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


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


class PositiveWins(BaseModel):
    low_risk: list[DiseaseOverview]


class OverviewReportResponse(BaseModel):
    assessment_id: int
    metabolic_age: float | None
    positive_wins: PositiveWins
    risk_analysis: list[RiskAnalysisItem]
