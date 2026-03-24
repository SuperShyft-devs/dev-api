"""Reports module schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class BloodParametersReportResponse(BaseModel):
    assessment_id: int
    blood_parameters: Any


class BloodParameterTrendPoint(BaseModel):
    date: str
    value: float
    engagement_id: int


class BloodParameterTrendResponse(BaseModel):
    parameter: str
    unit: str | None = None
    data_points: list[BloodParameterTrendPoint]
