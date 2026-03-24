"""Reports module schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class BloodParametersReportResponse(BaseModel):
    assessment_id: int
    blood_parameters: Any
