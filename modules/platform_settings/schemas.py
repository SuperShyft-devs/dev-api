"""Request/response schemas for platform settings API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class B2cOnboardingDefaultsRead(BaseModel):
    b2c_default_assessment_package_id: int
    b2c_default_diagnostic_package_id: int


class B2cOnboardingDefaultsUpdate(BaseModel):
    b2c_default_assessment_package_id: int = Field(..., ge=1)
    b2c_default_diagnostic_package_id: int = Field(..., ge=1)
