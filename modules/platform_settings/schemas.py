"""Request/response schemas for platform settings API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class B2cOnboardingDefaultsRead(BaseModel):
    b2c_default_assessment_package_id: int
    b2c_default_diagnostic_package_id: int


class B2cOnboardingDefaultsUpdate(BaseModel):
    b2c_default_assessment_package_id: int = Field(..., ge=1)
    b2c_default_diagnostic_package_id: int = Field(..., ge=1)


class MetsightsProfilesImportPageRequest(BaseModel):
    page: int = Field(..., ge=1)


class QuestionnaireCategoryProgressRefreshPageRequest(BaseModel):
    """Zero-based offset of the next assessment instance to process (one instance per request)."""

    offset: int = Field(0, ge=0)


class MetsightsProfilesStatsRead(BaseModel):
    local_total_users: int
    local_with_metsights_profile_id: int
    local_without_metsights_profile_id: int
    metsights_total: int
    estimated_not_imported: int
