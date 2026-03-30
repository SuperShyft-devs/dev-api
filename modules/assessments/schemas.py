"""Pydantic schemas for assessments APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class AssessmentListItem(BaseModel):
    assessment_instance_id: int
    package_id: int
    package_code: Optional[str] = None
    package_display_name: Optional[str] = None
    engagement_id: int
    status: Optional[str] = None
    metsights_record_id: Optional[str] = None
    assigned_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class AssessmentDetailsResponse(AssessmentListItem):
    pass


class AssessmentStatusUpdateRequest(BaseModel):
    status: str = Field(min_length=1, max_length=30)


class AssessmentStatusUpdateResponse(BaseModel):
    assessment_instance_id: int
    status: str
    completed_at: Optional[datetime] = None


class MetsightsRecordIdUpdate(BaseModel):
    metsights_record_id: str

    @field_validator("metsights_record_id")
    @classmethod
    def validate_metsights_record_id(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("metsights_record_id cannot be empty")
        return stripped


class AssessmentPackageCreateRequest(BaseModel):
    package_code: str = Field(min_length=1, max_length=50)
    display_name: str = Field(min_length=1, max_length=200)
    assessment_type_code: str = Field(min_length=1, max_length=10)
    status: str = Field(default="active", min_length=1, max_length=30)


class AssessmentPackageUpdateRequest(BaseModel):
    package_code: str = Field(min_length=1, max_length=50)
    display_name: str = Field(min_length=1, max_length=200)
    assessment_type_code: str = Field(min_length=1, max_length=10)


class AssessmentPackageCategoriesAddRequest(BaseModel):
    category_ids: list[int] = Field(..., min_length=1)


class AssessmentPackageCategoriesReorderRequest(BaseModel):
    category_ids: list[int] = Field(..., min_length=1)


class AssessmentPackageListItem(BaseModel):
    package_id: int
    package_code: Optional[str] = None
    display_name: Optional[str] = None
    assessment_type_code: Optional[str] = None
    status: Optional[str] = None


class AssessmentPackageDetailsResponse(AssessmentPackageListItem):
    pass
