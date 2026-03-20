"""Pydantic schemas for assessments APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class AssessmentListItem(BaseModel):
    assessment_instance_id: int
    package_id: int
    package_code: Optional[str] = None
    package_display_name: Optional[str] = None
    engagement_id: int
    status: Optional[str] = None
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


class AssessmentPackageCreateRequest(BaseModel):
    package_code: str = Field(min_length=1, max_length=50)
    display_name: str = Field(min_length=1, max_length=200)
    status: str = Field(default="active", min_length=1, max_length=30)


class AssessmentPackageUpdateRequest(BaseModel):
    package_code: str = Field(min_length=1, max_length=50)
    display_name: str = Field(min_length=1, max_length=200)


class AssessmentPackageCategoriesAddRequest(BaseModel):
    category_ids: list[int] = Field(..., min_length=1)


class AssessmentPackageCategoriesReorderRequest(BaseModel):
    category_ids: list[int] = Field(..., min_length=1)


class AssessmentPackageListItem(BaseModel):
    package_id: int
    package_code: Optional[str] = None
    display_name: Optional[str] = None
    status: Optional[str] = None


class AssessmentPackageDetailsResponse(AssessmentPackageListItem):
    pass
