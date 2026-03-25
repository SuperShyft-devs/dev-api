"""Pydantic schemas for engagements APIs."""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field

from modules.checklists.schemas import ChecklistReadiness


class EngagementCreateRequest(BaseModel):
    """Create a new B2B engagement."""

    engagement_name: Optional[str] = Field(default=None, max_length=200)
    organization_id: int = Field(gt=0)
    engagement_type: str = Field(min_length=1, max_length=50)
    engagement_code: Optional[str] = Field(default=None, max_length=50)
    assessment_package_id: int = Field(gt=0)
    # Optional on the request so the service can raise a domain error for B2B.
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    city: Optional[str] = Field(default=None, max_length=100)
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date


class EngagementUpdateRequest(BaseModel):
    """Update editable engagement fields."""

    engagement_name: Optional[str] = Field(default=None, max_length=200)
    organization_id: int = Field(gt=0)
    engagement_type: str = Field(min_length=1, max_length=50)
    assessment_package_id: int = Field(gt=0)
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    city: Optional[str] = Field(default=None, max_length=100)
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)


class EngagementStatusUpdateRequest(BaseModel):
    """Change engagement status."""

    status: str = Field(min_length=1, max_length=30)


class EngagementListItem(BaseModel):
    engagement_id: int
    engagement_name: Optional[str] = None
    organization_id: Optional[int] = None
    engagement_code: str
    engagement_type: Optional[str] = None
    assessment_package_id: int
    diagnostic_package_id: int
    city: Optional[str] = None
    slot_duration: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    status: Optional[str] = None
    participant_count: Optional[int] = None
    readiness: ChecklistReadiness


class EngagementDetailsResponse(EngagementListItem):
    metsights_engagement_id: Optional[str] = None
    readiness: ChecklistReadiness = Field(
        default_factory=lambda: ChecklistReadiness(done=0, total=0, percent=0),
    )


class OnboardingAssistantsAddRequest(BaseModel):
    """Request to assign employees as onboarding assistants."""

    employee_ids: list[int] = Field(..., min_length=1)
