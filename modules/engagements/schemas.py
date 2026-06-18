"""Pydantic schemas for engagements APIs."""

from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field

from modules.checklists.schemas import ChecklistReadiness
from modules.engagements.models import EngagementKind


class EngagementCreateRequest(BaseModel):
    """Create a new B2B engagement."""

    engagement_name: Optional[str] = Field(default=None, max_length=200)
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)
    organization_id: int = Field(gt=0)
    engagement_type: EngagementKind
    engagement_code: Optional[str] = Field(default=None, max_length=50)
    assessment_package_id: Optional[int] = Field(default=None, gt=0)
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    city: Optional[str] = Field(default=None, max_length=100)
    address: Optional[str] = Field(default=None, max_length=500)
    pincode: Optional[str] = Field(default=None, max_length=20)
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    notification_service_key: str | None = Field(default=None, max_length=200)
    pretest_guidelines_notification: str | None = Field(default=None, max_length=500)
    questionnaire_reminder_1: str | None = Field(default=None, max_length=500)
    questionnaire_reminder_2: str | None = Field(default=None, max_length=500)
    blood_report_notification: str | None = Field(default=None, max_length=500)
    bioai_report_notification: str | None = Field(default=None, max_length=500)
    camp_no: Optional[int] = Field(default=None, gt=0)


class EngagementUpdateRequest(BaseModel):
    """Update editable engagement fields."""

    engagement_name: Optional[str] = Field(default=None, max_length=200)
    engagement_code: str = Field(min_length=1, max_length=50)
    organization_id: Optional[int] = Field(default=None, gt=0)
    engagement_type: EngagementKind
    assessment_package_id: Optional[int] = Field(default=None, gt=0)
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    city: Optional[str] = Field(default=None, max_length=100)
    address: Optional[str] = Field(default=None, max_length=500)
    pincode: Optional[str] = Field(default=None, max_length=20)
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    notification_service_key: str | None = Field(default=None, max_length=200)
    pretest_guidelines_notification: str | None = Field(default=None, max_length=500)
    questionnaire_reminder_1: str | None = Field(default=None, max_length=500)
    questionnaire_reminder_2: str | None = Field(default=None, max_length=500)
    blood_report_notification: str | None = Field(default=None, max_length=500)
    bioai_report_notification: str | None = Field(default=None, max_length=500)
    camp_no: Optional[int] = Field(default=None, gt=0)


class EngagementStatusUpdateRequest(BaseModel):
    """Change engagement status."""

    status: str = Field(min_length=1, max_length=30)


class EngagementListItem(BaseModel):
    engagement_id: int
    engagement_name: Optional[str] = None
    organization_id: Optional[int] = None
    engagement_code: str
    engagement_type: Optional[str] = None
    assessment_package_id: Optional[int] = None
    diagnostic_package_id: Optional[int] = None
    city: Optional[str] = None
    address: Optional[str] = None
    pincode: Optional[str] = None
    slot_duration: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    status: Optional[str] = None
    participant_count: Optional[int] = None
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    notification_service_key: str
    pretest_guidelines_notification: str | None = None
    questionnaire_reminder_1: str | None = None
    questionnaire_reminder_2: str | None = None
    blood_report_notification: str | None = None
    bioai_report_notification: str | None = None
    camp_no: Optional[int] = None
    readiness: ChecklistReadiness


class EngagementDetailsResponse(EngagementListItem):
    metsights_engagement_id: Optional[str] = None
    readiness: ChecklistReadiness = Field(
        default_factory=lambda: ChecklistReadiness(done=0, total=0, percent=0),
    )


class OnboardingAssistantsAddRequest(BaseModel):
    """Request to assign employees as onboarding assistants."""

    employee_ids: list[int] = Field(..., min_length=1)


class EngagementAssessmentPackageAddRequest(BaseModel):
    """Request to add an additional assessment package to an engagement."""

    package_code: str = Field(..., min_length=1, max_length=100)


class EngagementPushQuestionnairesRequest(BaseModel):
    """Request to push questionnaire answers for a specific package."""

    package_id: int = Field(..., gt=0)


class EngagementConnectMetsightsRecordsRequest(BaseModel):
    """Request to create Metsights records for existing assessment instances."""

    package_id: int = Field(..., gt=0)


class AssignParticipantsRow(BaseModel):
    """One CSV row: Metsights record id + participant phone + email."""

    metsights_record_id: str = Field(..., min_length=1, max_length=200)
    phone: str = Field(..., max_length=50)
    email: str = Field(..., max_length=255)


class AssignParticipantsBatchRequest(BaseModel):
    """Batch assign participants from parsed CSV rows."""

    rows: list[AssignParticipantsRow] = Field(..., min_length=1, max_length=50)


class AssignParticipantsRowResult(BaseModel):
    metsights_record_id: str
    phone: str
    email: str
    status: str
    reason: Optional[str] = None
    user_id: Optional[int] = None
    assessment_instance_id: Optional[int] = None
    newly_enrolled: Optional[bool] = None


class AssignParticipantsBatchResponse(BaseModel):
    results: list[AssignParticipantsRowResult]


class CreateMetsightsProfilesRequest(BaseModel):
    """Request body for the create-metsights-profiles endpoint."""

    mode: str = Field(
        default="profile",
        pattern=r"^(enrol_force|enrol|profile)$",
        description="enrol_force: register all via engagement; enrol: register new only via engagement; profile: create standalone profiles for new only",
    )
