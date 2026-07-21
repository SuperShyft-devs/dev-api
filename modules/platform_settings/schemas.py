"""Request/response schemas for platform settings API."""

from __future__ import annotations

from pydantic import BaseModel, Field

from modules.engagements.models import BloodCollectionType, EngagementKind


class B2cOnboardingDefaultsRead(BaseModel):
    b2c_default_assessment_package_id: int
    b2c_default_diagnostic_package_id: int
    b2c_default_engagement_type: EngagementKind
    b2c_default_blood_collection_type: BloodCollectionType | None = None
    b2c_default_create_profile_on_metsights: bool
    b2c_default_enroll_for_fitprint_full: bool


class B2cOnboardingDefaultsUpdate(BaseModel):
    b2c_default_assessment_package_id: int = Field(..., ge=1)
    b2c_default_diagnostic_package_id: int = Field(..., ge=1)
    b2c_default_engagement_type: EngagementKind
    b2c_default_blood_collection_type: BloodCollectionType | None = None
    b2c_default_create_profile_on_metsights: bool
    b2c_default_enroll_for_fitprint_full: bool


class EngagementNotificationDefaultsRead(BaseModel):
    default_onboarding_notification: str | None = None
    default_pretest_guidelines_notification: str | None = None
    default_questionnaire_reminder_1: str | None = None
    default_questionnaire_reminder_2: str | None = None
    default_blood_report_notification: str | None = None
    default_bioai_report_notification: str | None = None
    default_notify_users_for_consultation: str | None = None


class EngagementNotificationDefaultsUpdate(BaseModel):
    default_onboarding_notification: str | None = Field(default=None, max_length=500)
    default_pretest_guidelines_notification: str | None = Field(default=None, max_length=500)
    default_questionnaire_reminder_1: str | None = Field(default=None, max_length=500)
    default_questionnaire_reminder_2: str | None = Field(default=None, max_length=500)
    default_blood_report_notification: str | None = Field(default=None, max_length=500)
    default_bioai_report_notification: str | None = Field(default=None, max_length=500)
    default_notify_users_for_consultation: str | None = Field(default=None, max_length=500)


class MetsightsProfilesImportPageRequest(BaseModel):
    page: int = Field(..., ge=1)


class MetsightsProfilesStatsRead(BaseModel):
    local_total_users: int
    local_with_metsights_profile_id: int
    local_without_metsights_profile_id: int
    metsights_total: int
    estimated_not_imported: int


class DefaultOnboardingAssistantItem(BaseModel):
    employee_id: int
    user_id: int
    role: str
    status: str
    first_name: str | None = None
    last_name: str | None = None


class DefaultOnboardingAssistantsRead(BaseModel):
    employee_ids: list[int]
    assistants: list[DefaultOnboardingAssistantItem]


class DefaultOnboardingAssistantsUpdate(BaseModel):
    employee_ids: list[int] = Field(default_factory=list)


class SupportQueryNotificationRead(BaseModel):
    default_support_query_notification: str | None = None


class SupportQueryNotificationUpdate(BaseModel):
    default_support_query_notification: str | None = Field(default=None, max_length=500)
