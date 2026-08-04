"""Request/response schemas for platform settings API."""

from __future__ import annotations

from pydantic import BaseModel, Field

from modules.engagements.models import BloodCollectionType


class B2cOnboardingTypeDefaults(BaseModel):
    assessment_package_id: int = Field(..., ge=1)
    diagnostic_package_id: int = Field(..., ge=1)
    blood_collection_type: BloodCollectionType | None = None
    create_profile_on_metsights: bool
    enroll_for_fitprint_full: bool


class B2cOnboardingDefaultsRead(BaseModel):
    """Keyed by engagement_types.code (active rows)."""

    defaults_by_engagement_type: dict[str, B2cOnboardingTypeDefaults]


class B2cOnboardingDefaultsUpdate(BaseModel):
    """Keyed by engagement_types.code (must be active)."""

    defaults_by_engagement_type: dict[str, B2cOnboardingTypeDefaults]


class EngagementNotificationDefaultsRead(BaseModel):
    """Kept for backward compatibility - now managed via engagement_notification_defaults table."""
    pass


class EngagementNotificationDefaultsUpdate(BaseModel):
    """Kept for backward compatibility - now managed via engagement_notification_defaults table."""
    pass


class MetsightsProfilesImportPageRequest(BaseModel):
    page: int = Field(..., ge=1)


class MetsightsProfilesStatsRead(BaseModel):
    local_total_users: int
    local_with_metsights_profile_id: int
    local_without_metsights_profile_id: int
    metsights_total: int
    estimated_not_imported: int


class EngagementsSyncImportPageRequest(BaseModel):
    page: int = Field(..., ge=1)


class EngagementsSyncStatsRead(BaseModel):
    users_with_metsights_profile_id: int
    b2c_engagements_total: int


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
