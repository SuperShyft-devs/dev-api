"""Request/response schemas for platform settings API."""

from __future__ import annotations

from pydantic import BaseModel, Field

from common.validation import OptionalChecklistText, PositiveIntId
from modules.engagements.enums import ConsultationMode, EngagementPaymentType
from modules.engagements.models import BloodCollectionType
from modules.geocoding.enums import GeocodingProvider


class B2cOnboardingTypeDefaults(BaseModel):
    assessment_package_id: PositiveIntId
    diagnostic_package_id: PositiveIntId | None = None
    diagnostic_package_id_male: PositiveIntId | None = None
    diagnostic_package_id_female: PositiveIntId | None = None
    blood_collection_type: BloodCollectionType | None = None
    create_profile_on_metsights: bool = True
    enroll_for_fitprint_full: bool = False
    load_prev_assessment_questionnaires: bool = False
    load_prev_questionnaire_category_keys: list[str] | None = None
    slot_duration: int = Field(default=20, ge=1, le=480)
    consultation_mode: ConsultationMode | None = None
    auto_booking: bool = False
    payment_type: EngagementPaymentType = EngagementPaymentType.paid_by_patient


class B2cOnboardingDefaultsRead(BaseModel):
    """Keyed by engagement_types.code (active rows)."""

    defaults_by_engagement_type: dict[str, B2cOnboardingTypeDefaults]
    default_engagement_type_code: str = "bio_ai"


class B2cOnboardingDefaultsUpdate(BaseModel):
    """Keyed by engagement_types.code (must be active)."""

    defaults_by_engagement_type: dict[str, B2cOnboardingTypeDefaults]
    default_engagement_type_code: str | None = None


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


class SupportQueryNotificationRead(BaseModel):
    default_support_query_notification: str | None = None


class SupportQueryNotificationUpdate(BaseModel):
    default_support_query_notification: OptionalChecklistText = None


class GeocodingProviderSettingsRead(BaseModel):
    geocoding_provider: GeocodingProvider = GeocodingProvider.google


class GeocodingProviderSettingsUpdate(BaseModel):
    geocoding_provider: GeocodingProvider
