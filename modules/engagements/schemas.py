"""Pydantic schemas for engagements APIs."""

from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from common.validation import (
    EngagementCode,
    OptionalAddressText,
    OptionalCityStateCountry,
    OptionalEngagementCode,
    OptionalLandmarkText,
    OptionalPinCode,
    OptionalSafeDisplayName,
    PackageCode,
    PhoneStr,
    PinCode,
    PositiveIntId,
    SafeDisplayName,
    SlugKey,
    StatusStr,
)
from modules.engagement_notifications.service_config import NotificationServiceConfigItem

from modules.checklists.schemas import ChecklistReadiness
from modules.engagements.models import BloodCollectionType, EngagementStatus
from modules.engagements.enums import ConsultationMode

_TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_hhmm(value: str) -> time:
    match = _TIME_RE.match(value)
    if not match:
        raise ValueError(f"Invalid time '{value}'; expected HH:MM")
    return time(int(match.group(1)), int(match.group(2)))


class CabinBreak(BaseModel):
    start_time: str
    end_time: str

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        _parse_hhmm(value)
        return value

    @model_validator(mode="after")
    def end_after_start(self) -> CabinBreak:
        if _parse_hhmm(self.end_time) <= _parse_hhmm(self.start_time):
            raise ValueError("Break end_time must be after start_time")
        return self


class CabinSlotConfig(BaseModel):
    cabin_name: SafeDisplayName
    cabin_key: SlugKey
    start_time: str
    end_time: str
    slot_duration: int = Field(gt=0, le=480)
    capacity_per_slot: int = Field(gt=0, le=1000)
    breaks: list[CabinBreak] = Field(default_factory=list)
    is_active: bool = True

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_time(cls, value: str) -> str:
        _parse_hhmm(value)
        return value

    @model_validator(mode="after")
    def validate_window_and_breaks(self) -> CabinSlotConfig:
        start = _parse_hhmm(self.start_time)
        end = _parse_hhmm(self.end_time)
        if end <= start:
            raise ValueError("Cabin end_time must be after start_time")
        for br in self.breaks:
            br_start = _parse_hhmm(br.start_time)
            br_end = _parse_hhmm(br.end_time)
            if br_start < start or br_end > end:
                raise ValueError("Break must be within cabin start_time and end_time")
        return self


class ConsultationCabinSlotConfig(CabinSlotConfig):
    expert_type: SlugKey


class DateSlotConfig(BaseModel):
    is_enable: bool = True
    cabins: list[CabinSlotConfig] = Field(default_factory=list)


class ConsultationDateSlotConfig(BaseModel):
    is_enable: bool = True
    cabins: list[ConsultationCabinSlotConfig] = Field(default_factory=list)


def _normalize_date_entry(value: Any) -> dict[str, Any]:
    """Accept legacy date→[cabins] or new date→{is_enable, cabins}."""
    if isinstance(value, list):
        return {"is_enable": True, "cabins": value}
    if isinstance(value, dict):
        if "cabins" in value or "is_enable" in value:
            return {
                "is_enable": value.get("is_enable", True) is not False,
                "cabins": value.get("cabins") if isinstance(value.get("cabins"), list) else [],
            }
        return {"is_enable": True, "cabins": []}
    return {"is_enable": True, "cabins": []}


class SlotDetail(BaseModel):
    blood_collection: Optional[dict[str, DateSlotConfig]] = None
    consultation: Optional[dict[str, ConsultationDateSlotConfig]] = None

    @field_validator("blood_collection", "consultation", mode="before")
    @classmethod
    def coerce_legacy_date_entries(cls, value: Any) -> Any:
        if value is None:
            return value
        if not isinstance(value, dict):
            return value
        return {date_key: _normalize_date_entry(entry) for date_key, entry in value.items()}

    @field_validator("blood_collection", "consultation")
    @classmethod
    def validate_date_keys(cls, value: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if value is None:
            return value
        for date_key in value:
            if not _DATE_RE.match(date_key):
                raise ValueError(f"Invalid date key '{date_key}'; expected YYYY-MM-DD")
        return value

    @model_validator(mode="after")
    def unique_cabin_keys(self) -> SlotDetail:
        keys: list[str] = []
        for section in (self.blood_collection, self.consultation):
            if not section:
                continue
            for date_entry in section.values():
                for cabin in date_entry.cabins:
                    keys.append(cabin.cabin_key)
        if len(keys) != len(set(keys)):
            raise ValueError("cabin_key must be unique within slot_detail")
        return self


class EngagementNotificationInput(BaseModel):
    """A single notification event config for create/update."""

    notification_event_id: PositiveIntId
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        from modules.engagement_notifications.service_config import normalize_notification_services

        return normalize_notification_services(value if isinstance(value, list) else [])


def _consultations_enabled(consultations: dict[str, bool] | None) -> bool:
    if not consultations:
        return False
    return any(value is True for value in consultations.values())


class EngagementCreateRequest(BaseModel):
    """Create a new B2B engagement."""

    engagement_name: OptionalSafeDisplayName = None
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)
    organization_id: PositiveIntId
    camp_no: Optional[int] = None
    engagement_type: PositiveIntId
    consultations: Optional[dict[str, bool]] = None
    slot_detail: Optional[SlotDetail] = None
    engagement_code: OptionalEngagementCode = None
    assessment_package_id: Optional[PositiveIntId] = None
    diagnostic_package_id: Optional[PositiveIntId] = None
    city: OptionalCityStateCountry = None
    address: OptionalAddressText = None
    sub_locality: OptionalLandmarkText = None
    landmark: OptionalLandmarkText = None
    pincode: OptionalPinCode = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    healthians_zone_id: Optional[str] = Field(default=None, max_length=50)
    external_camp_id: Optional[int] = None
    blood_collection_type: Optional[BloodCollectionType] = None
    consultation_mode: Optional[ConsultationMode] = None
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    load_prev_assessment_questionnaires: bool = False
    notifications: list[EngagementNotificationInput] | None = None

    @model_validator(mode="after")
    def consultation_mode_required_when_consultations(self) -> EngagementCreateRequest:
        if _consultations_enabled(self.consultations) and self.consultation_mode is None:
            raise ValueError("consultation_mode is required when consultations are enabled")
        return self


class EngagementUpdateRequest(BaseModel):
    """Update editable engagement fields."""

    engagement_name: OptionalSafeDisplayName = None
    engagement_code: EngagementCode
    organization_id: Optional[PositiveIntId] = None
    camp_no: Optional[int] = None
    engagement_type: PositiveIntId
    consultations: Optional[dict[str, bool]] = None
    slot_detail: Optional[SlotDetail] = None
    assessment_package_id: Optional[PositiveIntId] = None
    diagnostic_package_id: Optional[PositiveIntId] = None
    city: OptionalCityStateCountry = None
    address: OptionalAddressText = None
    sub_locality: OptionalLandmarkText = None
    landmark: OptionalLandmarkText = None
    pincode: OptionalPinCode = None
    state: OptionalCityStateCountry = None
    country: OptionalCityStateCountry = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    healthians_zone_id: Optional[str] = Field(default=None, max_length=50)
    external_camp_id: Optional[int] = None
    blood_collection_type: Optional[BloodCollectionType] = None
    consultation_mode: Optional[ConsultationMode] = None
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    load_prev_assessment_questionnaires: bool = False
    notifications: list[EngagementNotificationInput] | None = None

    @model_validator(mode="after")
    def consultation_mode_required_when_consultations(self) -> EngagementUpdateRequest:
        if _consultations_enabled(self.consultations) and self.consultation_mode is None:
            raise ValueError("consultation_mode is required when consultations are enabled")
        return self


class EngagementStatusUpdateRequest(BaseModel):
    """Change engagement status."""

    status: StatusStr


class ResolveHealthiansZoneRequest(BaseModel):
    """Resolve Healthians zone ID for an engagement location."""

    diagnostic_package_id: PositiveIntId
    latitude: float
    longitude: float
    pincode: PinCode


class ResolveHealthiansZoneResponse(BaseModel):
    serviceable: bool
    zone_id: Optional[str] = None
    message: str


class EngagementNotificationOutput(BaseModel):
    notification_event_id: int
    event_code: str | None = None
    event_display_name: str | None = None
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        from modules.engagement_notifications.service_config import normalize_notification_services

        return normalize_notification_services(value if isinstance(value, list) else [])


class EngagementListItem(BaseModel):
    engagement_id: int
    engagement_name: Optional[str] = None
    organization_id: Optional[int] = None
    camp_no: Optional[int] = None
    engagement_code: str
    engagement_type: Optional[int] = None
    consultations: Optional[dict[str, bool]] = None
    slot_detail: Optional[dict[str, Any]] = None
    assessment_package_id: Optional[int] = None
    diagnostic_package_id: Optional[int] = None
    city: Optional[str] = None
    address: Optional[str] = None
    sub_locality: Optional[str] = None
    landmark: Optional[str] = None
    pincode: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    slot_duration: Optional[int] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    status: Optional[str] = None
    participant_count: Optional[int] = None
    created_at: Optional[datetime] = None
    healthians_zone_id: Optional[str] = None
    external_camp_id: Optional[int] = None
    blood_collection_type: Optional[str] = None
    consultation_mode: Optional[str] = None
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    load_prev_assessment_questionnaires: bool = False
    notifications: list[EngagementNotificationOutput] = Field(default_factory=list)
    readiness: ChecklistReadiness


class EngagementDetailsResponse(EngagementListItem):
    metsights_engagement_id: Optional[str] = None
    readiness: ChecklistReadiness = Field(
        default_factory=lambda: ChecklistReadiness(done=0, total=0, percent=0),
    )


class OnboardingAssistantsAddRequest(BaseModel):
    """Request to assign employees as onboarding assistants."""

    employee_ids: list[int] = Field(..., min_length=1)


class CreatePhleboRequest(BaseModel):
    """Create (or reuse) a phlebo user/employee and assign to an engagement."""

    name: SafeDisplayName
    phone: PhoneStr
    confirm_existing: bool = False


class EngagementAssessmentPackageAddRequest(BaseModel):
    """Request to add an additional assessment package to an engagement."""

    # Must preserve case: seeded codes are uppercase (MY_FITNESS_PRINT). SlugKey lowercases.
    package_code: PackageCode


# Metsights record sub-resource keys accepted by engagement questionnaire push.
PUSH_QUESTIONNAIRE_CATEGORY_KEYS = frozenset(
    {
        "physical-measurement",
        "vitals",
        "diet-lifestyle-parameters",
        "blood-parameters",
        "advanced-blood-parameters",
        "fitness-parameters",
    }
)


class EngagementPushQuestionnairesRequest(BaseModel):
    """Request to push questionnaire answers for a specific package."""

    package_id: PositiveIntId
    assessment_instance_id: PositiveIntId | None = Field(
        default=None,
        description="When set, push only this assessment instance (client-side batching).",
    )
    categories: list[str] | None = Field(
        default=None,
        description="Metsights resource keys to push. None = all for package type.",
    )


class EngagementConnectMetsightsRecordsRequest(BaseModel):
    """Request to create Metsights records for existing assessment instances."""

    package_id: PositiveIntId


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


class EngagementParticipantUpdateRequest(BaseModel):
    participant_department: Optional[str] = Field(default=None, max_length=100)
    consultations: Optional[dict[str, Any]] = None
    blood_collection_time_slot_id: Optional[str] = Field(default=None, max_length=100)
    booking_id: Optional[str] = Field(default=None, max_length=100)
    engagement_date: Optional[date] = None
    slot_start_time: Optional[time] = None
    blood_collection_cabin: Optional[str] = Field(default=None, max_length=100)


class MoveParticipantRequest(BaseModel):
    target_engagement_id: PositiveIntId


class MoveParticipantsBatchRequest(BaseModel):
    target_engagement_id: PositiveIntId
    user_ids: list[int] = Field(min_length=1)


class LoadBloodReportsForParticipantsRequest(BaseModel):
    user_ids: list[int] = Field(min_length=1, max_length=50)


class ConsultationConsentRequest(BaseModel):
    bio_ai: bool = False
    blood_report: bool = False
    questionnaire: bool = False


# Backward-compatible alias
EngagementParticipantDepartmentUpdateRequest = EngagementParticipantUpdateRequest
