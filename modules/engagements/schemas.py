"""Pydantic schemas for engagements APIs."""

from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from modules.engagement_notifications.service_config import NotificationServiceConfigItem

from modules.checklists.schemas import ChecklistReadiness
from modules.engagements.models import BloodCollectionType, EngagementStatus

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
    cabin_name: str = Field(min_length=1, max_length=200)
    cabin_key: str = Field(min_length=1, max_length=100)
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
    expert_type: str = Field(min_length=1, max_length=100)

    @field_validator("expert_type")
    @classmethod
    def validate_expert_type(cls, value: str) -> str:
        stripped = (value or "").strip()
        if not stripped:
            raise ValueError("expert_type is required")
        return stripped


class SlotDetail(BaseModel):
    blood_collection: Optional[dict[str, list[CabinSlotConfig]]] = None
    consultation: Optional[dict[str, list[ConsultationCabinSlotConfig]]] = None

    @field_validator("blood_collection", "consultation")
    @classmethod
    def validate_date_keys(cls, value: Optional[dict[str, list[Any]]]) -> Optional[dict[str, list[Any]]]:
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
            for cabins in section.values():
                for cabin in cabins:
                    keys.append(cabin.cabin_key)
        if len(keys) != len(set(keys)):
            raise ValueError("cabin_key must be unique within slot_detail")
        return self


class EngagementNotificationInput(BaseModel):
    """A single notification event config for create/update."""

    notification_event_id: int
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)

    @field_validator("notification_services", mode="before")
    @classmethod
    def normalize_services(cls, value: object) -> list[NotificationServiceConfigItem]:
        from modules.engagement_notifications.service_config import normalize_notification_services

        return normalize_notification_services(value if isinstance(value, list) else [])


class EngagementCreateRequest(BaseModel):
    """Create a new B2B engagement."""

    engagement_name: Optional[str] = Field(default=None, max_length=200)
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)
    organization_id: int = Field(gt=0)
    camp_no: Optional[int] = None
    engagement_type: int = Field(gt=0)
    consultations: Optional[dict[str, bool]] = None
    slot_detail: Optional[SlotDetail] = None
    engagement_code: Optional[str] = Field(default=None, max_length=50)
    assessment_package_id: Optional[int] = Field(default=None, gt=0)
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    city: Optional[str] = Field(default=None, max_length=100)
    address: Optional[str] = Field(default=None, max_length=500)
    sub_locality: Optional[str] = Field(default=None, max_length=200)
    landmark: Optional[str] = Field(default=None, max_length=200)
    pincode: Optional[str] = Field(default=None, max_length=20)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    healthians_zone_id: Optional[str] = Field(default=None, max_length=50)
    external_camp_id: Optional[int] = None
    blood_collection_type: Optional[BloodCollectionType] = None
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    notifications: list[EngagementNotificationInput] | None = None


class EngagementUpdateRequest(BaseModel):
    """Update editable engagement fields."""

    engagement_name: Optional[str] = Field(default=None, max_length=200)
    engagement_code: str = Field(min_length=1, max_length=50)
    organization_id: Optional[int] = Field(default=None, gt=0)
    camp_no: Optional[int] = None
    engagement_type: int = Field(gt=0)
    consultations: Optional[dict[str, bool]] = None
    slot_detail: Optional[SlotDetail] = None
    assessment_package_id: Optional[int] = Field(default=None, gt=0)
    diagnostic_package_id: Optional[int] = Field(default=None, gt=0)
    city: Optional[str] = Field(default=None, max_length=100)
    address: Optional[str] = Field(default=None, max_length=500)
    sub_locality: Optional[str] = Field(default=None, max_length=200)
    landmark: Optional[str] = Field(default=None, max_length=200)
    pincode: Optional[str] = Field(default=None, max_length=20)
    state: Optional[str] = Field(default=None, max_length=100)
    country: Optional[str] = Field(default=None, max_length=100)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    slot_duration: int = Field(gt=0, le=480)
    start_date: date
    end_date: date
    healthians_zone_id: Optional[str] = Field(default=None, max_length=50)
    external_camp_id: Optional[int] = None
    blood_collection_type: Optional[BloodCollectionType] = None
    metsights_engagement_id: Optional[str] = Field(default=None, max_length=200)
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
    notifications: list[EngagementNotificationInput] | None = None


class EngagementStatusUpdateRequest(BaseModel):
    """Change engagement status."""

    status: str = Field(min_length=1, max_length=30)


class ResolveHealthiansZoneRequest(BaseModel):
    """Resolve Healthians zone ID for an engagement location."""

    diagnostic_package_id: int = Field(gt=0)
    latitude: float
    longitude: float
    pincode: str = Field(min_length=1, max_length=20)


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
    create_profile_on_metsights: bool = False
    enroll_for_fitprint_full: bool = False
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


class EngagementAssessmentPackageAddRequest(BaseModel):
    """Request to add an additional assessment package to an engagement."""

    package_code: str = Field(..., min_length=1, max_length=100)


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

    package_id: int = Field(..., gt=0)
    assessment_instance_id: int | None = Field(
        default=None,
        gt=0,
        description="When set, push only this assessment instance (client-side batching).",
    )
    categories: list[str] | None = Field(
        default=None,
        description="Metsights resource keys to push. None = all for package type.",
    )


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


class EngagementParticipantUpdateRequest(BaseModel):
    participant_department: Optional[str] = Field(default=None, max_length=100)
    consultations: Optional[dict[str, Any]] = None
    blood_collection_time_slot_id: Optional[str] = Field(default=None, max_length=100)
    booking_id: Optional[str] = Field(default=None, max_length=100)


class MoveParticipantRequest(BaseModel):
    target_engagement_id: int


class MoveParticipantsBatchRequest(BaseModel):
    target_engagement_id: int
    user_ids: list[int] = Field(min_length=1)


class ConsultationConsentRequest(BaseModel):
    bio_ai: bool = False
    blood_report: bool = False
    questionnaire: bool = False


# Backward-compatible alias
EngagementParticipantDepartmentUpdateRequest = EngagementParticipantUpdateRequest
