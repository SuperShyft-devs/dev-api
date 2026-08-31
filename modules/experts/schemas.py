"""Pydantic schemas for experts APIs."""

from __future__ import annotations

from datetime import date, time
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from common.validation import (
    OptionalChecklistText,
    OptionalExpertAboutText,
    OptionalSafeDisplayName,
    OptionalSafeText,
    OptionalSlugKey,
    PositiveIntId,
    SafeDisplayName,
    SlugKey,
)


class ExpertTypeCreateRequest(BaseModel):
    type_key: SlugKey
    type: SafeDisplayName


class ExpertTypeUpdateRequest(BaseModel):
    type_key: OptionalSlugKey = None
    type: OptionalSafeDisplayName = None


ConsultationModeLiteral = Literal["video", "voice", "chat"]


class ExpertCreateRequest(BaseModel):
    user_id: PositiveIntId
    expert_type: SlugKey
    specialization: SafeDisplayName
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    experience_years: Optional[int] = Field(default=None, ge=0, le=80)
    qualifications: OptionalChecklistText = None
    about_text: OptionalExpertAboutText = None
    consultation_modes: Optional[list[ConsultationModeLiteral]] = None
    languages: Optional[list[SafeDisplayName]] = None
    session_duration_mins: Optional[int] = Field(default=None, ge=5, le=480)
    appointment_fee_paise: Optional[int] = Field(default=None, ge=0)
    original_fee_paise: Optional[int] = Field(default=None, ge=0)
    patient_count: Optional[int] = Field(default=0, ge=0)
    effective_from: Optional[date] = None
    effective_until: Optional[date] = None

    @field_validator("languages")
    @classmethod
    def _languages_items(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v
        for item in v:
            if len(item) > 50:
                raise ValueError("Language name must be at most 50 characters")
        return v


class ExpertUpdateRequest(BaseModel):
    user_id: PositiveIntId
    expert_type: SlugKey
    specialization: SafeDisplayName
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    experience_years: Optional[int] = Field(default=None, ge=0, le=80)
    qualifications: OptionalChecklistText = None
    about_text: OptionalExpertAboutText = None
    consultation_modes: Optional[list[ConsultationModeLiteral]] = None
    languages: Optional[list[SafeDisplayName]] = None
    session_duration_mins: Optional[int] = Field(default=None, ge=5, le=480)
    appointment_fee_paise: Optional[int] = Field(default=None, ge=0)
    original_fee_paise: Optional[int] = Field(default=None, ge=0)
    patient_count: Optional[int] = Field(default=None, ge=0)
    effective_from: Optional[date] = None
    effective_until: Optional[date] = None

    @field_validator("languages")
    @classmethod
    def _languages_items(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v
        for item in v:
            if len(item) > 50:
                raise ValueError("Language name must be at most 50 characters")
        return v


class ExpertStatusUpdateRequest(BaseModel):
    status: Literal["active", "inactive"]


class ExpertTagCreateRequest(BaseModel):
    tag_name: SafeDisplayName
    display_order: Optional[int] = Field(default=None, ge=0)


class ExpertReviewCreateRequest(BaseModel):
    rating: Decimal = Field(ge=Decimal("1.0"), le=Decimal("5.0"))
    review_text: OptionalSafeText = None


# ─── Availability schemas ─────────────────────────────────────────────────────

class AvailabilityBlockCreate(BaseModel):
    day_of_week: int = Field(ge=0, le=6)
    start_time: time
    end_time: time
    slot_duration: int = Field(gt=0, le=480)
    buffer_time: int = Field(default=5, ge=0, le=120)

    @model_validator(mode="after")
    def _end_after_start(self) -> "AvailabilityBlockCreate":
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        return self


class AvailabilityBlockUpdate(BaseModel):
    day_of_week: Optional[int] = Field(default=None, ge=0, le=6)
    start_time: Optional[time] = None
    end_time: Optional[time] = None
    slot_duration: Optional[int] = Field(default=None, gt=0, le=480)
    buffer_time: Optional[int] = Field(default=None, ge=0, le=120)


class AvailabilityBulkSave(BaseModel):
    blocks: list[AvailabilityBlockCreate]


OverrideStatusLiteral = Literal["available", "unavailable", "booked"]


class OverrideCreate(BaseModel):
    override_date: date
    status: OverrideStatusLiteral
    start_time: Optional[time] = None
    end_time: Optional[time] = None
    buffer_time: Optional[int] = Field(default=None, ge=0, le=120)

    @model_validator(mode="after")
    def _times_for_status(self) -> "OverrideCreate":
        if self.status in ("available", "unavailable"):
            if self.start_time is None or self.end_time is None:
                raise ValueError("start_time and end_time are required for available/unavailable")
            if self.end_time <= self.start_time:
                raise ValueError("end_time must be after start_time")
        elif self.status == "booked":
            if self.start_time is None:
                raise ValueError("start_time is required when status is booked")
        return self


class ConsultationPreferenceSchema(BaseModel):
    consultation_id: Optional[int] = Field(default=None, gt=0)
    want: bool = False
    date: Optional[str] = None
    cabin: Optional[str] = Field(default=None, max_length=100)
    slot: Optional[str] = None
    expert_id: Optional[int] = Field(default=None, gt=0)
    done: bool = False
    meet_link: Optional[str] = None
    consent: Optional[dict[str, bool]] = None
    consultation_summary: Optional[str] = None
    attachments: Optional[list[str]] = None


class ConsultationManageUpdateRequest(BaseModel):
    consultation_summary: Optional[str] = None
    attachments: Optional[list[str]] = None
    meet_link: Optional[str] = Field(default=None, max_length=500)


class ConsultationBookRequest(BaseModel):
    engagement_id: int = Field(gt=0)
    expert_type: str = Field(min_length=1, max_length=100)
    expert_id: Optional[int] = Field(default=None, gt=0)
    date: date
    slot: str = Field(min_length=1, max_length=20)
    cabin: Optional[str] = Field(default=None, max_length=100)


class ConsultationRescheduleRequest(BaseModel):
    engagement_id: int = Field(gt=0)
    consultation_date: date
    consultation_slot: str = Field(min_length=1, max_length=20)
    expert_type: str = Field(min_length=1, max_length=100)


class ConsultationConfirmRequest(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)
    expert_type: str = Field(min_length=1, max_length=100)
    date: date
    slot: str = Field(min_length=1, max_length=20)
    expert_id: Optional[int] = Field(default=None, gt=0)


class ConsultationDoneRequest(BaseModel):
    user_id: int = Field(gt=0)
    engagement_id: int = Field(gt=0)
    expert_type: str = Field(min_length=1, max_length=100)
    expert_id: Optional[int] = Field(default=None, gt=0)
    meet_link: str = Field(min_length=1, max_length=500)
