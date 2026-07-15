"""Pydantic schemas for experts APIs."""

from __future__ import annotations

from datetime import date, time
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class ExpertTypeCreateRequest(BaseModel):
    type_key: str = Field(min_length=1, max_length=100)
    type: str = Field(min_length=1, max_length=200)

    @field_validator("type_key")
    @classmethod
    def _type_key_slug(cls, v: str) -> str:
        v = v.strip().lower()
        if not v.replace("_", "").isalnum():
            raise ValueError("type_key must be alphanumeric with underscores only")
        return v


class ExpertTypeUpdateRequest(BaseModel):
    type_key: Optional[str] = Field(default=None, min_length=1, max_length=100)
    type: Optional[str] = Field(default=None, min_length=1, max_length=200)

    @field_validator("type_key")
    @classmethod
    def _type_key_slug(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        if not v.replace("_", "").isalnum():
            raise ValueError("type_key must be alphanumeric with underscores only")
        return v


ConsultationModeLiteral = Literal["video", "voice", "chat"]


class ExpertCreateRequest(BaseModel):
    user_id: int = Field(gt=0)
    expert_type: str = Field(min_length=1, max_length=100)
    specialization: str = Field(min_length=1, max_length=200)
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    experience_years: Optional[int] = Field(default=None, ge=0, le=80)
    qualifications: Optional[str] = Field(default=None, max_length=500)
    about_text: Optional[str] = Field(default=None, max_length=8000)
    consultation_modes: Optional[list[ConsultationModeLiteral]] = None
    languages: Optional[list[str]] = None
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
            if not item or len(item.strip()) > 50:
                raise ValueError("Invalid languages")
        return [x.strip() for x in v]


class ExpertUpdateRequest(BaseModel):
    user_id: int = Field(gt=0)
    expert_type: str = Field(min_length=1, max_length=100)
    specialization: str = Field(min_length=1, max_length=200)
    profile_photo: Optional[str] = Field(default=None, max_length=500)
    experience_years: Optional[int] = Field(default=None, ge=0, le=80)
    qualifications: Optional[str] = Field(default=None, max_length=500)
    about_text: Optional[str] = Field(default=None, max_length=8000)
    consultation_modes: Optional[list[ConsultationModeLiteral]] = None
    languages: Optional[list[str]] = None
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
            if not item or len(item.strip()) > 50:
                raise ValueError("Invalid languages")
        return [x.strip() for x in v]


class ExpertStatusUpdateRequest(BaseModel):
    status: Literal["active", "inactive"]


class ExpertTagCreateRequest(BaseModel):
    tag_name: str = Field(min_length=1, max_length=120)
    display_order: Optional[int] = Field(default=None, ge=0)


class ExpertReviewCreateRequest(BaseModel):
    rating: Decimal = Field(ge=Decimal("1.0"), le=Decimal("5.0"))
    review_text: Optional[str] = Field(default=None, max_length=4000)


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


class OverrideCreate(BaseModel):
    override_date: date
    availability: bool
    start_time: Optional[time] = None
    end_time: Optional[time] = None
    buffer_time: Optional[int] = Field(default=None, ge=0, le=120)

    @model_validator(mode="after")
    def _times_required_when_available(self) -> "OverrideCreate":
        if self.availability:
            if self.start_time is None or self.end_time is None:
                raise ValueError("start_time and end_time are required when available is true")
            if self.end_time <= self.start_time:
                raise ValueError("end_time must be after start_time")
        return self
