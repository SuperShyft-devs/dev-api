"""Pydantic schemas for experts APIs."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


ExpertTypeLiteral = Literal["doctor", "nutritionist"]
ConsultationModeLiteral = Literal["video", "voice", "chat"]


class ExpertCreateRequest(BaseModel):
    user_id: int = Field(gt=0)
    expert_type: ExpertTypeLiteral
    display_name: str = Field(min_length=1, max_length=200)
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
    expert_type: ExpertTypeLiteral
    display_name: str = Field(min_length=1, max_length=200)
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
