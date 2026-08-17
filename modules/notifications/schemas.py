"""Notifications module Pydantic schemas."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, field_validator


class SessionDetails(BaseModel):
    want: bool
    date: date
    slot: str
    expert_type: str


class PrepareReportsRequest(BaseModel):
    user_id: int = Field(..., gt=0)
    require_blood_report_url: bool = False
    require_bio_ai_report_url: bool = False


class DispatchRequest(BaseModel):
    service_key: str = Field(..., min_length=1)
    user_ids: list[int] = Field(..., min_length=1)
    engagement_id: int | None = None
    assessment_instance_id: int | None = None
    participant_details: dict | None = None
    otp: str | None = None
    session_details: SessionDetails | None = None
    session_details_by_user_id: dict[int, SessionDetails] | None = None

    @field_validator("session_details_by_user_id", mode="before")
    @classmethod
    def _coerce_session_details_user_ids(cls, value: object) -> object:
        if value is None or not isinstance(value, dict):
            return value
        return {int(user_id): details for user_id, details in value.items()}


class CallbackRequest(BaseModel):
    notification_id: int
    status: str = Field(..., pattern=r"^(sent|failed)$")
    message: str | None = None


class NotificationServiceCreate(BaseModel):
    service_key: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)
    channel: str = Field(..., pattern=r"^(email|whatsapp)$")
    webhook_path: str = Field(..., min_length=1)
    is_active: bool = True
    require_blood_report_url: bool = False
    require_bio_ai_report_url: bool = False
    require_participant_detail: bool = False
    require_otp: bool = False
    require_session_details: bool = False


class NotificationServiceUpdate(BaseModel):
    display_name: str | None = None
    channel: str | None = Field(None, pattern=r"^(email|whatsapp)$")
    webhook_path: str | None = None
    is_active: bool | None = None
    require_blood_report_url: bool | None = None
    require_bio_ai_report_url: bool | None = None
    require_participant_detail: bool | None = None
    require_otp: bool | None = None
    require_session_details: bool | None = None
