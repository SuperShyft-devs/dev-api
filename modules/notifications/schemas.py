"""Notifications module Pydantic schemas."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, field_validator, model_validator

from common.validation import (
    OptionalSafeDisplayName,
    OptionalSafeText,
    OtpCode,
    PositiveIntId,
    SafeDisplayName,
    SafeText,
    ServiceKey,
    SlugKey,
    validate_nested_strings,
)


class SessionDetails(BaseModel):
    want: bool
    date: date
    slot: str
    expert_type: SlugKey


class PrepareReportsRequest(BaseModel):
    user_id: PositiveIntId
    require_blood_report_url: bool = False
    require_bio_ai_report_url: bool = False


class DispatchRequest(BaseModel):
    service_key: ServiceKey
    user_ids: list[PositiveIntId] = Field(..., min_length=1)
    engagement_id: PositiveIntId | None = None
    assessment_instance_id: PositiveIntId | None = None
    participant_details: dict | None = None
    otp: OtpCode | None = None
    session_details: SessionDetails | None = None
    session_details_by_user_id: dict[int, SessionDetails] | None = None
    external_link: str | None = None

    @model_validator(mode="after")
    def sanitize_participant_details(self) -> "DispatchRequest":
        if self.participant_details is not None:
            validate_nested_strings(self.participant_details)
        return self

    @field_validator("external_link")
    @classmethod
    def validate_external_link(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        from urllib.parse import urlparse

        parsed = urlparse(stripped)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("external_link must be a valid http(s) URL")
        return stripped

    @field_validator("session_details_by_user_id", mode="before")
    @classmethod
    def _coerce_session_details_user_ids(cls, value: object) -> object:
        if value is None or not isinstance(value, dict):
            return value
        return {int(user_id): details for user_id, details in value.items()}


class CallbackRequest(BaseModel):
    notification_id: int
    status: str = Field(..., pattern=r"^(sent|failed)$")
    message: OptionalSafeText = None


class NotificationServiceCreate(BaseModel):
    service_key: ServiceKey
    display_name: SafeDisplayName
    channel: str = Field(..., pattern=r"^(email|whatsapp)$")
    webhook_path: SafeText
    is_active: bool = True
    require_blood_report_url: bool = False
    require_bio_ai_report_url: bool = False
    require_participant_detail: bool = False
    require_otp: bool = False
    require_session_details: bool = False
    require_external_link: bool = False


class NotificationServiceUpdate(BaseModel):
    display_name: OptionalSafeDisplayName = None
    channel: str | None = Field(None, pattern=r"^(email|whatsapp)$")
    webhook_path: OptionalSafeText = None
    is_active: bool | None = None
    require_blood_report_url: bool | None = None
    require_bio_ai_report_url: bool | None = None
    require_participant_detail: bool | None = None
    require_otp: bool | None = None
    require_session_details: bool | None = None
    require_external_link: bool | None = None
